"""澄清回答（AnswerClarificationUseCase）与 Replan（ReplanUseCase）事务及多 Revision 测试。

核心业务不变量（遵循 AGENTS.md 规范）：
1. 澄清回答复用源 Turn（In-place Clarification Answer）：
   - 在单事务内锁定源 Turn、ClarificationRequest 和 Plan；
   - 将回答写入 `ConversationTurn.clarification_input`，请求标记为 `answered`，Turn 推进到 `processing`；
   - 写入 `planning.replan_requested` Outbox 事件。
2. 多 Revision 与状态继承：
   - 新 Plan revision 复用同一 Turn、Context Selection 和 workflow_id；Planner 输入包含原始 `user_input` 与澄清补充；
   - 同一 workflow 最多 3 个 revision，新 revision 创建前将旧 Plan 和未完成 Task 标记为 `superseded`。
"""

from __future__ import annotations

import unittest

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.infrastructure.database.base import Base
from app.infrastructure.database.model_registry import load_all_models
from app.infrastructure.database.uow import SQLAlchemyUnitOfWork
from app.modules.clarification.application.answer import (
    AnswerClarificationUseCase,
)
from app.modules.clarification.application.errors import (
    ClarificationApplicationError,
)
from app.modules.clarification.infrastructure.persistence.models import (
    ClarificationRequest,
)
from app.modules.context.infrastructure.persistence.models.conversation_turn import (
    ConversationTurn,
)
from app.modules.messaging.infrastructure.persistence.models import (
    InboxEvent,
    OutboxEvent,
)
from app.modules.planning.application.dto import RunPlanningResult
from app.modules.planning.application.ports import PlanningApplicationPorts
from app.modules.planning.application.replan import (
    ReplanRequested,
    ReplanUseCase,
)
from app.modules.planning.application.run_planning import (
    _compose_current_user_input,
)
from app.modules.planning.domain.enums import PlanStatus
from app.modules.planning.infrastructure.persistence.models import (
    Plan,
    Task,
    TaskDependency,
)


class _RunPlanning:
    """测试用 Planner 执行替身。"""
    def __init__(self) -> None:
        self.calls = []

    async def execute_existing(self, command, plan_id):
        self.calls.append((command, plan_id))
        return RunPlanningResult(
            plan_id=plan_id,
            turn_id=command.turn_id,
            status=PlanStatus.RETRY_PENDING,
            task_ids=[],
            failure_reason="offline test",
        )


class ReplanClarificationTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        load_all_models()
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.session_factory = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
        )
        self.tables = [
            ConversationTurn.__table__,
            Plan.__table__,
            Task.__table__,
            TaskDependency.__table__,
            OutboxEvent.__table__,
            InboxEvent.__table__,
            ClarificationRequest.__table__,
        ]
        Base.metadata.create_all(self.engine, tables=self.tables)
        with self.session_factory() as session:
            session.add_all(
                [
                    ConversationTurn(
                        turn_id="turn-question",
                        conversation_id="conversation-1",
                        user_input="处理那个文档",
                        assistant_content="请确认文档。",
                        task_ids=[],
                        status="needs_clarification",
                    ),
                ]
            )
            session.add(
                Plan(
                    plan_id="plan-question",
                    workflow_id="workflow-1",
                    turn_id="turn-question",
                    parent_plan_id=None,
                    current_task_id=None,
                    status="needs_clarification",
                    revision=1,
                    failure_code="clarification_required",
                    failure_reason="文档不唯一",
                )
            )
            session.add(
                ClarificationRequest(
                    clarification_id="clarification-1",
                    conversation_id="conversation-1",
                    source_turn_id="turn-question",
                    source_plan_id="plan-question",
                    kind="resource",
                    reason="文档不唯一",
                    question="请确认文档。",
                    required_information_json=["document_id"],
                    known_resource_refs_json=[],
                    status="open",
                    answer_turn_id=None,
                )
            )
            session.commit()

    async def asyncTearDown(self) -> None:
        Base.metadata.drop_all(
            self.engine,
            tables=list(reversed(self.tables)),
        )
        self.engine.dispose()

    async def test_answer_creates_replan_event_and_new_revision(self) -> None:
        uow_factory = lambda: SQLAlchemyUnitOfWork(self.session_factory)
        answer = AnswerClarificationUseCase(
            uow_factory=uow_factory,
            outbox_event_factory=OutboxEvent,
        )
        source_plan_id = answer.execute(
            conversation_id="conversation-1",
            clarification_id="clarification-1",
            answer="文档 7",
        )
        self.assertEqual(source_plan_id.plan_id, "plan-question")
        with self.session_factory() as session:
            event = session.query(OutboxEvent).one()
            request = session.get(ClarificationRequest, "clarification-1")
            source_turn = session.get(ConversationTurn, "turn-question")
            self.assertEqual(request.status, "answered")
            self.assertEqual(request.answer_turn_id, "turn-question")
            self.assertIn("文档 7", source_turn.clarification_input)
            self.assertEqual(source_turn.status, "processing")

        runner = _RunPlanning()
        ports = PlanningApplicationPorts(
            uow_factory=uow_factory,
            plan_factory=Plan,
            task_factory=Task,
            task_dependency_factory=TaskDependency,
            outbox_event_factory=OutboxEvent,
            inbox_event_factory=InboxEvent,
            clarification_request_factory=ClarificationRequest,
            integrity_error_type=IntegrityError,
        )
        replan = ReplanUseCase(ports=ports, run_planning=runner)
        result = await replan.execute(
            ReplanRequested(event_id=event.event_id, **event.payload_json)
        )
        self.assertEqual(result.status, PlanStatus.RETRY_PENDING)
        with self.session_factory() as session:
            old_plan = session.get(Plan, "plan-question")
            new_plan = (
                session.query(Plan).filter(Plan.revision == 2).one()
            )
            self.assertEqual(old_plan.status, "superseded")
            self.assertEqual(new_plan.turn_id, "turn-question")
            self.assertEqual(new_plan.workflow_id, "workflow-1")
            self.assertEqual(new_plan.parent_plan_id, "plan-question")
            self.assertEqual(session.query(InboxEvent).count(), 1)

    async def test_answer_cannot_be_submitted_twice(self) -> None:
        answer = AnswerClarificationUseCase(
            uow_factory=lambda: SQLAlchemyUnitOfWork(self.session_factory),
            outbox_event_factory=OutboxEvent,
        )
        answer.execute(
            conversation_id="conversation-1",
            clarification_id="clarification-1",
            answer="文档 7",
        )

        with self.assertRaises(ClarificationApplicationError) as raised:
            answer.execute(
                conversation_id="conversation-1",
                clarification_id="clarification-1",
                answer="文档 8",
            )
        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(
            str(raised.exception),
            "Clarification 当前状态不允许回答",
        )

        with self.session_factory() as session:
            self.assertEqual(session.query(OutboxEvent).count(), 1)

    async def test_invalid_answer_is_rejected_without_writing_event(self) -> None:
        answer = AnswerClarificationUseCase(
            uow_factory=lambda: SQLAlchemyUnitOfWork(self.session_factory),
            outbox_event_factory=OutboxEvent,
        )

        with self.assertRaises(ClarificationApplicationError) as raised:
            answer.execute(
                conversation_id="conversation-1",
                clarification_id="clarification-1",
                answer="   ",
            )
        self.assertEqual(raised.exception.status_code, 400)

        with self.session_factory() as session:
            request = session.get(ClarificationRequest, "clarification-1")
            self.assertEqual(request.status, "open")
            self.assertEqual(session.query(OutboxEvent).count(), 0)

    async def test_unknown_source_turn_is_not_found(self) -> None:
        answer = AnswerClarificationUseCase(
            uow_factory=lambda: SQLAlchemyUnitOfWork(self.session_factory),
            outbox_event_factory=OutboxEvent,
        )

        with self.assertRaises(ClarificationApplicationError) as raised:
            answer.execute(
                conversation_id="conversation-1",
                clarification_id="missing",
                answer="文档 7",
            )
        self.assertEqual(raised.exception.status_code, 404)

    async def test_cross_conversation_answer_is_not_found(self) -> None:
        answer = AnswerClarificationUseCase(
            uow_factory=lambda: SQLAlchemyUnitOfWork(self.session_factory),
            outbox_event_factory=OutboxEvent,
        )

        with self.assertRaises(ClarificationApplicationError) as raised:
            answer.execute(
                conversation_id="conversation-other",
                clarification_id="clarification-1",
                answer="文档 7",
            )
        self.assertEqual(raised.exception.status_code, 404)

    def test_planner_input_contains_original_request_and_clarification(self) -> None:
        combined = _compose_current_user_input("处理那个文档", "文档 7")

        self.assertIn("原始用户请求：\n处理那个文档", combined)
        self.assertIn("用户对澄清问题的补充：\n文档 7", combined)


if __name__ == "__main__":
    unittest.main()
