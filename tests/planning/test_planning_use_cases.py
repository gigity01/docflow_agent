"""Planning 持久化用例（CreatePlan, CreateTask, FinalizePlan）核心业务不变量测试。

核心业务不变量（遵循 AGENTS.md 规范）：
1. Plan 与 Task 约束：
   - 每个 Plan 包含 1~10 个 Task；`sequence` 必须从 1 开始严格递增连续且唯一。
   - Task 依赖仅通过 `task_ref` 和 `depends_on_task_refs` 表达，发布时持久化依赖边并校验无环有向图（DAG），最大深度限制为 3。
2. 状态机流转与不可伪造原则：
   - 包含标记 needs_clarification、unsupported、retry_pending 等状态转换；模型输出不能冒充数据库事实。
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
from app.modules.context.infrastructure.persistence.models.conversation_turn import (
    ConversationTurn,
)
from app.modules.planning.application.dto import (
    CreateBuildChunksTaskInput,
    CreateIndexVectorsTaskInput,
    CreatePlanInput,
    CreateProcessDocumentTaskInput,
    FinalizePlanInput,
    MarkPlanNeedsClarificationInput,
    MarkPlanUnsupportedInput,
    SetClarificationQuestionInput,
)
from app.modules.planning.application.errors import PlanningApplicationError
from app.modules.planning.application.ports import PlanningApplicationPorts
from app.modules.planning.application.use_cases import (
    build_planning_use_cases,
)
from app.modules.planning.infrastructure.persistence.models import Plan, Task
from app.modules.planning.infrastructure.persistence.models import TaskDependency
from app.modules.messaging.infrastructure.persistence.models import InboxEvent, OutboxEvent
from app.modules.clarification.infrastructure.persistence.models import ClarificationRequest


class _TrackingUnitOfWork(SQLAlchemyUnitOfWork):
    """测试用追踪 commit 调用次数的 UnitOfWork 替身。"""
    def __init__(self, session_factory) -> None:
        super().__init__(session_factory)
        self.commit_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1
        super().commit()


class _UnitOfWorkFactory:
    """测试用 UnitOfWork 工厂替身。"""
    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory
        self.instances: list[_TrackingUnitOfWork] = []

    def __call__(self) -> _TrackingUnitOfWork:
        uow = _TrackingUnitOfWork(self._session_factory)
        self.instances.append(uow)
        return uow


class PlanningUseCasesTest(unittest.TestCase):
    """验证 Planning 用例的 DAG 校验、Task 依赖持久化、原子事务与状态机流转。"""
    def setUp(self) -> None:
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
            ClarificationRequest.__table__,
        ]
        Base.metadata.create_all(self.engine, tables=self.tables)
        self.uow_factory = _UnitOfWorkFactory(self.session_factory)
        self.use_cases = build_planning_use_cases(
            PlanningApplicationPorts(
                uow_factory=self.uow_factory,
                plan_factory=Plan,
                task_factory=Task,
                task_dependency_factory=TaskDependency,
                outbox_event_factory=OutboxEvent,
                inbox_event_factory=InboxEvent,
                clarification_request_factory=ClarificationRequest,
                integrity_error_type=IntegrityError,
            )
        )
        with self.session_factory() as session:
            session.add(
                ConversationTurn(
                    turn_id="turn-1",
                    conversation_id="conversation-1",
                    user_input="处理并索引文档 7",
                    task_ids=[],
                    status="context_ready",
                )
            )
            session.commit()

    def tearDown(self) -> None:
        Base.metadata.drop_all(
            self.engine,
            tables=list(reversed(self.tables)),
        )
        self.engine.dispose()

    def test_finalize_plan_atomically_publishes_tasks_and_updates_turn(self) -> None:
        plan = self.use_cases.create_plan.execute(
            CreatePlanInput(turn_id="turn-1")
        )
        created_tasks = [
            self.use_cases.create_process_document_task.execute(
                CreateProcessDocumentTaskInput(
                    plan_id=plan.plan_id,
                    turn_id="turn-1",
                    document_id=7,
                    sequence=1,
                    task_ref="process",
                )
            ),
            self.use_cases.create_build_chunks_task.execute(
                CreateBuildChunksTaskInput(
                    plan_id=plan.plan_id,
                    turn_id="turn-1",
                    document_id=7,
                    sequence=2,
                    task_ref="chunks",
                    depends_on_task_refs=["process"],
                )
            ),
            self.use_cases.create_index_vectors_task.execute(
                CreateIndexVectorsTaskInput(
                    plan_id=plan.plan_id,
                    turn_id="turn-1",
                    document_id=7,
                    sequence=3,
                    task_ref="vectors",
                    depends_on_task_refs=["chunks"],
                )
            ),
        ]

        result = self.use_cases.finalize_plan.execute(
            FinalizePlanInput(plan_id=plan.plan_id, turn_id="turn-1")
        )
        finalize_uow = self.uow_factory.instances[-1]

        expected_task_ids = [task.task_id for task in created_tasks]
        self.assertEqual(result.plan_status, "ready")
        self.assertEqual(result.task_ids, expected_task_ids)
        self.assertEqual(finalize_uow.commit_calls, 1)
        with self.session_factory() as session:
            stored_plan = session.get(Plan, plan.plan_id)
            stored_tasks = (
                session.query(Task)
                .filter(Task.plan_id == plan.plan_id)
                .order_by(Task.sequence.asc())
                .all()
            )
            stored_turn = session.get(ConversationTurn, "turn-1")
            dependencies = session.query(TaskDependency).all()
            outbox = session.query(OutboxEvent).filter(OutboxEvent.event_type == "runtime.plan_wakeup").one()
            self.assertEqual(stored_plan.status, "ready")
            self.assertEqual(
                [task.status for task in stored_tasks],
                ["pending", "pending", "pending"],
            )
            self.assertEqual(stored_turn.task_ids, expected_task_ids)
            self.assertEqual(stored_turn.status, "processing")
            self.assertEqual(len(dependencies), 2)
            self.assertEqual(outbox.event_type, "runtime.plan_wakeup")
            self.assertEqual(outbox.aggregate_id, plan.plan_id)

    def test_non_planning_plan_rejects_new_task(self) -> None:
        plan = self.use_cases.create_plan.execute(
            CreatePlanInput(turn_id="turn-1")
        )
        self.use_cases.mark_plan_unsupported.execute(
            MarkPlanUnsupportedInput(
                plan_id=plan.plan_id,
                reason="当前能力不支持",
            )
        )

        with self.assertRaises(PlanningApplicationError) as raised:
            self.use_cases.create_process_document_task.execute(
                CreateProcessDocumentTaskInput(
                    plan_id=plan.plan_id,
                    turn_id="turn-1",
                    document_id=7,
                    sequence=1,
                    task_ref="process",
                )
            )

        self.assertEqual(raised.exception.result_code, "plan_state_conflict")
        with self.session_factory() as session:
            self.assertEqual(session.query(Task).count(), 0)

    def test_clarification_marks_turn_and_persists_question_atomically(
        self,
    ) -> None:
        plan = self.use_cases.create_plan.execute(
            CreatePlanInput(turn_id="turn-1")
        )

        self.use_cases.mark_plan_needs_clarification.execute(
            MarkPlanNeedsClarificationInput(
                plan_id=plan.plan_id,
                conversation_id="conversation-1",
                kind="resource",
                reason="文档不唯一",
                required_information=["document_id"],
            )
        )
        question = self.use_cases.set_clarification_question.execute(
            SetClarificationQuestionInput(
                plan_id=plan.plan_id,
                question="请确认要处理哪一个文档？",
            )
        )

        self.assertEqual(question, "请确认要处理哪一个文档？")
        with self.session_factory() as session:
            stored_plan = session.get(Plan, plan.plan_id)
            stored_turn = session.get(ConversationTurn, "turn-1")
            request = session.query(ClarificationRequest).one()
            self.assertEqual(stored_plan.status, "needs_clarification")
            self.assertEqual(stored_turn.status, "needs_clarification")
            self.assertEqual(
                stored_turn.assistant_content,
                "请确认要处理哪一个文档？",
            )
            self.assertEqual(request.status, "open")
            self.assertEqual(request.source_turn_id, "turn-1")

        self.assertEqual(self.uow_factory.instances[-2].commit_calls, 1)
        self.assertEqual(self.uow_factory.instances[-1].commit_calls, 1)

    def test_task_count_and_sequence_invariants_are_enforced(self) -> None:
        plan = self.use_cases.create_plan.execute(
            CreatePlanInput(turn_id="turn-1")
        )
        with self.assertRaises(PlanningApplicationError) as empty_plan:
            self.use_cases.finalize_plan.execute(
                FinalizePlanInput(
                    plan_id=plan.plan_id,
                    turn_id="turn-1",
                )
            )
        self.assertEqual(
            empty_plan.exception.result_code,
            "plan_task_count_invalid",
        )

        def create_task(sequence: int):
            return self.use_cases.create_process_document_task.execute(
                CreateProcessDocumentTaskInput(
                    plan_id=plan.plan_id,
                    turn_id="turn-1",
                    document_id=7,
                    sequence=sequence,
                    task_ref=f"task_{sequence}",
                )
            )

        create_task(1)
        with self.assertRaises(PlanningApplicationError) as duplicate:
            create_task(1)
        self.assertEqual(
            duplicate.exception.result_code,
            "plan_task_sequence_conflict",
        )

        for sequence in range(2, 11):
            create_task(sequence)
        with self.assertRaises(PlanningApplicationError) as overflow:
            create_task(11)
        self.assertEqual(
            overflow.exception.result_code,
            "plan_task_limit_exceeded",
        )
        self.assertIn(
            "uq_tasks_plan_sequence",
            {constraint.name for constraint in Task.__table__.constraints},
        )
        with self.session_factory() as session:
            self.assertEqual(session.query(Task).count(), 10)

    def test_finalize_rejects_cycles_and_dag_depth_over_three(self) -> None:
        deep_plan = self.use_cases.create_plan.execute(
            CreatePlanInput(turn_id="turn-1", revision=1)
        )
        previous: str | None = None
        for sequence, task_ref in enumerate(("a", "b", "c", "d"), start=1):
            self.use_cases.create_process_document_task.execute(
                CreateProcessDocumentTaskInput(
                    plan_id=deep_plan.plan_id,
                    turn_id="turn-1",
                    document_id=sequence,
                    sequence=sequence,
                    task_ref=task_ref,
                    depends_on_task_refs=(
                        [] if previous is None else [previous]
                    ),
                )
            )
            previous = task_ref
        with self.assertRaises(PlanningApplicationError) as depth:
            self.use_cases.finalize_plan.execute(
                FinalizePlanInput(
                    plan_id=deep_plan.plan_id,
                    turn_id="turn-1",
                )
            )
        self.assertEqual(
            depth.exception.result_code,
            "plan_task_dependency_depth_exceeded",
        )

        cycle_plan = self.use_cases.create_plan.execute(
            CreatePlanInput(turn_id="turn-1", revision=2)
        )
        for sequence, task_ref, dependencies in (
            (1, "left", ["right"]),
            (2, "right", ["left"]),
        ):
            self.use_cases.create_process_document_task.execute(
                CreateProcessDocumentTaskInput(
                    plan_id=cycle_plan.plan_id,
                    turn_id="turn-1",
                    document_id=sequence,
                    sequence=sequence,
                    task_ref=task_ref,
                    depends_on_task_refs=dependencies,
                )
            )
        with self.assertRaises(PlanningApplicationError) as cycle:
            self.use_cases.finalize_plan.execute(
                FinalizePlanInput(
                    plan_id=cycle_plan.plan_id,
                    turn_id="turn-1",
                )
            )
        self.assertEqual(
            cycle.exception.result_code,
            "plan_task_dependency_cycle",
        )


if __name__ == "__main__":
    unittest.main()
