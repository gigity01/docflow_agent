"""Plan 执行结果聚合与 Turn Completion 命令生成测试。

核心业务不变量：
1. 确定性结果事实聚合：
   - AggregatePlanUseCase 仅从数据库中已成功执行的 Task 与 TaskExecution 事实表中提取输出和资源引用。
   - 不进行幻觉生成或额外 LLM 推理，保持结果聚合的确定性。
2. Context Turn 完成契约：
   - 聚合成功的 Task 输出和 resource_refs，构造结构化 CompleteTurnCommand。
   - 正确传递 ContextSelectionRecord 中确定的链归属（attribution）、链增量资源更新（chain_updates）与任务 ID 列表，
     驱动下游完成 Turn、挂载链节点并刷新资源队列。
"""

from __future__ import annotations

import unittest
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.infrastructure.database.base import Base
from app.modules.clarification.infrastructure.persistence.models import ClarificationRequest
from app.infrastructure.database.model_registry import load_all_models
from app.infrastructure.database.uow import SQLAlchemyUnitOfWork
from app.modules.aggregation.application.aggregate_plan import (
    AggregatePlanUseCase,
)
from app.modules.context.infrastructure.persistence.models.context_selection_record import (
    ContextSelectionRecord,
)
from app.modules.context.infrastructure.persistence.models.conversation_turn import (
    ConversationTurn,
)
from app.modules.planning.infrastructure.persistence.models import Plan, Task
from app.modules.task_runtime.infrastructure.persistence.models import (
    TaskExecution,
)


class AggregationTest(unittest.IsolatedAsyncioTestCase):
    """验证 Plan 聚合用例从 TaskExecution 事实提取数据并驱动 Context CompleteTurn 的正确性。"""

    async def asyncSetUp(self) -> None:
        """初始化内存 SQLite 数据库及测试所需的 Turn、ContextSelection、Plan、Task 与 TaskExecution 实体。"""
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
            ClarificationRequest.__table__,
            ConversationTurn.__table__,
            ContextSelectionRecord.__table__,
            Plan.__table__,
            Task.__table__,
            TaskExecution.__table__,
        ]
        Base.metadata.create_all(self.engine, tables=self.tables)
        with self.session_factory() as session:
            # 1. 创建处于 processing 状态的会话轮次
            session.add(
                ConversationTurn(
                    turn_id="turn-aggregate",
                    conversation_id="conversation-1",
                    user_input="处理文档 7",
                    task_ids=["task-aggregate"],
                    status="processing",
                )
            )
            # 2. 创建上下文选择记录（关联到已有 chain-1）
            session.add(
                ContextSelectionRecord(
                    selection_id="selection-aggregate",
                    conversation_id="conversation-1",
                    current_turn_id="turn-aggregate",
                    relevant_chain_ids=["chain-1"],
                    selection_mode="single_context",
                    reason_summary="继续现有链",
                )
            )
            # 3. 创建已完成的 Plan
            session.add(
                Plan(
                    plan_id="plan-aggregate",
                    workflow_id="workflow-aggregate",
                    turn_id="turn-aggregate",
                    parent_plan_id=None,
                    current_task_id=None,
                    status="completed",
                    revision=1,
                    failure_code=None,
                    failure_reason=None,
                )
            )
            # 4. 创建执行成功的 Task
            session.add(
                Task(
                    task_id="task-aggregate",
                    plan_id="plan-aggregate",
                    turn_id="turn-aggregate",
                    task_ref="process",
                    capability_code="process_document",
                    input_json={"document_id": 7},
                    sequence=1,
                    status="succeeded",
                    attempt_count=1,
                    max_attempts=3,
                    output_json={"document_id": 7, "status": "processed"},
                )
            )
            # 5. 创建对应的 TaskExecution 事实记录（包含资源引用 document:7）
            session.add(
                TaskExecution(
                    execution_id="execution-aggregate",
                    task_id="task-aggregate",
                    plan_id="plan-aggregate",
                    workflow_id="workflow-aggregate",
                    attempt=1,
                    status="succeeded",
                    executor_code="document.process",
                    input_snapshot_json={"document_id": 7},
                    output_json={"document_id": 7, "status": "processed"},
                    resource_refs_json=["document:7"],
                    retryable=False,
                    operation_id="operation-aggregate",
                )
            )
            session.commit()

    async def asyncTearDown(self) -> None:
        """清理数据库表并销毁连接引擎。"""
        Base.metadata.drop_all(
            self.engine,
            tables=list(reversed(self.tables)),
        )
        self.engine.dispose()

    async def test_aggregates_all_outputs_and_resource_refs(self) -> None:
        """验证聚合用例正确收集所有 TaskExecution 的输出与资源引用，并准确构造 CompleteTurnCommand。"""
        context_service = mock.Mock()
        context_service.complete_turn = mock.AsyncMock(return_value="completed")
        use_case = AggregatePlanUseCase(
            uow_factory=lambda: SQLAlchemyUnitOfWork(self.session_factory),
            context_service=context_service,
        )
        result = await use_case.execute("plan-aggregate")
        self.assertEqual(result, "completed")
        turn_id, command = context_service.complete_turn.await_args.args
        # 验证关联的 Turn ID
        self.assertEqual(turn_id, "turn-aggregate")
        # 验证 Task ID 列表完整性
        self.assertEqual(command.task_ids, ["task-aggregate"])
        # 验证链归属（归属于已有 chain-1，不新建链）
        self.assertEqual(
            command.attribution.existing_chain_ids,
            ["chain-1"],
        )
        self.assertFalse(command.attribution.create_new_chain)
        # 验证增量资源更新包含了 execution 中产出的 document:7
        self.assertEqual(command.chain_updates[0].chain_id, "chain-1")
        self.assertEqual(
            command.chain_updates[0].related_resources[0].resource_key,
            "document:7",
        )


if __name__ == "__main__":
    unittest.main()
