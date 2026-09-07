"""Planner 从 context_ready Turn 状态到发布 ready Plan 的全流程规划闭环测试。

核心业务不变量（遵循 AGENTS.md 规范）：
1. 物理隔离的双阶段 Planner（Evidence Run vs Commit Run）：
   - Evidence Run 仅暴露只读 Collector Agent-as-Tool 取证；
   - Commit Run 仅暴露 Planning Tools（create_process_document_task, create_build_chunks_task, create_index_vectors_task, finalize_plan）与 Clarification Handoff，单线程串行执行。
2. Plan 发布原子事务：
   - 同一数据库事务内原子创建 Plan、Task（sequence 从 1 连续）、TaskDependency 依赖边、更新 Turn 为 processing，并写入首个 `runtime.plan_wakeup` Outbox 事件。
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import unittest
from unittest import mock

from agents import ModelSettings, RunContextWrapper
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.agent_runtime.audit import AgentToolAuditLogger
from app.agent_runtime.context import (
    ContextToolServices,
    DocumentToolServices,
    OperationsToolServices,
)
from app.agents.collectors import (
    CollectorResult,
    EvidenceItem,
    build_collector_agents,
)
from app.agents.gap_handler import GapDecision
from app.agents.planner import build_planner_agent
from app.infrastructure.database.base import Base
from app.infrastructure.database.model_registry import load_all_models
from app.infrastructure.database.uow import SQLAlchemyUnitOfWork
from app.modules.context.infrastructure.persistence.models.conversation_turn import (
    ConversationTurn,
)
from app.modules.context.infrastructure.persistence.mapper import (
    build_context_chain,
)
from app.modules.context.infrastructure.persistence.models.context_chain import (
    ContextChain,
)
from app.modules.context.infrastructure.persistence.models.context_chain_node import (
    ContextChainNode,
)
from app.modules.context.infrastructure.persistence.models.context_selection_record import (
    ContextSelectionRecord,
)
from app.modules.context.domain.models import ContextResourceQueue
from app.modules.planning.agent_tools.planning_tools import (
    create_build_chunks_task_handler,
    create_index_vectors_task_handler,
    create_process_document_task_handler,
    finalize_plan_handler,
)
from app.modules.planning.agent_tools.schemas import (
    CreateBuildChunksTaskToolInput,
    CreateIndexVectorsTaskToolInput,
    CreateProcessDocumentTaskToolInput,
    FinalizePlanToolInput,
)
from app.modules.planning.application.dto import (
    CreateBuildChunksTaskInput,
    CreateIndexVectorsTaskInput,
    CreatePlanInput,
    CreateProcessDocumentTaskInput,
    FinalizePlanInput,
    PlannerContextInput,
    RunPlanningInput,
)
from app.modules.planning.application.ports import PlanningApplicationPorts
from app.modules.planning.application.run_planning import RunPlanningUseCase
from app.modules.planning.application.use_cases import (
    build_planning_use_cases,
)
from app.modules.planning.infrastructure.persistence.models import Plan, Task
from app.modules.planning.infrastructure.persistence.models import TaskDependency
from app.modules.messaging.infrastructure.persistence.models import InboxEvent, OutboxEvent
from app.modules.clarification.infrastructure.persistence.models import ClarificationRequest


class _AuditWriter:
    def write(self, _event: dict) -> bool:
        return True


def _mock_document_services() -> DocumentToolServices:
    return DocumentToolServices(
        get_document=mock.Mock(),
        list_documents=mock.Mock(),
        search_documents=mock.Mock(),
        get_document_pipeline_state=mock.Mock(),
        list_document_artifacts=mock.Mock(),
        search_document_artifacts=mock.Mock(),
        list_parent_blocks=mock.Mock(),
        list_child_chunks=mock.Mock(),
        get_document_chunk_statistics=mock.Mock(),
        get_knowledge_base_statistics=mock.Mock(),
        process_document=mock.Mock(),
        build_chunks=mock.Mock(),
        index_vectors=mock.Mock(),
    )


class _ClosedLoopPlannerRunner:
    def __init__(self) -> None:
        self.active_collectors = 0
        self.max_active_collectors = 0
        self.planner_input: PlannerContextInput | None = None
        self.agent_context = None

    async def run(self, *, planner_input: PlannerContextInput, context) -> str:
        self.planner_input = planner_input
        self.agent_context = context

        async def collect() -> None:
            self.active_collectors += 1
            self.max_active_collectors = max(
                self.max_active_collectors,
                self.active_collectors,
            )
            await asyncio.sleep(0)
            self.active_collectors -= 1

        await asyncio.gather(collect(), collect(), collect())
        wrapper = RunContextWrapper(context)
        task_outputs = (
            create_process_document_task_handler(
                wrapper,
                CreateProcessDocumentTaskToolInput(
                    task_ref="process",
                    document_id=7,
                    sequence=1,
                ),
            ),
            create_build_chunks_task_handler(
                wrapper,
                CreateBuildChunksTaskToolInput(
                    task_ref="chunks",
                    document_id=7,
                    sequence=2,
                    depends_on_task_refs=["process"],
                ),
            ),
            create_index_vectors_task_handler(
                wrapper,
                CreateIndexVectorsTaskToolInput(
                    task_ref="vectors",
                    document_id=7,
                    sequence=3,
                    depends_on_task_refs=["chunks"],
                ),
            ),
        )
        if any(output.outcome != "succeeded" for output in task_outputs):
            raise AssertionError("Planning Task Tool 未成功")
        finalized = finalize_plan_handler(wrapper, FinalizePlanToolInput())
        if finalized.outcome != "succeeded":
            raise AssertionError("finalize_plan 未成功")
        return "该文本不是业务事实"


class PlannerRunTest(unittest.IsolatedAsyncioTestCase):
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
            ContextChain.__table__,
            ContextChainNode.__table__,
            ContextSelectionRecord.__table__,
            Plan.__table__,
            Task.__table__,
            TaskDependency.__table__,
            OutboxEvent.__table__,
            ClarificationRequest.__table__,
        ]
        Base.metadata.create_all(self.engine, tables=self.tables)
        with self.session_factory() as session:
            session.add_all(
                [
                    ConversationTurn(
                        turn_id="turn-planner-1",
                        conversation_id="conversation-1",
                        user_input="处理、切块并索引文档 7",
                        task_ids=[],
                        status="context_ready",
                    ),
                    ConversationTurn(
                        turn_id="turn-history-a",
                        conversation_id="conversation-1",
                        user_input="历史上下文 A",
                        assistant_content="回答 A",
                        assistant_compact="摘要 A",
                        task_ids=[],
                        status="completed",
                    ),
                    ConversationTurn(
                        turn_id="turn-history-b",
                        conversation_id="conversation-1",
                        user_input="历史上下文 B",
                        assistant_content="回答 B",
                        assistant_compact="摘要 B",
                        task_ids=[],
                        status="completed",
                    ),
                ]
            )
            session.add_all(
                [
                    ContextChain(
                        chain_id="chain-a",
                        conversation_id="conversation-1",
                        resources={},
                        resource_version=0,
                        last_active_at=datetime(2026, 8, 1, 12, 0, 0),
                        archived=False,
                    ),
                    ContextChain(
                        chain_id="chain-b",
                        conversation_id="conversation-1",
                        resources={},
                        resource_version=0,
                        last_active_at=datetime(2026, 8, 1, 12, 1, 0),
                        archived=False,
                    ),
                ]
            )
            session.flush()
            session.add_all(
                [
                    ContextChainNode(
                        node_id="node-a",
                        chain_id="chain-a",
                        turn_id="turn-history-a",
                        sequence=0,
                        related_task_ids=[],
                        related_resource_refs=[],
                    ),
                    ContextChainNode(
                        node_id="node-b",
                        chain_id="chain-b",
                        turn_id="turn-history-b",
                        sequence=0,
                        related_task_ids=[],
                        related_resource_refs=[],
                    ),
                ]
            )
            session.add(
                ContextSelectionRecord(
                    selection_id="selection-planner-1",
                    conversation_id="conversation-1",
                    current_turn_id="turn-planner-1",
                    relevant_chain_ids=["chain-a", "chain-b"],
                    selection_mode="multi_context",
                    reason_summary="Planner 需要两条历史上下文",
                )
            )
            session.commit()

    async def asyncTearDown(self) -> None:
        Base.metadata.drop_all(
            self.engine,
            tables=list(reversed(self.tables)),
        )
        self.engine.dispose()

    async def test_planner_exposes_independent_collectors_with_concurrency(self) -> None:
        collectors = build_collector_agents(
            model="test-model",
            model_settings=ModelSettings(parallel_tool_calls=False),
        )
        configured_runner = build_planner_agent(
            model="test-model",
            model_settings=ModelSettings(parallel_tool_calls=False),
            collectors=collectors,
        )
        self.assertEqual(
            {tool.name for tool in configured_runner.evidence_agent.tools},
            {
                "collect_document_information",
                "collect_context_information",
                "collect_operation_information",
            },
        )
        self.assertEqual(
            {tool.name for tool in configured_runner.commit_agent.tools},
            {
                "create_process_document_task",
                "create_build_chunks_task",
                "create_index_vectors_task",
                "finalize_plan",
                "mark_plan_unsupported",
            },
        )
        self.assertEqual(
            {
                tool.name
                for tool in configured_runner.gap_handler_agent.tools
            },
            {
                "search_business_docs",
                "list_evidence_tools",
                "find_evidence_tools",
            },
        )
        collector_tool_names = {
            "collect_document_information",
            "collect_context_information",
            "collect_operation_information",
        }
        exposed_collector_tools = [
            tool
            for tool in configured_runner.evidence_agent.tools
            if tool.name in collector_tool_names
        ]
        self.assertEqual(len(exposed_collector_tools), 3)
        self.assertTrue(
            all(tool._is_agent_tool for tool in exposed_collector_tools)
        )
        self.assertTrue(
            configured_runner.evidence_agent.model_settings.parallel_tool_calls
        )
        self.assertFalse(
            configured_runner.commit_agent.model_settings.parallel_tool_calls
        )
        self.assertFalse(
            configured_runner.gap_handler_agent.model_settings.parallel_tool_calls
        )
        self.assertEqual(
            configured_runner.evidence_run_config.tool_execution.max_function_tool_concurrency,
            3,
        )
        self.assertEqual(
            configured_runner.commit_run_config.tool_execution.max_function_tool_concurrency,
            1,
        )
        self.assertEqual(
            configured_runner.gap_handler_run_config.tool_execution.max_function_tool_concurrency,
            1,
        )
        self.assertIs(
            configured_runner.gap_handler_agent.output_type,
            GapDecision,
        )
        self.assertIsNone(configured_runner.commit_agent.output_type)
        self.assertEqual(configured_runner.evidence_agent.handoffs, [])
        self.assertEqual(
            [
                handoff.tool_name
                for handoff in configured_runner.commit_agent.handoffs
            ],
            ["clarification_handoff"],
        )
        self.assertEqual(
            configured_runner.commit_agent.tool_use_behavior,
            {
                "stop_at_tool_names": [
                    "finalize_plan",
                    "mark_plan_unsupported",
                ]
            },
        )

        commit_instructions = configured_runner.commit_agent.instructions
        self.assertIsInstance(commit_instructions, str)
        for invariant in (
            "EvidenceItem.arguments 表示 Query Tool 实际使用的查询条件",
            "evidence_items 为空只表示该 Collector 未提供可验证业务证据",
            "前置 Gap 层已经确认现有 Evidence 足够进入规划",
            "Tool succeeded 但业务对象不存在仍是有效查询事实",
            "本阶段不得重新分类 gap",
        ):
            with self.subTest(invariant=invariant):
                self.assertIn(invariant, commit_instructions)

        evidence_instructions = configured_runner.evidence_agent.instructions
        for invariant in (
            "Collector 已经返回足够证据的事实，不得重复调查",
            "必须明确写入对应 Collector 的 gap",
            "gap 只描述未知事实",
            "Service Map",
        ):
            with self.subTest(invariant=invariant):
                self.assertIn(invariant, evidence_instructions)

    async def test_planner_continues_commit_from_full_evidence_history(self) -> None:
        collectors = build_collector_agents(
            model="test-model",
            model_settings=ModelSettings(parallel_tool_calls=False),
        )
        configured_runner = build_planner_agent(
            model="test-model",
            model_settings=ModelSettings(parallel_tool_calls=False),
            collectors=collectors,
        )
        evidence_result = mock.Mock()
        evidence_result.new_items = []
        evidence_history = [{"role": "user", "content": "evidence"}]
        evidence_result.to_input_list.return_value = evidence_history
        commit_result = mock.Mock()
        context = mock.Mock()
        collector_results = [
            CollectorResult(
                collector_code="document_collector",
                summary="文档已确认",
                evidence_items=[
                    EvidenceItem(
                        tool_name="get_document",
                        tool_call_id="call-1",
                        original_tool_call_id="call-1",
                        attempt_count=1,
                        arguments={"document_id": 7},
                        outcome="succeeded",
                        result_code="document_retrieved",
                        message="文档读取成功",
                        retryable=False,
                        resource_refs=["document:7"],
                        payload={"document": {"id": 7}},
                    )
                ],
            )
        ]

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(side_effect=[evidence_result, commit_result]),
        ) as run, mock.patch(
            "app.agents.planner.extract_collector_results",
            return_value=collector_results,
        ):
            result = await configured_runner.run(
                planner_input=PlannerContextInput(
                    current_user_input="处理文档 7",
                    context_chains=[],
                ),
                context=context,
            )

        self.assertIs(result, commit_result)
        self.assertEqual(run.await_count, 2)
        evidence_call, commit_call = run.await_args_list
        self.assertIs(evidence_call.args[0], configured_runner.evidence_agent)
        self.assertIn(
            '"current_user_input": "处理文档 7"',
            evidence_call.args[1],
        )
        self.assertIn('"context_chains": []', evidence_call.args[1])
        self.assertIs(commit_call.args[0], configured_runner.commit_agent)
        self.assertIs(commit_call.args[1], evidence_history)
        self.assertIs(
            commit_call.kwargs["run_config"],
            configured_runner.commit_run_config,
        )
        evidence_result.to_input_list.assert_called_once_with()

    async def test_context_ready_turn_runs_planner_and_publishes_plan(
        self,
    ) -> None:
        ports = PlanningApplicationPorts(
            uow_factory=lambda: SQLAlchemyUnitOfWork(self.session_factory),
            plan_factory=Plan,
            task_factory=Task,
            task_dependency_factory=TaskDependency,
            outbox_event_factory=OutboxEvent,
            inbox_event_factory=InboxEvent,
            clarification_request_factory=ClarificationRequest,
            integrity_error_type=IntegrityError,
        )
        planning_use_cases = build_planning_use_cases(ports)
        closed_loop_runner = _ClosedLoopPlannerRunner()
        resource_service = mock.Mock()
        resource_service.empty_queue.return_value = ContextResourceQueue(
            capacity=16,
            items=[],
        )
        resource_service.get_queue = mock.AsyncMock(
            return_value=ContextResourceQueue(capacity=16, items=[])
        )
        run_planning = RunPlanningUseCase(
            ports=ports,
            planning_use_cases=planning_use_cases,
            planner_runner=closed_loop_runner,
            document_services=_mock_document_services(),
            context_services=ContextToolServices(),
            context_resource_service=resource_service,
            context_chain_mapper=build_context_chain,
            operations_services=OperationsToolServices(),
            audit_logger_factory=lambda: AgentToolAuditLogger(_AuditWriter()),
        )

        result = await run_planning.execute(
            RunPlanningInput(
                conversation_id="conversation-1",
                turn_id="turn-planner-1",
            )
        )

        self.assertEqual(closed_loop_runner.max_active_collectors, 3)
        self.assertEqual(
            closed_loop_runner.planner_input.current_user_input,
            "处理、切块并索引文档 7",
        )
        self.assertEqual(
            [
                chain.chain_id
                for chain in closed_loop_runner.planner_input.context_chains
            ],
            ["chain-a", "chain-b"],
        )
        self.assertEqual(
            [
                chain.nodes[0].turn.user_input
                for chain in closed_loop_runner.planner_input.context_chains
            ],
            ["历史上下文 A", "历史上下文 B"],
        )
        self.assertEqual(
            closed_loop_runner.agent_context.allowed_context_chain_ids,
            frozenset({"chain-a", "chain-b"}),
        )
        self.assertEqual(
            closed_loop_runner.agent_context.allowed_context_turn_ids,
            frozenset(
                {"turn-planner-1", "turn-history-a", "turn-history-b"}
            ),
        )
        self.assertEqual(result.status.value, "ready")
        self.assertEqual(len(result.task_ids), 3)
        self.assertIsNone(result.failure_reason)
        with self.session_factory() as session:
            plan = session.get(Plan, result.plan_id)
            tasks = (
                session.query(Task)
                .filter(Task.plan_id == result.plan_id)
                .order_by(Task.sequence.asc())
                .all()
            )
            turn = session.get(ConversationTurn, "turn-planner-1")
            self.assertEqual(plan.status, "ready")
            self.assertEqual(
                [task.status for task in tasks],
                ["pending", "pending", "pending"],
            )
            self.assertEqual(turn.task_ids, result.task_ids)


if __name__ == "__main__":
    unittest.main()
