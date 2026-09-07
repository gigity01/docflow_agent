"""HTTP、Outbox、Redis Stream、Runtime Worker 与 Aggregation 端到端闭环集成测试。

核心业务不变量（遵循 AGENTS.md 规范）：
1. 异步任务编排与可靠消息闭环：
   - HTTP Message 接入 -> Context 路由选择 -> Planner 发布 Plan、Task DAG 与 Outbox Event (`runtime.plan_wakeup`)。
   - Outbox Publisher 扫描 Outbox 投递至 Redis Stream；
   - Redis Stream 提供至少一次投递，业务处理方通过 Inbox 与状态机幂等抑制已完成事件的重复执行。
2. Task 执行与三段式短事务边界：
   - Claim 阶段：严格按 DAG 依赖关系领取 Task，写入 execution 快照与 operation ownership；
   - 事务外 Executor：分派至 capability-scoped Agent Executor 或 deterministic 后备 Executor；
   - Completion 阶段：更新 Task 为 succeeded，并在最后一个 Task 完成时触发 `runtime.plan_completed` 事件。
3. 结果聚合与上下文回写（Aggregation）：
   - Aggregator 消费聚合事件，基于已完成 Task 结果生成确定性执行摘要，调用 ContextService 完成 Turn 并原子更新链节点与资源队列。
4. 澄清与 Replan（Clarification / Replan）：
   - 澄清触发生成 ClarificationRequest -> 用户携带 source_turn_id 回复 -> 触发 Replan 并发布新 revision Plan -> 聚合后 Clarification 转为 resolved。
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace
import json
import os
from types import SimpleNamespace
import unittest
from unittest import mock

import httpx
from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.config import environment


with (
    mock.patch.object(environment, "load_local_env_file", lambda _: None),
    mock.patch.dict(
        os.environ,
        {
            "SQLALCHEMY_DATABASE_URL": "sqlite+pysqlite:///:memory:",
            "DASHSCOPE_API_KEY": "runtime-worker-test-placeholder",
        },
    ),
):
    from agents import ModelSettings, RunContextWrapper

    from app.agent_runtime.audit import AgentToolAuditLogger
    from app.agent_runtime.context import (
        ContextToolServices,
        DocumentToolServices,
        OperationsToolServices,
    )
    from app.agents.document_executors import (
        build_document_executor_agents,
    )
    from app.infrastructure.database.base import Base
    from app.infrastructure.database.model_registry import load_all_models
    from app.infrastructure.database.uow import SQLAlchemyUnitOfWork
    from app.modules.aggregation.application.aggregate_plan import (
        AggregatePlanUseCase,
    )
    from app.modules.clarification.application.answer import (
        AnswerClarificationUseCase,
    )
    from app.modules.clarification.infrastructure.persistence.models import (
        ClarificationRequest,
    )
    from app.modules.context.application.context_service import ContextService
    from app.modules.context.application.resource_service import (
        ContextResourceService,
    )
    from app.modules.context.domain.models import (
        ContextResourceQueue,
        ContextSelectionDecision,
    )
    from app.modules.context.infrastructure.persistence.mapper import (
        SQLAlchemyContextRecordFactory,
        build_context_chain,
    )
    from app.modules.context.infrastructure.persistence.models.conversation_turn import (
        ConversationTurn,
    )
    from app.modules.context.infrastructure.persistence.models.context_chain import (
        ContextChain,
    )
    from app.modules.context.infrastructure.persistence.models.context_chain_node import (
        ContextChainNode,
    )
    from app.modules.context.infrastructure.persistence.models.context_resource import (
        ContextChainResource,
    )
    from app.modules.context.infrastructure.persistence.models.context_resource_event import (
        ContextChainResourceEvent,
    )
    from app.modules.context.infrastructure.persistence.models.context_selection_record import (
        ContextSelectionRecord,
    )
    from app.modules.conversation.application.send_message import (
        SendConversationMessageUseCase,
    )
    from app.modules.conversation.presentation.dependencies import (
        get_send_conversation_message,
    )
    from app.modules.conversation.presentation.router import router
    from app.modules.document.application.errors import DocumentApplicationError
    from app.modules.document.agent_tools.command_tools import (
        DOCUMENT_BUILD_CHUNKS_PERMISSION,
        DOCUMENT_INDEX_VECTORS_PERMISSION,
        DOCUMENT_PROCESS_PERMISSION,
        build_document_chunks_handler,
        index_document_vectors_handler,
        process_document_handler,
    )
    from app.modules.document.agent_tools.query_tools import (
        DOCUMENT_READ_PERMISSION,
    )
    from app.modules.document.agent_tools.schemas import (
        BuildDocumentChunksToolInput,
        BuildDocumentChunksToolOutput,
        IndexDocumentVectorsToolInput,
        IndexDocumentVectorsToolOutput,
        ProcessDocumentToolInput,
        ProcessDocumentToolOutput,
    )
    from app.modules.messaging.application.outbox import OutboxPublisher
    from app.modules.messaging.infrastructure.persistence.models import (
        InboxEvent,
        OutboxEvent,
    )
    from app.modules.messaging.infrastructure.redis_streams import (
        RedisStreamPublisher,
        RedisStreamWorker,
    )
    from app.modules.messaging.worker.dispatcher import RuntimeEventDispatcher
    from app.modules.planning.application.dto import (
        CreateBuildChunksTaskInput,
        CreateIndexVectorsTaskInput,
        CreateProcessDocumentTaskInput,
        FinalizePlanInput,
    )
    from app.modules.planning.application.ports import PlanningApplicationPorts
    from app.modules.planning.application.replan import ReplanUseCase
    from app.modules.planning.application.run_planning import RunPlanningUseCase
    from app.modules.planning.application.use_cases import (
        build_planning_use_cases,
    )
    from app.modules.planning.infrastructure.persistence.models import (
        Plan,
        Task,
        TaskDependency,
    )
    from app.modules.task_runtime.application.ports import (
        CompensatorRegistry,
        ExecutorRegistry,
        TaskRuntimePorts,
    )
    from app.modules.task_runtime.application.registry import (
        build_capability_registry,
    )
    from app.modules.task_runtime.application.runtime import TaskRuntimeService
    from app.modules.task_runtime.infrastructure.executors import (
        AgentTaskExecutor,
        adapt_build_document_chunks_output,
        adapt_index_document_vectors_output,
        adapt_process_document_output,
    )
    from app.modules.task_runtime.infrastructure.persistence.models import (
        TaskExecution,
    )


class _RouteLockManager:
    @asynccontextmanager
    async def hold(self, _conversation_id: str):
        yield


class _ContextRouter:
    async def route(self, _agent_input):
        return ContextSelectionDecision(
            relevant_chain_ids=[],
            reason_summary="端到端测试不需要历史上下文。",
        )


class _ResourceQueueRepository:
    capacity = 16

    def __init__(self) -> None:
        self.queues = {}
        self.versions = {}

    async def get(
        self,
        *,
        conversation_id: str,
        chain_id: str,
        expected_version: int,
    ):
        key = (conversation_id, chain_id)
        if self.versions.get(key) != expected_version:
            return None
        return ContextResourceQueue(
            capacity=self.capacity,
            items=list(self.queues.get(key, [])),
        )

    async def replace(
        self,
        *,
        conversation_id: str,
        chain_id: str,
        resources,
        database_version: int,
    ) -> None:
        key = (conversation_id, chain_id)
        self.queues[key] = list(resources)
        self.versions[key] = database_version

    async def refresh(
        self,
        *,
        conversation_id: str,
        chain_id: str,
        resources,
        removed_resource_keys,
        expected_previous_version: int,
        database_version: int,
    ) -> bool:
        key = (conversation_id, chain_id)
        if self.versions.get(key, 0) != expected_previous_version:
            return False
        queue = [
            item
            for item in self.queues.get(key, [])
            if item.resource_key not in set(removed_resource_keys)
        ]
        for resource in resources:
            queue = [
                item
                for item in queue
                if item.resource_key != resource.resource_key
            ]
            queue.append(resource)
        self.queues[key] = queue[-self.capacity :]
        self.versions[key] = database_version
        return True

    async def invalidate(
        self,
        *,
        conversation_id: str,
        chain_id: str,
    ) -> None:
        key = (conversation_id, chain_id)
        self.queues.pop(key, None)
        self.versions.pop(key, None)


class _InMemoryRedisStreams:
    """只实现本测试所需的 Redis Streams consumer group 语义。"""

    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, str]]] = []
        self.delivered: set[str] = set()
        self.pending: dict[str, str] = {}
        self.acknowledged: list[str] = []

    async def xadd(self, _stream_name: str, fields: dict[str, str]):
        message_id = f"{len(self.messages) + 1}-0"
        self.messages.append((message_id, dict(fields)))
        return message_id

    async def xgroup_create(self, *args, **kwargs):
        return True

    async def xautoclaim(self, *args, **kwargs):
        return ["0-0", []]

    async def xreadgroup(self, group_name, consumer_name, streams, **kwargs):
        del group_name, kwargs
        stream_name, stream_id = next(iter(streams.items()))
        if stream_id == "0":
            selected = [
                message
                for message in self.messages
                if self.pending.get(message[0]) == consumer_name
            ]
        else:
            selected = [
                message
                for message in self.messages
                if message[0] not in self.delivered
            ]
            for message_id, _ in selected:
                self.delivered.add(message_id)
                self.pending[message_id] = consumer_name
        return [(stream_name, selected)] if selected else []

    async def xack(self, _stream_name, _group_name, message_id):
        self.pending.pop(message_id, None)
        self.acknowledged.append(message_id)

    @property
    def event_types(self) -> list[str]:
        return [fields["event_type"] for _, fields in self.messages]


class _AuditWriter:
    def write(self, _event: dict) -> bool:
        return True


def _unused_document_tool_services() -> DocumentToolServices:
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


class _PlannerRunner:
    def __init__(self, capability_codes: tuple[str, ...]) -> None:
        self._capability_codes = capability_codes

    async def run(self, *, planner_input, context):
        del planner_input
        services = context.planning_services
        if services is None or context.plan_id is None or context.turn_id is None:
            raise AssertionError("Planner 测试上下文不完整")
        previous_ref: str | None = None
        for sequence, capability_code in enumerate(
            self._capability_codes,
            start=1,
        ):
            task_ref = f"task-{sequence}"
            common = {
                "plan_id": context.plan_id,
                "turn_id": context.turn_id,
                "document_id": 1,
                "sequence": sequence,
                "task_ref": task_ref,
                "depends_on_task_refs": (
                    [] if previous_ref is None else [previous_ref]
                ),
            }
            if capability_code == "process_document":
                services.create_process_document_task.execute(
                    CreateProcessDocumentTaskInput(**common)
                )
            elif capability_code == "build_document_chunks":
                services.create_build_chunks_task.execute(
                    CreateBuildChunksTaskInput(**common)
                )
            elif capability_code == "index_document_vectors":
                services.create_index_vectors_task.execute(
                    CreateIndexVectorsTaskInput(**common)
                )
            else:
                raise AssertionError(f"未知测试 Capability: {capability_code}")
            previous_ref = task_ref
        services.finalize_plan.execute(
            FinalizePlanInput(
                plan_id=context.plan_id,
                turn_id=context.turn_id,
            )
        )
        return SimpleNamespace(final_output=None)


class _DocumentUseCase:
    def __init__(
        self,
        operation: str,
        calls: list[str],
        *,
        failure_statuses: list[int] | None = None,
    ) -> None:
        self._operation = operation
        self._calls = calls
        self._failure_statuses = list(failure_statuses or [])

    def execute(self, document_id: int, *, operation_context):
        del operation_context
        self._calls.append(self._operation)
        if self._failure_statuses:
            status_code = self._failure_statuses.pop(0)
            raise DocumentApplicationError(
                status_code,
                f"{self._operation} test failure",
            )
        if self._operation == "process":
            return SimpleNamespace(
                document_id=document_id,
                status="processed",
                cleaned_uri=f"cleaned/{document_id}.txt",
            )
        if self._operation == "build_chunks":
            return SimpleNamespace(
                document_id=document_id,
                status="chunked",
                parent_count=1,
                child_count=2,
            )
        return SimpleNamespace(
            document_id=document_id,
            status="indexed",
            total_chunks=2,
            indexed_chunks=2,
            failed_chunks=0,
        )


class _RuntimeExecutorAgentRunner:
    """离线执行 Agent 已选定的唯一 Command Tool。"""

    async def __call__(self, _agent, agent_input, **kwargs):
        data = json.loads(agent_input)
        payload = data["task_payload"]
        context = RunContextWrapper(kwargs["context"])
        handlers = {
            "document.process": (
                process_document_handler,
                ProcessDocumentToolInput,
            ),
            "document.build_chunks": (
                build_document_chunks_handler,
                BuildDocumentChunksToolInput,
            ),
            "document.index_vectors": (
                index_document_vectors_handler,
                IndexDocumentVectorsToolInput,
            ),
        }
        handler, input_model = handlers[data["executor_code"]]
        return SimpleNamespace(
            final_output=handler(context, input_model.model_validate(payload))
        )


class _NoopOperationCompensator:
    def __init__(self, code: str, calls: list[tuple[str, str, int]]) -> None:
        self._code = code
        self._calls = calls

    async def compensate(self, *, operation_id, payload, context) -> None:
        del context
        self._calls.append((self._code, operation_id, payload.document_id))


class RuntimeWorkerEndToEndTest(unittest.IsolatedAsyncioTestCase):
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
            ContextSelectionRecord.__table__,
            ContextChainNode.__table__,
            ContextChainResource.__table__,
            ContextChainResourceEvent.__table__,
            Plan.__table__,
            Task.__table__,
            TaskDependency.__table__,
            TaskExecution.__table__,
            OutboxEvent.__table__,
            InboxEvent.__table__,
            ClarificationRequest.__table__,
        ]
        Base.metadata.create_all(self.engine, tables=self.tables)
        self.uow_factory = lambda: SQLAlchemyUnitOfWork(self.session_factory)

    async def asyncTearDown(self) -> None:
        self.engine.dispose()

    def _build_runtime(
        self,
        capability_codes: tuple[str, ...],
        *,
        process_failure_statuses: list[int] | None = None,
    ):
        queue_repository = _ResourceQueueRepository()
        record_factory = SQLAlchemyContextRecordFactory()
        resource_service = ContextResourceService(
            queue_repository=queue_repository,
            uow_factory=self.uow_factory,
            record_factory=record_factory,
        )
        context_service = ContextService(
            agent_router=_ContextRouter(),
            route_lock_manager=_RouteLockManager(),
            resource_service=resource_service,
            uow_factory=self.uow_factory,
            record_factory=record_factory,
            chain_mapper=build_context_chain,
        )
        planning_ports = PlanningApplicationPorts(
            uow_factory=self.uow_factory,
            plan_factory=Plan,
            task_factory=Task,
            task_dependency_factory=TaskDependency,
            outbox_event_factory=OutboxEvent,
            inbox_event_factory=InboxEvent,
            clarification_request_factory=ClarificationRequest,
            integrity_error_type=IntegrityError,
        )
        planning_use_cases = build_planning_use_cases(planning_ports)
        run_planning = RunPlanningUseCase(
            ports=planning_ports,
            planning_use_cases=planning_use_cases,
            planner_runner=_PlannerRunner(capability_codes),
            document_services=_unused_document_tool_services(),
            context_services=ContextToolServices(),
            context_resource_service=resource_service,
            context_chain_mapper=build_context_chain,
            operations_services=OperationsToolServices(),
            audit_logger_factory=lambda: AgentToolAuditLogger(_AuditWriter()),
        )
        answer_clarification = AnswerClarificationUseCase(
            uow_factory=self.uow_factory,
            outbox_event_factory=OutboxEvent,
        )
        send_message = SendConversationMessageUseCase(
            context_service=context_service,
            run_planning=run_planning,
            answer_clarification=answer_clarification,
        )

        calls: list[str] = []
        process = _DocumentUseCase(
            "process",
            calls,
            failure_statuses=process_failure_statuses,
        )
        build_chunks = _DocumentUseCase("build_chunks", calls)
        index_vectors = _DocumentUseCase("index_vectors", calls)
        executor_services = replace(
            _unused_document_tool_services(),
            process_document=process,
            build_chunks=build_chunks,
            index_vectors=index_vectors,
        )
        executor_agents = build_document_executor_agents(
            model="test-model",
            model_settings=ModelSettings(parallel_tool_calls=True),
        )
        executor_runner = _RuntimeExecutorAgentRunner()
        compensation_calls: list[tuple[str, str, int]] = []
        task_runtime = TaskRuntimeService(
            ports=TaskRuntimePorts(
                uow_factory=self.uow_factory,
                task_execution_factory=TaskExecution,
                outbox_event_factory=OutboxEvent,
                inbox_event_factory=InboxEvent,
            ),
            capabilities=build_capability_registry(),
            executors=ExecutorRegistry(
                {
                    "document.process": AgentTaskExecutor(
                        agent=executor_agents.process,
                        executor_code="document.process",
                        primary_tool_name="process_document",
                        tool_output_model=ProcessDocumentToolOutput,
                        output_adapter=adapt_process_document_output,
                        document_services=executor_services,
                        permissions=frozenset(
                            {
                                DOCUMENT_READ_PERMISSION,
                                DOCUMENT_PROCESS_PERMISSION,
                            }
                        ),
                        run_config=executor_agents.run_config,
                        runner=executor_runner,
                        audit_logger_factory=lambda: AgentToolAuditLogger(
                            _AuditWriter()
                        ),
                    ),
                    "document.build_chunks": AgentTaskExecutor(
                        agent=executor_agents.build_chunks,
                        executor_code="document.build_chunks",
                        primary_tool_name="build_document_chunks",
                        tool_output_model=BuildDocumentChunksToolOutput,
                        output_adapter=adapt_build_document_chunks_output,
                        document_services=executor_services,
                        permissions=frozenset(
                            {
                                DOCUMENT_READ_PERMISSION,
                                DOCUMENT_BUILD_CHUNKS_PERMISSION,
                            }
                        ),
                        run_config=executor_agents.run_config,
                        runner=executor_runner,
                        audit_logger_factory=lambda: AgentToolAuditLogger(
                            _AuditWriter()
                        ),
                    ),
                    "document.index_vectors": AgentTaskExecutor(
                        agent=executor_agents.index_vectors,
                        executor_code="document.index_vectors",
                        primary_tool_name="index_document_vectors",
                        tool_output_model=IndexDocumentVectorsToolOutput,
                        output_adapter=adapt_index_document_vectors_output,
                        document_services=executor_services,
                        permissions=frozenset(
                            {
                                DOCUMENT_READ_PERMISSION,
                                DOCUMENT_INDEX_VECTORS_PERMISSION,
                            }
                        ),
                        run_config=executor_agents.run_config,
                        runner=executor_runner,
                        audit_logger_factory=lambda: AgentToolAuditLogger(
                            _AuditWriter()
                        ),
                    ),
                }
            ),
            compensators=CompensatorRegistry(
                {
                    "document.process": _NoopOperationCompensator(
                        "document.process",
                        compensation_calls,
                    ),
                    "document.build_chunks": _NoopOperationCompensator(
                        "document.build_chunks",
                        compensation_calls,
                    ),
                    "document.index_vectors": _NoopOperationCompensator(
                        "document.index_vectors",
                        compensation_calls,
                    ),
                }
            ),
            retry_delay_seconds=0,
        )
        aggregate = AggregatePlanUseCase(
            uow_factory=self.uow_factory,
            context_service=context_service,
        )
        replan = ReplanUseCase(
            ports=planning_ports,
            run_planning=run_planning,
        )
        dispatcher = RuntimeEventDispatcher(
            uow_factory=self.uow_factory,
            inbox_event_factory=InboxEvent,
            runtime=task_runtime,
            replan=replan,
            aggregate_plan=aggregate,
        )
        redis = _InMemoryRedisStreams()
        publisher = OutboxPublisher(
            uow_factory=self.uow_factory,
            publisher=RedisStreamPublisher(redis),
        )
        worker = RedisStreamWorker(
            redis,
            dispatcher=dispatcher,
            consumer_name="runtime-worker-e2e",
        )
        return SimpleNamespace(
            send_message=send_message,
            publisher=publisher,
            worker=worker,
            redis=redis,
            calls=calls,
            compensation_calls=compensation_calls,
        )

    async def _post_message(self, send_message, message: str):
        app = FastAPI()
        app.include_router(router, prefix="/api")

        async def get_use_case():
            return send_message

        app.dependency_overrides[get_send_conversation_message] = get_use_case
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.post(
                "/api/conversations/conversation-e2e/messages",
                json={"message": message},
            )

    async def _drain_until_completed(self, runtime, turn_id: str) -> None:
        for _ in range(50):
            await runtime.publisher.publish_batch()
            await runtime.worker.run_once(block_milliseconds=0)
            with self.session_factory() as session:
                turn = session.get(ConversationTurn, turn_id)
                if turn is not None and turn.status == "completed":
                    return
        self.fail("Worker 未在限定轮次内完成 Turn")

    async def test_single_task_completes_from_http_to_aggregation(self) -> None:
        runtime = self._build_runtime(("process_document",))
        response = await self._post_message(
            runtime.send_message,
            "处理 document_id=1",
        )

        self.assertEqual(response.status_code, 202)
        body = response.json()
        with self.session_factory() as session:
            initial_plan = session.get(Plan, body["plan_id"])
            initial_task = session.get(Task, body["task_ids"][0])
            initial_outbox = session.query(OutboxEvent).filter(OutboxEvent.event_type == "runtime.plan_wakeup").one()
            self.assertEqual(initial_plan.status, "ready")
            self.assertEqual(initial_task.status, "pending")
            self.assertEqual(initial_outbox.status, "pending")

        await self._drain_until_completed(runtime, body["turn_id"])

        with self.session_factory() as session:
            plan = session.get(Plan, body["plan_id"])
            task = session.get(Task, body["task_ids"][0])
            turn = session.get(ConversationTurn, body["turn_id"])
            first_outbox = session.get(OutboxEvent, initial_outbox.event_id)
            self.assertEqual(first_outbox.status, "published")
            self.assertEqual(task.status, "succeeded")
            self.assertEqual(plan.status, "completed")
            self.assertEqual(turn.status, "completed")
            self.assertIn("已完成 1 项任务", turn.assistant_content)
        self.assertEqual(runtime.calls, ["process"])
        self.assertIn("runtime.plan_wakeup", runtime.redis.event_types)
        self.assertIn("aggregation.requested", runtime.redis.event_types)

    async def test_three_task_dag_runs_in_dependency_order(self) -> None:
        runtime = self._build_runtime(
            (
                "process_document",
                "build_document_chunks",
                "index_document_vectors",
            )
        )
        response = await self._post_message(
            runtime.send_message,
            "依次处理、切块并索引 document_id=1",
        )
        body = response.json()

        await self._drain_until_completed(runtime, body["turn_id"])

        self.assertEqual(
            runtime.calls,
            ["process", "build_chunks", "index_vectors"],
        )
        with self.session_factory() as session:
            tasks = (
                session.query(Task)
                .filter(Task.plan_id == body["plan_id"])
                .order_by(Task.sequence)
                .all()
            )
            dependencies = session.query(TaskDependency).all()
            turn = session.get(ConversationTurn, body["turn_id"])
            self.assertEqual(
                [task.status for task in tasks],
                ["succeeded", "succeeded", "succeeded"],
            )
            self.assertEqual(len(dependencies), 2)
            self.assertEqual(turn.status, "completed")
            self.assertIn("已完成 3 项任务", turn.assistant_content)

    async def test_executor_failure_retries_then_replans(self) -> None:
        runtime = self._build_runtime(
            ("process_document",),
            process_failure_statuses=[503, 409],
        )
        response = await self._post_message(
            runtime.send_message,
            "处理 document_id=1，失败时重试并重新规划",
        )
        body = response.json()

        await self._drain_until_completed(runtime, body["turn_id"])

        with self.session_factory() as session:
            plans = (
                session.query(Plan)
                .filter(Plan.turn_id == body["turn_id"])
                .order_by(Plan.revision)
                .all()
            )
            old_executions = (
                session.query(TaskExecution)
                .filter(TaskExecution.plan_id == plans[0].plan_id)
                .order_by(TaskExecution.attempt)
                .all()
            )
            new_executions = (
                session.query(TaskExecution)
                .filter(TaskExecution.plan_id == plans[1].plan_id)
                .all()
            )
            turn = session.get(ConversationTurn, body["turn_id"])
            self.assertEqual(
                [plan.status for plan in plans],
                ["superseded", "completed"],
            )
            self.assertEqual(
                [execution.status for execution in old_executions],
                ["compensated", "compensated"],
            )
            self.assertEqual(
                [execution.retryable for execution in old_executions],
                [True, False],
            )
            self.assertEqual(new_executions[0].status, "succeeded")
            self.assertEqual(turn.status, "completed")
        self.assertEqual(runtime.calls, ["process", "process", "process"])
        self.assertEqual(
            [call[0] for call in runtime.compensation_calls],
            ["document.process", "document.process"],
        )
        self.assertIn("planning.replan_requested", runtime.redis.event_types)

    async def test_failure_compensates_then_retry_succeeds(self) -> None:
        runtime = self._build_runtime(
            ("process_document",),
            process_failure_statuses=[503],
        )
        response = await self._post_message(
            runtime.send_message,
            "处理 document_id=1，暂时失败后重试",
        )
        body = response.json()

        await self._drain_until_completed(runtime, body["turn_id"])

        with self.session_factory() as session:
            plan = session.get(Plan, body["plan_id"])
            task = session.get(Task, body["task_ids"][0])
            executions = (
                session.query(TaskExecution)
                .filter(TaskExecution.plan_id == plan.plan_id)
                .order_by(TaskExecution.attempt)
                .all()
            )
            self.assertEqual(plan.status, "completed")
            self.assertEqual(task.status, "succeeded")
            self.assertEqual(
                [execution.status for execution in executions],
                ["compensated", "succeeded"],
            )
            self.assertNotEqual(
                executions[0].operation_id,
                executions[1].operation_id,
            )
            self.assertEqual(
                runtime.compensation_calls,
                [
                    (
                        "document.process",
                        executions[0].operation_id,
                        1,
                    )
                ],
            )
        self.assertEqual(runtime.calls, ["process", "process"])


if __name__ == "__main__":
    unittest.main()
