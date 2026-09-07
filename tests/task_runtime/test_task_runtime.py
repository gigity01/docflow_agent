"""Task Runtime 任务领取（Claim）、事务外执行、完成/失败推进与确定性补偿恢复测试。

核心业务不变量（遵循 AGENTS.md 规范）：
1. 单 Plan 串行度与 DAG 拓扑感知（Claim）：
   - 同一 Plan 并发度固定为 1；只有所有依赖 Task 均已成功的 Task 才可被领取。
   - 领取时持久化输入快照、attempt、execution_id 与 active_operation_id。
2. 三段式状态机与副作用补偿（Claim -> Executor -> Completion/Compensate）：
   - 执行失败时进入 `compensation_required -> begin attempt COMMIT -> 事务外 Compensator -> compensated`。
   - 只有补偿成功才进入 retry 或 replan；若补偿多次失败达到上限则进入 `compensation_locked`，禁止新 attempt 接管。
   - 全部 Task 完成时触发 `runtime.plan_completed` 事件，交由 Aggregator 处理。
"""

from __future__ import annotations

from app.shared.time import utc_now

import asyncio
import threading
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.infrastructure.database.base import Base
from app.infrastructure.database.model_registry import load_all_models
from app.infrastructure.database.uow import SQLAlchemyUnitOfWork
from app.modules.context.infrastructure.persistence.models.conversation_turn import (
    ConversationTurn,
)
from app.modules.document.infrastructure.persistence.models.document import (
    Document,
)
from app.modules.document.infrastructure.persistence.models.knowledge_base import (
    KnowledgeBase,
)
from app.modules.messaging.infrastructure.persistence.models import OutboxEvent
from app.modules.messaging.infrastructure.persistence.models import InboxEvent
from app.modules.planning.domain.enums import (
    PlanningCapabilityCode,
    PlanStatus,
    TaskStatus,
)
from app.modules.planning.infrastructure.persistence.models import (
    Plan,
    Task,
    TaskDependency,
)
from app.modules.task_runtime.application.dto import (
    ClaimNextTaskInput,
    TaskExecutorResult,
)
from app.modules.task_runtime.application.errors import TaskExecutionError
from app.modules.task_runtime.application.ports import (
    CapabilityDefinition,
    CapabilityRegistry,
    CompensatorRegistry,
    ExecutorRegistry,
    TaskRuntimePorts,
)
from app.modules.task_runtime.application.registry import (
    build_capability_registry,
)
from app.modules.task_runtime.application.runtime import TaskRuntimeService
from app.modules.task_runtime.application.schemas import (
    ProcessDocumentTaskOutput,
    ProcessDocumentTaskPayload,
)
from app.modules.task_runtime.infrastructure.executors.document import (
    DeterministicProcessDocumentExecutor,
)
from app.modules.task_runtime.infrastructure.persistence.models import (
    TaskExecution,
)


class _ProcessExecutor:
    def __init__(self) -> None:
        self.errors: list[TaskExecutionError] = []
        self.on_execute = None

    async def execute(self, payload, context) -> TaskExecutorResult:
        if self.on_execute is not None:
            self.on_execute(context)
        if self.errors:
            raise self.errors.pop(0)
        return TaskExecutorResult(
            output_json={
                "document_id": payload.document_id,
                "status": "processed",
                "cleaned_uri": f"cleaned/{payload.document_id}.txt",
            },
            resource_refs=[f"document:{payload.document_id}"],
        )


class _NoopCompensator:
    def __init__(self) -> None:
        self.calls = []
        self.error: Exception | None = None
        self.on_call = None

    async def compensate(self, *, operation_id, payload, context) -> None:
        self.calls.append((operation_id, payload.document_id, context))
        if self.on_call is not None:
            self.on_call()
        if self.error is not None:
            raise self.error


class _BlockingFilesystemUseCase:
    def __init__(self, side_effect_path: Path, order: list[str]) -> None:
        self._side_effect_path = side_effect_path
        self._order = order
        self.started = threading.Event()
        self.allow_side_effect = threading.Event()
        self.side_effect_completed = threading.Event()

    def execute(self, document_id, *, operation_context):
        del operation_context
        self.started.set()
        if not self.allow_side_effect.wait(timeout=2):
            raise TimeoutError("test did not release blocking use case")
        self._side_effect_path.write_text("late side effect", encoding="utf-8")
        self._order.append("use_case_side_effect")
        self.side_effect_completed.set()
        return SimpleNamespace(
            document_id=document_id,
            status="processed",
            cleaned_uri=str(self._side_effect_path),
        )


class _FilesystemCleanupCompensator:
    def __init__(self, side_effect_path: Path, order: list[str]) -> None:
        self._side_effect_path = side_effect_path
        self._order = order
        self.started = threading.Event()

    async def compensate(self, *, operation_id, payload, context) -> None:
        del operation_id, payload, context
        self._order.append("compensation_started")
        self.started.set()
        if self._side_effect_path.exists():
            self._side_effect_path.unlink()


class TaskRuntimeTest(unittest.IsolatedAsyncioTestCase):
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
            KnowledgeBase.__table__,
            Document.__table__,
            ConversationTurn.__table__,
            Plan.__table__,
            Task.__table__,
            TaskDependency.__table__,
            TaskExecution.__table__,
            OutboxEvent.__table__,
            InboxEvent.__table__,
        ]
        Base.metadata.create_all(self.engine, tables=self.tables)
        with self.session_factory() as session:
            session.add(
                KnowledgeBase(
                    id=1,
                    kb_code="kb-runtime",
                    name="Runtime Test",
                    domain_code="test",
                    embedding_model="test-embedding",
                    vector_collection="runtime-test",
                )
            )
            session.add(
                Document(
                    id=1,
                    doc_code="doc-runtime",
                    kb_id=1,
                    domain_code="test",
                    title="Runtime Test Document",
                    source_type="txt",
                    source_uri="storage/raw/runtime.txt",
                    content_hash="runtime-content-hash",
                    active_content_hash="runtime-content-hash",
                )
            )
            session.add(
                ConversationTurn(
                    turn_id="turn-runtime",
                    conversation_id="conversation-runtime",
                    user_input="依次处理两个文档",
                    task_ids=["task-1", "task-2"],
                    status="processing",
                )
            )
            session.add(
                Plan(
                    plan_id="plan-runtime",
                    workflow_id="workflow-runtime",
                    turn_id="turn-runtime",
                    parent_plan_id=None,
                    current_task_id=None,
                    status=PlanStatus.READY.value,
                    revision=1,
                    failure_code=None,
                    failure_reason=None,
                )
            )
            session.add_all(
                [
                    Task(
                        task_id="task-1",
                        plan_id="plan-runtime",
                        turn_id="turn-runtime",
                        task_ref="first",
                        capability_code=(
                            PlanningCapabilityCode.PROCESS_DOCUMENT.value
                        ),
                        input_json={"document_id": 1},
                        sequence=1,
                        status=TaskStatus.PENDING.value,
                        attempt_count=0,
                        max_attempts=3,
                    ),
                    Task(
                        task_id="task-2",
                        plan_id="plan-runtime",
                        turn_id="turn-runtime",
                        task_ref="second",
                        capability_code=(
                            PlanningCapabilityCode.PROCESS_DOCUMENT.value
                        ),
                        input_json={"document_id": 2},
                        sequence=2,
                        status=TaskStatus.PENDING.value,
                        attempt_count=0,
                        max_attempts=3,
                    ),
                ]
            )
            session.add(
                TaskDependency(
                    dependency_id="dependency-1",
                    plan_id="plan-runtime",
                    task_id="task-2",
                    depends_on_task_id="task-1",
                )
            )
            session.commit()

        self.executor = _ProcessExecutor()
        self.compensator = _NoopCompensator()
        self.runtime = TaskRuntimeService(
            ports=TaskRuntimePorts(
                uow_factory=lambda: SQLAlchemyUnitOfWork(
                    self.session_factory
                ),
                task_execution_factory=TaskExecution,
                outbox_event_factory=OutboxEvent,
                inbox_event_factory=InboxEvent,
            ),
            capabilities=build_capability_registry(),
            executors=ExecutorRegistry(
                {"document.process": self.executor}
            ),
            compensators=CompensatorRegistry(
                {"document.process": self.compensator}
            ),
            retry_delay_seconds=0,
            compensation_retry_delay_seconds=5,
            compensation_retry_max_delay_seconds=20,
        )

    async def asyncTearDown(self) -> None:
        Base.metadata.drop_all(
            self.engine,
            tables=list(reversed(self.tables)),
        )
        self.engine.dispose()

    async def test_tasks_run_in_dependency_order_and_request_aggregation(self) -> None:
        first_claim = self.runtime.claim_next_task(
            ClaimNextTaskInput(plan_id="plan-runtime")
        )
        second_claim = self.runtime.claim_next_task(
            ClaimNextTaskInput(plan_id="plan-runtime")
        )
        self.assertEqual(first_claim.outcome, "claimed")
        self.assertEqual(second_claim.outcome, "already_running")
        self.assertTrue(first_claim.task.agent_run_id.startswith("agent_run_"))
        self.assertEqual(first_claim.task.turn_id, "turn-runtime")
        self.assertEqual(
            first_claim.task.conversation_id,
            "conversation-runtime",
        )
        self.runtime.complete_task(
            first_claim.task,
            {
                "document_id": 1,
                "status": "processed",
                "cleaned_uri": "cleaned/1.txt",
            },
            ["document:1"],
        )

        second = await self.runtime.execute_next("plan-runtime")
        self.assertEqual(second.outcome, "task_succeeded")
        self.assertEqual(second.task_id, "task-2")

        with self.session_factory() as session:
            plan = session.get(Plan, "plan-runtime")
            tasks = session.query(Task).order_by(Task.sequence).all()
            executions = (
                session.query(TaskExecution)
                .order_by(TaskExecution.started_at)
                .all()
            )
            outbox_types = [
                event.event_type
                for event in session.query(OutboxEvent)
                .order_by(OutboxEvent.created_at)
                .all()
            ]
            self.assertEqual(plan.status, "completed")
            self.assertIsNone(plan.current_task_id)
            self.assertEqual(
                [task.status for task in tasks],
                ["succeeded", "succeeded"],
            )
            self.assertEqual([item.attempt for item in executions], [1, 1])
            self.assertEqual(
                executions[0].agent_run_id,
                first_claim.task.agent_run_id,
            )
            self.assertEqual(
                executions[0].resource_refs_json,
                ["document:1"],
            )
            self.assertIn("runtime.plan_wakeup", outbox_types)
            self.assertEqual(outbox_types[-1], "aggregation.requested")

    async def test_stale_execution_is_compensated_before_retry(self) -> None:
        claimed = self.runtime.claim_next_task(
            ClaimNextTaskInput(plan_id="plan-runtime")
        )
        with self.session_factory() as session:
            task = session.get(Task, "task-1")
            task.started_at = utc_now() - timedelta(seconds=301)
            session.commit()

        result = await self.runtime.execute_next("plan-runtime")

        self.assertEqual(result.outcome, "retry_scheduled")
        self.assertEqual(
            self.compensator.calls[0][0],
            claimed.task.operation_id,
        )
        with self.session_factory() as session:
            plan = session.get(Plan, "plan-runtime")
            task = session.get(Task, "task-1")
            execution = session.get(
                TaskExecution,
                claimed.task.execution_id,
            )
            self.assertIsNone(plan.current_task_id)
            self.assertEqual(task.status, "pending")
            self.assertEqual(execution.status, "compensated")
            self.assertEqual(
                execution.error_code,
                "execution_lease_expired",
            )
            self.assertEqual(execution.compensation_attempt_count, 1)
            self.assertIsNotNone(execution.compensation_last_attempt_at)

    async def test_timeout_waits_for_sync_executor_before_compensation(self) -> None:
        with TemporaryDirectory() as temp_dir:
            side_effect_path = Path(temp_dir) / "operation-output.txt"
            order: list[str] = []
            use_case = _BlockingFilesystemUseCase(side_effect_path, order)
            compensator = _FilesystemCleanupCompensator(
                side_effect_path,
                order,
            )
            definition = CapabilityDefinition.model_construct(
                capability_code=(
                    PlanningCapabilityCode.PROCESS_DOCUMENT.value
                ),
                input_model=ProcessDocumentTaskPayload,
                output_model=ProcessDocumentTaskOutput,
                executor_code="document.process",
                compensator_code="document.process",
                max_attempts=3,
                timeout_seconds=0.02,
                side_effect=True,
            )
            runtime = TaskRuntimeService(
                ports=self.runtime._ports,
                capabilities=CapabilityRegistry([definition]),
                executors=ExecutorRegistry(
                    {
                        "document.process": (
                            DeterministicProcessDocumentExecutor(use_case)
                        )
                    }
                ),
                compensators=CompensatorRegistry(
                    {"document.process": compensator}
                ),
                retry_delay_seconds=0,
            )

            execution_task = asyncio.create_task(
                runtime.execute_next("plan-runtime")
            )
            started = await asyncio.to_thread(use_case.started.wait, 1)
            self.assertTrue(started)
            await asyncio.sleep(0.08)
            compensation_started_before_release = compensator.started.is_set()

            use_case.allow_side_effect.set()
            result = await asyncio.wait_for(execution_task, timeout=1)
            side_effect_completed = await asyncio.to_thread(
                use_case.side_effect_completed.wait,
                1,
            )

            self.assertFalse(compensation_started_before_release)
            self.assertTrue(side_effect_completed)
            self.assertEqual(
                order,
                ["use_case_side_effect", "compensation_started"],
            )
            self.assertFalse(side_effect_path.exists())
            self.assertEqual(result.outcome, "retry_scheduled")

    async def test_first_successful_compensation_counts_as_attempt_one(
        self,
    ) -> None:
        self.executor.errors.append(
            TaskExecutionError(
                "temporary_failure",
                "temporary failure",
                retryable=True,
            )
        )
        observed_attempts: list[tuple[str, int, bool]] = []

        def observe_status() -> None:
            with self.session_factory() as session:
                execution = session.query(TaskExecution).one()
                observed_attempts.append(
                    (
                        execution.status,
                        execution.compensation_attempt_count,
                        execution.compensation_last_attempt_at is not None,
                    )
                )

        self.compensator.on_call = observe_status

        result = await self.runtime.execute_next("plan-runtime")

        self.assertEqual(result.outcome, "retry_scheduled")
        self.assertEqual(
            observed_attempts,
            [("compensation_required", 1, True)],
        )
        with self.session_factory() as session:
            plan = session.get(Plan, "plan-runtime")
            task = session.get(Task, "task-1")
            execution = session.query(TaskExecution).one()
            self.assertIsNone(plan.current_task_id)
            self.assertEqual(task.status, "retry_wait")
            self.assertEqual(execution.status, "compensated")
            self.assertEqual(execution.error_code, "temporary_failure")
            self.assertTrue(execution.retryable)
            self.assertFalse(execution.blocked)
            self.assertEqual(execution.compensation_attempt_count, 1)

    async def test_failed_compensation_persists_attempt_before_invocation(
        self,
    ) -> None:
        self.executor.errors.append(
            TaskExecutionError(
                "temporary_failure",
                "temporary failure",
                retryable=True,
            )
        )
        observed_attempts: list[tuple[str, int, bool]] = []

        def observe_attempt() -> None:
            with self.session_factory() as session:
                execution = session.query(TaskExecution).one()
                observed_attempts.append(
                    (
                        execution.status,
                        execution.compensation_attempt_count,
                        execution.compensation_last_attempt_at is not None,
                    )
                )

        self.compensator.on_call = observe_attempt
        self.compensator.error = RuntimeError("compensation failed")

        result = await self.runtime.execute_next("plan-runtime")

        self.assertEqual(result.outcome, "compensation_retry_scheduled")
        self.assertEqual(
            observed_attempts,
            [("compensation_required", 1, True)],
        )

    async def test_compensation_attempt_survives_crash_before_completion(
        self,
    ) -> None:
        claimed = self.runtime.claim_next_task(
            ClaimNextTaskInput(plan_id="plan-runtime")
        )
        error = TaskExecutionError(
            "temporary_failure",
            "temporary failure",
            retryable=True,
        )
        self.runtime._require_compensation(claimed.task, error)

        attempt = self.runtime._begin_compensation_attempt(claimed.task)

        self.assertEqual(attempt, 1)
        with self.session_factory() as session:
            execution = session.query(TaskExecution).one()
            self.assertEqual(execution.status, "compensation_required")
            self.assertEqual(execution.compensation_attempt_count, 1)

        result = await self.runtime.execute_next("plan-runtime")

        self.assertEqual(result.outcome, "retry_scheduled")
        self.assertEqual(len(self.compensator.calls), 1)
        with self.session_factory() as session:
            execution = session.query(TaskExecution).one()
            self.assertEqual(execution.status, "compensated")
            self.assertEqual(execution.compensation_attempt_count, 2)

    async def test_normal_compensation_failure_is_resumable(self) -> None:
        self.executor.errors.append(
            TaskExecutionError(
                "temporary_failure",
                "temporary failure",
                retryable=True,
            )
        )
        self.compensator.error = RuntimeError("compensation failed")

        result = await self.runtime.execute_next("plan-runtime")

        self.assertEqual(result.outcome, "compensation_retry_scheduled")

        with self.session_factory() as session:
            plan = session.get(Plan, "plan-runtime")
            task = session.get(Task, "task-1")
            execution = session.query(TaskExecution).one()
            self.assertEqual(plan.current_task_id, task.task_id)
            self.assertEqual(task.status, "running")
            self.assertEqual(execution.status, "compensation_required")
            self.assertEqual(execution.compensation_attempt_count, 1)
            self.assertEqual(
                execution.compensation_last_error,
                "compensation failed",
            )
            self.assertIsNotNone(execution.compensation_last_attempt_at)
            retry_events = [
                event
                for event in session.query(OutboxEvent).all()
                if event.payload_json.get("execution_id")
                == execution.execution_id
            ]
            self.assertEqual(len(retry_events), 1)
            self.assertEqual(
                retry_events[0].payload_json["operation_id"],
                execution.operation_id,
            )
            self.assertNotIn(
                "compensation_attempt",
                retry_events[0].payload_json,
            )

        self.compensator.error = None
        result = await self.runtime.execute_next(
            "plan-runtime",
            event_id=retry_events[0].event_id,
            compensation_execution_id=execution.execution_id,
            compensation_operation_id=execution.operation_id,
        )

        self.assertEqual(result.outcome, "retry_scheduled")
        self.assertEqual(len(self.compensator.calls), 2)
        with self.session_factory() as session:
            plan = session.get(Plan, "plan-runtime")
            task = session.get(Task, "task-1")
            execution = session.query(TaskExecution).one()
            self.assertIsNone(plan.current_task_id)
            self.assertEqual(task.status, "retry_wait")
            self.assertEqual(execution.status, "compensated")
            self.assertEqual(execution.compensation_attempt_count, 2)
            self.assertEqual(
                execution.compensation_last_error,
                "compensation failed",
            )

        stale_retry = await self.runtime.execute_next(
            "plan-runtime",
            event_id="stale-compensation-retry",
            compensation_execution_id=execution.execution_id,
            compensation_operation_id=execution.operation_id,
        )
        self.assertEqual(stale_retry.outcome, "terminal")
        with self.session_factory() as session:
            task = session.get(Task, "task-1")
            self.assertEqual(task.status, "retry_wait")

    async def test_blocked_disposition_survives_compensation_retry(self) -> None:
        self.executor.errors.append(
            TaskExecutionError(
                "resource_blocked",
                "resource blocked",
                retryable=False,
                blocked=True,
            )
        )
        self.compensator.error = RuntimeError("compensation failed")

        result = await self.runtime.execute_next("plan-runtime")

        self.assertEqual(result.outcome, "compensation_retry_scheduled")

        with self.session_factory() as session:
            execution = session.query(TaskExecution).one()
            retry_event = next(
                event
                for event in session.query(OutboxEvent).all()
                if event.payload_json.get("execution_id")
                == execution.execution_id
            )

        self.compensator.error = None
        result = await self.runtime.execute_next(
            "plan-runtime",
            event_id=retry_event.event_id,
            compensation_execution_id=execution.execution_id,
            compensation_operation_id=execution.operation_id,
        )

        self.assertEqual(result.outcome, "replan_requested")
        with self.session_factory() as session:
            plan = session.get(Plan, "plan-runtime")
            task = session.get(Task, "task-1")
            execution = session.query(TaskExecution).one()
            self.assertEqual(plan.status, "replan_pending")
            self.assertIsNone(plan.current_task_id)
            self.assertEqual(task.status, "blocked")
            self.assertEqual(execution.status, "compensated")
            self.assertTrue(execution.blocked)

    async def test_compensation_failure_keeps_stale_task_owned(self) -> None:
        claimed = self.runtime.claim_next_task(
            ClaimNextTaskInput(plan_id="plan-runtime")
        )
        with self.session_factory() as session:
            task = session.get(Task, "task-1")
            task.started_at = utc_now() - timedelta(seconds=301)
            session.commit()
        self.compensator.error = RuntimeError("compensation failed")

        result = await self.runtime.execute_next("plan-runtime")

        self.assertEqual(result.outcome, "compensation_retry_scheduled")

        with self.session_factory() as session:
            plan = session.get(Plan, "plan-runtime")
            task = session.get(Task, "task-1")
            execution = session.get(
                TaskExecution,
                claimed.task.execution_id,
            )
            self.assertEqual(plan.current_task_id, task.task_id)
            self.assertEqual(task.status, "running")
            self.assertEqual(execution.status, "compensation_required")
            retry_event = next(
                event
                for event in session.query(OutboxEvent).all()
                if event.payload_json.get("execution_id")
                == execution.execution_id
            )
            self.assertGreaterEqual(
                retry_event.available_at,
                utc_now() + timedelta(seconds=4),
            )

    async def test_compensation_retry_uses_exponential_backoff_with_cap(self) -> None:
        self.executor.errors.append(
            TaskExecutionError(
                "temporary_failure",
                "temporary failure",
                retryable=True,
            )
        )
        self.compensator.error = RuntimeError("compensation failed")

        retry_event = None
        for failed_attempt in range(1, 5):
            before = utc_now()
            kwargs = {}
            if retry_event is not None:
                kwargs = {
                    "event_id": retry_event.event_id,
                    "compensation_execution_id": (
                        retry_event.payload_json["execution_id"]
                    ),
                    "compensation_operation_id": (
                        retry_event.payload_json["operation_id"]
                    ),
                }
            result = await self.runtime.execute_next(
                "plan-runtime",
                **kwargs,
            )
            self.assertEqual(
                result.outcome,
                "compensation_retry_scheduled",
            )
            with self.session_factory() as session:
                execution = session.query(TaskExecution).one()
                self.assertEqual(
                    execution.compensation_attempt_count,
                    failed_attempt,
                )
                retry_event = next(
                    item
                    for item in reversed(
                        session.query(OutboxEvent)
                        .order_by(OutboxEvent.created_at).all()
                    )
                    if item.payload_json.get("execution_id")
                    == execution.execution_id
                )
                expected_delay = min(
                    5 * (2 ** (failed_attempt - 1)),
                    20,
                )
                self.assertGreaterEqual(
                    retry_event.available_at,
                    before + timedelta(seconds=expected_delay - 1),
                )
                self.assertLessEqual(
                    retry_event.available_at,
                    utc_now() + timedelta(seconds=expected_delay + 1),
                )

    async def test_compensation_is_locked_after_attempt_limit(self) -> None:
        self.executor.errors.append(
            TaskExecutionError(
                "temporary_failure",
                "temporary failure",
                retryable=True,
            )
        )

        def claim_document(context) -> None:
            with self.session_factory() as session:
                document = session.get(Document, 1)
                document.status = "processing"
                document.active_operation_id = context.operation_id
                session.commit()

        self.executor.on_execute = claim_document
        self.compensator.error = RuntimeError("compensation failed")

        retry_event = None
        result = None
        for failed_attempt in range(1, 6):
            kwargs = {}
            if retry_event is not None:
                kwargs = {
                    "event_id": retry_event.event_id,
                    "compensation_execution_id": (
                        retry_event.payload_json["execution_id"]
                    ),
                    "compensation_operation_id": (
                        retry_event.payload_json["operation_id"]
                    ),
                }
            result = await self.runtime.execute_next("plan-runtime", **kwargs)
            expected_outcome = (
                "compensation_locked"
                if failed_attempt == 5
                else "compensation_retry_scheduled"
            )
            self.assertEqual(result.outcome, expected_outcome)
            with self.session_factory() as session:
                execution = session.query(TaskExecution).one()
                self.assertEqual(
                    execution.compensation_attempt_count,
                    failed_attempt,
                )
                retry_events = [
                    event
                    for event in session.query(OutboxEvent).all()
                    if event.payload_json.get("execution_id")
                    == execution.execution_id
                ]
                if failed_attempt < 5:
                    retry_event = retry_events[-1]

        self.assertIsNotNone(result)
        self.assertEqual(len(self.compensator.calls), 5)
        with self.session_factory() as session:
            plan = session.get(Plan, "plan-runtime")
            task = session.get(Task, "task-1")
            execution = session.query(TaskExecution).one()
            document = session.get(Document, 1)
            retry_events = [
                event
                for event in session.query(OutboxEvent).all()
                if event.payload_json.get("execution_id")
                == execution.execution_id
            ]
            replan_events = [
                event
                for event in session.query(OutboxEvent).all()
                if event.event_type == "planning.replan_requested"
            ]
            self.assertEqual(plan.current_task_id, task.task_id)
            self.assertEqual(plan.status, "running")
            self.assertEqual(task.status, "running")
            self.assertEqual(task.attempt_count, 1)
            self.assertEqual(execution.status, "compensation_locked")
            self.assertEqual(execution.compensation_attempt_count, 5)
            self.assertEqual(
                execution.compensation_last_error,
                "compensation failed",
            )
            self.assertIsNotNone(execution.compensation_last_attempt_at)
            self.assertIsNotNone(execution.compensation_locked_at)
            self.assertEqual(
                execution.compensation_lock_reason,
                "retry_exhausted",
            )
            self.assertIsNone(execution.completed_at)
            self.assertEqual(len(retry_events), 4)
            self.assertEqual(replan_events, [])
            self.assertEqual(document.status, "processing")
            self.assertEqual(
                document.active_operation_id,
                execution.operation_id,
            )

        locked_result = await self.runtime.execute_next(
            "plan-runtime",
            event_id="locked-plan-wakeup",
        )

        self.assertEqual(locked_result.outcome, "compensation_locked")
        self.assertEqual(len(self.compensator.calls), 5)
        with self.session_factory() as session:
            task = session.get(Task, "task-1")
            execution = session.query(TaskExecution).one()
            document = session.get(Document, 1)
            retry_events = [
                event
                for event in session.query(OutboxEvent).all()
                if event.payload_json.get("execution_id")
                == execution.execution_id
            ]
            self.assertEqual(task.status, "running")
            self.assertEqual(task.attempt_count, 1)
            self.assertEqual(execution.status, "compensation_locked")
            self.assertEqual(len(retry_events), 4)
            self.assertEqual(
                document.active_operation_id,
                execution.operation_id,
            )


if __name__ == "__main__":
    unittest.main()
