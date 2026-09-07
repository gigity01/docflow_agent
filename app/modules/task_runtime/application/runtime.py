"""管理任务领取、事务外执行及结果提交，同一 Plan 内串行执行任务。

短事务负责状态变更，耗时执行和补偿在事务外完成。执行归属由
execution_id / operation_id 校验；旧执行停止写入后才能开始补偿。
补偿成功后允许重试或重新规划，补偿耗尽则锁定并保留操作归属。
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from uuid import uuid4

from app.modules.messaging.domain.enums import (
    OutboxEventStatus,
    RuntimeEventType,
)
from app.modules.planning.domain.enums import PlanStatus, TaskStatus
from app.modules.task_runtime.application.dto import (
    ClaimNextTaskInput,
    ClaimNextTaskResult,
    ExecutePlanResult,
    RecoverySnapshot,
    TaskRuntimeContext,
    TaskSnapshot,
)
from app.modules.task_runtime.application.errors import TaskExecutionError
from app.modules.task_runtime.application.ports import (
    CapabilityRegistry,
    CompensatorRegistry,
    ExecutorRegistry,
    TaskRuntimePorts,
)
from app.modules.task_runtime.domain.enums import (
    CompensationLockReason,
    TaskExecutionStatus,
)


def _new_id(prefix: str) -> str:
    """生成带前缀的唯一十六进制 ID。"""
    return f"{prefix}_{uuid4().hex}"


class TaskRuntimeService:
    """Claim、事务外执行、Completion 三段式业务 Runtime。"""

    def __init__(
        self,
        *,
        ports: TaskRuntimePorts,
        capabilities: CapabilityRegistry,
        executors: ExecutorRegistry,
        compensators: CompensatorRegistry,
        retry_delay_seconds: int = 30,
        compensation_retry_delay_seconds: int = 30,
        compensation_retry_max_delay_seconds: int = 300,
        max_compensation_attempts: int = 5,
    ) -> None:
        """初始化 TaskRuntimeService。

        Args:
            ports: 数据库能力集合。
            capabilities: 领域能力元数据注册表。
            executors: 执行器注册表。
            compensators: 补偿器注册表。
            retry_delay_seconds: 任务失败重试的默认退避延迟（秒，默认 30s）。
            compensation_retry_delay_seconds: 补偿重试初始延迟（秒，默认 30s）。
            compensation_retry_max_delay_seconds: 补偿重试最大延迟上限（秒，默认 300s）。
            max_compensation_attempts: 最大自动补偿重试次数（默认 5 次，耗尽后锁定）。

        Raises:
            ValueError: 当延迟参数小于 0 或最大补偿次数小于 1 时。
        """
        if compensation_retry_delay_seconds < 0:
            raise ValueError("补偿重试延迟不能小于 0")
        if compensation_retry_max_delay_seconds < 0:
            raise ValueError("补偿重试最大延迟不能小于 0")
        if max_compensation_attempts < 1:
            raise ValueError("最大自动补偿次数必须大于 0")
        self._ports = ports
        self._capabilities = capabilities
        self._executors = executors
        self._compensators = compensators
        self._retry_delay_seconds = retry_delay_seconds
        self._compensation_retry_delay_seconds = (
            compensation_retry_delay_seconds
        )
        self._compensation_retry_max_delay_seconds = (
            compensation_retry_max_delay_seconds
        )
        self._max_compensation_attempts = max_compensation_attempts

    async def execute_next(
        self,
        plan_id: str,
        *,
        event_id: str | None = None,
        compensation_execution_id: str | None = None,
        compensation_operation_id: str | None = None,
    ) -> ExecutePlanResult:
        """执行指定 Plan 的下一个就绪任务或处理挂起的补偿恢复。

        优先恢复历史执行；只有不存在待处理恢复项时才执行新任务。
        Executor 在事务外运行，完成后通过归属校验提交结果。

        Args:
            plan_id: 目标 Plan ID。
            event_id: 触发该步进的事件 ID（用于 Inbox 幂等）。
            compensation_execution_id: 定向恢复的 execution_id（若适用）。
            compensation_operation_id: 定向恢复的 operation_id（若适用）。

        Returns:
            步进执行结果。
        """
        # 先提交领取事务，避免执行器的外部调用长时间占用行锁。
        claimed = await asyncio.to_thread(
            self._claim_next,
            ClaimNextTaskInput(plan_id=plan_id),
            event_id,
            compensation_execution_id,
            compensation_operation_id,
        )
        if claimed.outcome == "compensation_locked":
            recovery = claimed.recovery
            return ExecutePlanResult(
                plan_id=plan_id,
                outcome="compensation_locked",
                task_id=recovery.task_id if recovery is not None else None,
                execution_id=(
                    recovery.execution_id if recovery is not None else None
                ),
            )
        # 历史副作用未清理前，不允许同一 Plan 继续推进。
        if claimed.recovery is not None:
            recovery = claimed.recovery
            definition = self._capabilities.require(
                recovery.capability_code
            )
            return await self._execute_compensation(
                recovery,
                definition,
                event_id=event_id,
            )
        if claimed.task is None:
            return ExecutePlanResult(plan_id=plan_id, outcome=claimed.outcome)

        task = claimed.task
        definition = self._capabilities.require(task.capability_code)
        executor = self._executors.require(definition.executor_code)
        try:
            payload = definition.input_model.model_validate(task.input_json)
            raw_result = await asyncio.wait_for(
                executor.execute(
                    payload,
                    TaskRuntimeContext(
                        workflow_id=task.workflow_id,
                        plan_id=task.plan_id,
                        task_id=task.task_id,
                        conversation_id=task.conversation_id,
                        turn_id=task.turn_id,
                        execution_id=task.execution_id,
                        operation_id=task.operation_id,
                        agent_run_id=task.agent_run_id,
                        attempt=task.attempt,
                    ),
                ),
                timeout=definition.timeout_seconds,
            )
            output = definition.output_model.model_validate(
                raw_result.output_json
            )
        except asyncio.TimeoutError:
            error = TaskExecutionError(
                "executor_timeout",
                "Task Executor 执行超时",
                retryable=True,
            )
            return await self._handle_failure(
                task,
                definition,
                error,
                event_id=event_id,
            )
        except TaskExecutionError as error:
            return await self._handle_failure(
                task,
                definition,
                error,
                event_id=event_id,
            )
        except Exception as exc:
            error = TaskExecutionError(
                "executor_system_error",
                f"Task Executor 系统异常: {type(exc).__name__}",
                retryable=False,
            )
            return await self._handle_failure(
                task,
                definition,
                error,
                event_id=event_id,
            )

        # 第三阶段：短事务标记任务完成，并触发下一任务唤醒或聚合
        await asyncio.to_thread(
            self._complete,
            task,
            output.model_dump(mode="json"),
            raw_result.resource_refs,
        )
        return ExecutePlanResult(
            plan_id=plan_id,
            outcome="task_succeeded",
            task_id=task.task_id,
            execution_id=task.execution_id,
        )

    def claim_next_task(
        self,
        command: ClaimNextTaskInput,
    ) -> ClaimNextTaskResult:
        """同步短事务接口：Claim 下一个可执行任务。"""
        return self._claim_next(command)

    def complete_task(
        self,
        snapshot: TaskSnapshot,
        output_json: dict,
        resource_refs: list[str],
    ) -> None:
        """同步短事务接口：标记 Task 成功完成。"""
        self._complete(snapshot, output_json, resource_refs)

    async def fail_task(
        self,
        snapshot: TaskSnapshot,
        error: TaskExecutionError,
    ) -> ExecutePlanResult:
        """异步接口：处理 Task 执行失败。"""
        definition = self._capabilities.require(snapshot.capability_code)
        return await self._handle_failure(snapshot, definition, error)

    def _claim_next(
        self,
        command: ClaimNextTaskInput,
        event_id: str | None = None,
        compensation_execution_id: str | None = None,
        compensation_operation_id: str | None = None,
    ) -> ClaimNextTaskResult:
        """短事务内抢占下一个依赖满足的 Task 或发现陈旧执行补偿项。"""
        with self._ports.uow_factory() as uow:
            # 1. 幂等校验
            if event_id is not None and uow.inbox.exists(
                "task_runtime", event_id
            ):
                return ClaimNextTaskResult(outcome="terminal")
            # 2. 锁定 Plan 实体
            plan = uow.plans.get_by_id_for_update(command.plan_id)
            if plan is None:
                raise ValueError("Plan 不存在")
            # 终态 Plan 直接跳过
            if plan.status in {
                PlanStatus.COMPLETED.value,
                PlanStatus.FAILED.value,
                PlanStatus.CANCELLED.value,
                PlanStatus.SUPERSEDED.value,
                PlanStatus.REPLAN_PENDING.value,
            }:
                self._record_inbox(uow, event_id)
                if event_id is not None:
                    uow.commit()
                return ClaimNextTaskResult(outcome="terminal")
            # 3. 检查是否有正在执行的任务（并发度为 1）
            if plan.current_task_id is not None:
                recovery_claim = self._recover_stale_execution(uow, plan)
                if recovery_claim is not None:
                    recovery = recovery_claim.recovery
                    if recovery is None:
                        raise RuntimeError("补偿恢复结果缺少 TaskExecution 快照")
                    if (
                        compensation_execution_id is not None
                        and recovery.execution_id
                        != compensation_execution_id
                    ) or (
                        compensation_operation_id is not None
                        and recovery.operation_id
                        != compensation_operation_id
                    ):
                        self._record_inbox(uow, event_id)
                        if event_id is not None:
                            uow.commit()
                        return ClaimNextTaskResult(outcome="terminal")
                    if recovery_claim.outcome == "compensation_locked":
                        self._record_inbox(uow, event_id)
                        if event_id is not None:
                            uow.commit()
                        return recovery_claim
                    uow.commit()
                    return recovery_claim
                self._record_inbox(uow, event_id)
                if event_id is not None:
                    uow.commit()
                return ClaimNextTaskResult(outcome="already_running")
            if (
                compensation_execution_id is not None
                or compensation_operation_id is not None
            ):
                self._record_inbox(uow, event_id)
                if event_id is not None:
                    uow.commit()
                return ClaimNextTaskResult(outcome="terminal")
            if plan.status not in {
                PlanStatus.READY.value,
                PlanStatus.RUNNING.value,
            }:
                self._record_inbox(uow, event_id)
                if event_id is not None:
                    uow.commit()
                return ClaimNextTaskResult(outcome="terminal")

            # 4. 将到期的 RETRY_WAIT 任务恢复为 PENDING
            retrying = uow.tasks.list_by_plan_id_and_status_for_update(
                plan.plan_id,
                TaskStatus.RETRY_WAIT.value,
            )
            if retrying:
                uow.tasks.set_status(retrying, TaskStatus.PENDING.value)
            # 5. 按照 DAG 拓扑排序选取下一个前置依赖全为 SUCCEEDED 的任务
            task = uow.tasks.get_next_runnable_for_update(
                plan.plan_id,
                TaskStatus.PENDING.value,
                TaskStatus.SUCCEEDED.value,
            )
            if task is None:
                if retrying or event_id is not None:
                    self._record_inbox(uow, event_id)
                    uow.commit()
                return ClaimNextTaskResult(outcome="no_task")

            # 6. 生成 execution_id、operation_id 与 attempt，推进任务为 RUNNING
            definition = self._capabilities.require(task.capability_code)
            attempt = task.attempt_count + 1
            task.attempt_count = attempt
            task.max_attempts = definition.max_attempts
            task.status = TaskStatus.RUNNING.value
            task.started_at = datetime.now()
            plan.status = PlanStatus.RUNNING.value
            if plan.started_at is None:
                plan.started_at = datetime.now()
            operation_id = _new_id("operation")
            execution_id = _new_id("execution")
            agent_run_id = _new_id("agent_run")
            conversation_id = self._turn_conversation_id(uow, plan.turn_id)
            uow.plans.set_current_task(plan, task.task_id)
            uow.task_executions.add(
                self._ports.task_execution_factory(
                    execution_id=execution_id,
                    task_id=task.task_id,
                    plan_id=plan.plan_id,
                    workflow_id=plan.workflow_id,
                    attempt=attempt,
                    status=TaskExecutionStatus.RUNNING.value,
                    executor_code=definition.executor_code,
                    input_snapshot_json=dict(task.input_json),
                    output_json=None,
                    resource_refs_json=[],
                    error_code=None,
                    error_message=None,
                    retryable=None,
                    blocked=False,
                    agent_run_id=agent_run_id,
                    operation_id=operation_id,
                    started_at=datetime.now(),
                    completed_at=None,
                )
            )
            # 写入超时唤醒事件（以防 Worker 异常挂起）
            uow.outbox.add(
                self._event(
                    plan,
                    RuntimeEventType.PLAN_WAKEUP,
                    available_at=(
                        datetime.now()
                        + timedelta(seconds=definition.timeout_seconds)
                    ),
                )
            )
            self._record_inbox(uow, event_id)
            uow.commit()
            return ClaimNextTaskResult(
                outcome="claimed",
                task=TaskSnapshot(
                    task_id=task.task_id,
                    plan_id=plan.plan_id,
                    workflow_id=plan.workflow_id,
                    conversation_id=conversation_id,
                    turn_id=plan.turn_id,
                    capability_code=task.capability_code,
                    input_json=dict(task.input_json),
                    sequence=task.sequence,
                    attempt=attempt,
                    max_attempts=task.max_attempts,
                    execution_id=execution_id,
                    operation_id=operation_id,
                    agent_run_id=agent_run_id,
                    executor_code=definition.executor_code,
                ),
            )

    def _record_inbox(self, uow, event_id: str | None) -> None:
        """记录 Inbox 事件以保证幂等去重。"""
        if event_id is None:
            return
        uow.inbox.add(
            self._ports.inbox_event_factory(
                inbox_id=_new_id("inbox"),
                consumer_name="task_runtime",
                event_id=event_id,
                processed_at=datetime.now(),
            )
        )

    def _recover_stale_execution(
        self,
        uow,
        plan,
    ) -> ClaimNextTaskResult | None:
        """检查并恢复超时挂起的陈旧执行记录（Stale Execution Recovery）。"""
        task = uow.tasks.get_by_id_for_update(plan.current_task_id)
        if task is None:
            raise RuntimeError("Plan current_task_id 指向不存在的 Task")
        definition = self._capabilities.require(task.capability_code)
        execution = uow.task_executions.get_latest_by_task_for_update(
            task.task_id
        )
        if execution is None:
            raise RuntimeError("Running Task 缺少 TaskExecution")
        if execution.status == TaskExecutionStatus.COMPENSATION_LOCKED.value:
            return ClaimNextTaskResult(
                outcome="compensation_locked",
                recovery=self._recovery_snapshot(uow, plan, task, execution),
            )
        if execution.status == TaskExecutionStatus.COMPENSATION_REQUIRED.value:
            return ClaimNextTaskResult(
                outcome="compensation_required",
                recovery=self._recovery_snapshot(uow, plan, task, execution),
            )
        if execution.status != TaskExecutionStatus.RUNNING.value:
            raise RuntimeError("Running Task 的 TaskExecution 状态不一致")
        started_at = task.started_at
        # 未达到租约超时时间则依然视为在运行中
        if (
            started_at is None
            or datetime.now()
            < started_at + timedelta(seconds=definition.timeout_seconds)
        ):
            return None
        # 超时判定：标记为 COMPENSATION_REQUIRED 并进入补偿恢复
        execution.status = TaskExecutionStatus.COMPENSATION_REQUIRED.value
        execution.error_code = "execution_lease_expired"
        execution.error_message = "Task Execution 超时未完成"
        execution.retryable = True
        execution.blocked = False
        execution.completed_at = None
        return ClaimNextTaskResult(
            outcome="compensation_required",
            recovery=self._recovery_snapshot(uow, plan, task, execution),
        )

    def _recovery_snapshot(self, uow, plan, task, execution) -> RecoverySnapshot:
        """构建 RecoverySnapshot 快照对象。"""
        if execution.agent_run_id is None:
            raise RuntimeError("TaskExecution 缺少 agent_run_id")
        return RecoverySnapshot(
            task_id=task.task_id,
            plan_id=plan.plan_id,
            workflow_id=plan.workflow_id,
            conversation_id=self._turn_conversation_id(uow, plan.turn_id),
            turn_id=plan.turn_id,
            capability_code=task.capability_code,
            input_json=dict(execution.input_snapshot_json),
            attempt=execution.attempt,
            max_attempts=task.max_attempts,
            execution_id=execution.execution_id,
            operation_id=execution.operation_id,
            agent_run_id=execution.agent_run_id,
        )

    async def _handle_failure(
        self,
        snapshot: TaskSnapshot,
        definition,
        error: TaskExecutionError,
        *,
        event_id: str | None = None,
    ) -> ExecutePlanResult:
        """处理任务执行失败的分支逻辑。"""
        # 无副作用能力直接标记失败并流转状态
        if not definition.side_effect:
            await asyncio.to_thread(self._fail, snapshot, error)
            return self._failure_result(snapshot, error)

        # 有副作用能力先落盘 COMPENSATION_REQUIRED 状态，再进入事务外补偿
        await asyncio.to_thread(
            self._require_compensation,
            snapshot,
            error,
        )
        return await self._execute_compensation(
            snapshot,
            definition,
            event_id=None,
        )

    async def _execute_compensation(
        self,
        snapshot: TaskSnapshot | RecoverySnapshot,
        definition,
        *,
        event_id: str | None,
    ) -> ExecutePlanResult:
        """执行确定性副作用补偿并处理补偿结果。"""
        attempt = await asyncio.to_thread(
            self._begin_compensation_attempt,
            snapshot,
        )
        try:
            # 事务外执行补偿器（如清理 staging 目录或回滚 Qdrant 向量）
            await self._run_compensator(snapshot, definition)
        except Exception as compensation_error:
            # 补偿失败：指数退避重试或超限锁定
            return await asyncio.to_thread(
                self._handle_compensation_failure,
                snapshot,
                event_id,
                compensation_error,
                attempt,
            )
        # 补偿成功：释放 ownership 并进入 retry 或 replan
        return await asyncio.to_thread(
            self._complete_compensation,
            snapshot,
            event_id,
        )

    async def _run_compensator(self, snapshot, definition) -> None:
        """调用注册表中的 OperationCompensator 实例。"""
        if definition.compensator_code is None:
            if definition.side_effect:
                raise RuntimeError("有副作用的 Capability 缺少 Compensator")
            return
        payload = definition.input_model.model_validate(snapshot.input_json)
        compensator = self._compensators.require(
            definition.compensator_code
        )
        await compensator.compensate(
            operation_id=snapshot.operation_id,
            payload=payload,
            context=self._runtime_context(snapshot),
        )

    @staticmethod
    def _runtime_context(snapshot) -> TaskRuntimeContext:
        """构建 TaskRuntimeContext 上下文对象。"""
        return TaskRuntimeContext(
            workflow_id=snapshot.workflow_id,
            plan_id=snapshot.plan_id,
            task_id=snapshot.task_id,
            conversation_id=snapshot.conversation_id,
            turn_id=snapshot.turn_id,
            execution_id=snapshot.execution_id,
            operation_id=snapshot.operation_id,
            agent_run_id=snapshot.agent_run_id,
            attempt=snapshot.attempt,
        )

    def _begin_compensation_attempt(
        self,
        snapshot: TaskSnapshot | RecoverySnapshot,
    ) -> int:
        """在调用 Compensator 前持久化一次真实补偿尝试。"""
        with self._ports.uow_factory() as uow:
            plan = uow.plans.get_by_id_for_update(snapshot.plan_id)
            task = uow.tasks.get_by_id_for_update(snapshot.task_id)
            execution = uow.task_executions.get_by_id_for_update(
                snapshot.execution_id
            )
            self._assert_compensation_token(
                plan,
                task,
                execution,
                snapshot,
            )

            execution.compensation_attempt_count += 1
            execution.compensation_last_attempt_at = datetime.now()
            attempt = execution.compensation_attempt_count
            uow.commit()
            return attempt

    def _handle_compensation_failure(
        self,
        snapshot: TaskSnapshot | RecoverySnapshot,
        event_id: str | None,
        error: Exception,
        attempt: int,
    ) -> ExecutePlanResult:
        """记录已执行补偿的失败，并按指数退避可靠重试或冻结补偿生命周期。"""
        with self._ports.uow_factory() as uow:
            plan = uow.plans.get_by_id_for_update(snapshot.plan_id)
            task = uow.tasks.get_by_id_for_update(snapshot.task_id)
            execution = uow.task_executions.get_by_id_for_update(
                snapshot.execution_id
            )
            self._assert_compensation_token(
                plan,
                task,
                execution,
                snapshot,
            )
            if execution.compensation_attempt_count != attempt:
                raise RuntimeError("Task Execution 补偿 attempt 已失效")

            now = datetime.now()
            execution.compensation_last_error = str(error)
            # 若补偿达到最大次数上限，进入 COMPENSATION_LOCKED
            if attempt >= self._max_compensation_attempts:
                return self._lock_compensation(
                    uow,
                    plan,
                    task,
                    execution,
                    event_id=event_id,
                    locked_at=now,
                )
            # 指数退避计算下次补偿唤醒延迟
            delay_seconds = min(
                self._compensation_retry_delay_seconds
                * (2 ** (attempt - 1)),
                self._compensation_retry_max_delay_seconds,
            )
            uow.outbox.add(
                self._event(
                    plan,
                    RuntimeEventType.PLAN_WAKEUP,
                    payload={
                        "workflow_id": plan.workflow_id,
                        "plan_id": plan.plan_id,
                        "execution_id": snapshot.execution_id,
                        "operation_id": snapshot.operation_id,
                    },
                    available_at=(
                        now + timedelta(seconds=delay_seconds)
                    ),
                )
            )
            if event_id is not None and not uow.inbox.exists(
                "task_runtime",
                event_id,
            ):
                self._record_inbox(uow, event_id)
            uow.commit()
            return ExecutePlanResult(
                plan_id=plan.plan_id,
                outcome="compensation_retry_scheduled",
                task_id=task.task_id,
                execution_id=execution.execution_id,
            )

    def _lock_compensation(
        self,
        uow,
        plan,
        task,
        execution,
        *,
        event_id: str | None,
        locked_at: datetime,
    ) -> ExecutePlanResult:
        """冻结已耗尽自动尝试的补偿生命周期并保留 Operation ownership。"""
        execution.status = TaskExecutionStatus.COMPENSATION_LOCKED.value
        execution.compensation_locked_at = locked_at
        execution.compensation_lock_reason = (
            CompensationLockReason.RETRY_EXHAUSTED.value
        )
        execution.completed_at = None
        if event_id is not None and not uow.inbox.exists(
            "task_runtime",
            event_id,
        ):
            self._record_inbox(uow, event_id)
        uow.commit()
        return ExecutePlanResult(
            plan_id=plan.plan_id,
            outcome="compensation_locked",
            task_id=task.task_id,
            execution_id=execution.execution_id,
        )

    def _require_compensation(
        self,
        snapshot: TaskSnapshot,
        error: TaskExecutionError,
    ) -> None:
        """先持久化失败事实和补偿意图，再允许事务外补偿。"""
        with self._ports.uow_factory() as uow:
            _, _, execution = self._lock_execution(uow, snapshot)
            execution.status = TaskExecutionStatus.COMPENSATION_REQUIRED.value
            execution.error_code = error.error_code
            execution.error_message = str(error)
            execution.retryable = error.retryable
            execution.blocked = error.blocked
            execution.completed_at = None
            uow.commit()

    def _complete_compensation(
        self,
        snapshot: TaskSnapshot | RecoverySnapshot,
        event_id: str | None,
    ) -> ExecutePlanResult:
        """补偿成功后原子释放 Task ownership 并决定 retry 或 replan。"""
        with self._ports.uow_factory() as uow:
            plan = uow.plans.get_by_id_for_update(snapshot.plan_id)
            task = uow.tasks.get_by_id_for_update(snapshot.task_id)
            execution = uow.task_executions.get_by_id_for_update(
                snapshot.execution_id
            )
            self._assert_compensation_token(
                plan,
                task,
                execution,
                snapshot,
            )

            now = datetime.now()
            execution.status = TaskExecutionStatus.COMPENSATED.value
            execution.completed_at = now
            task.last_error_code = execution.error_code
            task.last_error_message = execution.error_message
            task.completed_at = now
            uow.plans.set_current_task(plan, None)
            if execution.retryable and task.attempt_count < task.max_attempts:
                stale_recovery = (
                    execution.error_code == "execution_lease_expired"
                )
                task.status = (
                    TaskStatus.PENDING.value
                    if stale_recovery
                    else TaskStatus.RETRY_WAIT.value
                )
                uow.outbox.add(
                    self._event(
                        plan,
                        RuntimeEventType.PLAN_WAKEUP,
                        available_at=(
                            now
                            if stale_recovery
                            else now
                            + timedelta(seconds=self._retry_delay_seconds)
                        ),
                    )
                )
                outcome = "retry_scheduled"
            else:
                task.status = (
                    TaskStatus.BLOCKED.value
                    if execution.blocked
                    else TaskStatus.FAILED.value
                )
                plan.status = PlanStatus.REPLAN_PENDING.value
                plan.failure_code = execution.error_code
                plan.failure_reason = execution.error_message
                uow.outbox.add(
                    self._event(
                        plan,
                        RuntimeEventType.REPLAN_REQUESTED,
                        payload={
                            "workflow_id": plan.workflow_id,
                            "conversation_id": snapshot.conversation_id,
                            "root_turn_id": plan.turn_id,
                            "previous_plan_id": plan.plan_id,
                            "next_revision": plan.revision + 1,
                            "trigger_type": (
                                "task_blocked"
                                if execution.blocked
                                else "task_terminal_failure"
                            ),
                            "source_task_id": task.task_id,
                            "error_code": execution.error_code,
                            "error_message": execution.error_message,
                        },
                    )
                )
                outcome = "replan_requested"
            self._record_inbox(uow, event_id)
            uow.commit()
            return ExecutePlanResult(
                plan_id=plan.plan_id,
                outcome=outcome,
                task_id=task.task_id,
                execution_id=execution.execution_id,
            )

    @staticmethod
    def _assert_compensation_token(
        plan,
        task,
        execution,
        snapshot: TaskSnapshot | RecoverySnapshot,
    ) -> None:
        """统一校验 Runtime 当前补偿 Operation 的持久化 ownership。"""
        if plan is None or task is None or execution is None:
            raise RuntimeError("Task Runtime 补偿状态记录不存在")
        if (
            plan.current_task_id != task.task_id
            or task.status != TaskStatus.RUNNING.value
            or execution.status
            != TaskExecutionStatus.COMPENSATION_REQUIRED.value
            or execution.attempt != snapshot.attempt
            or execution.operation_id != snapshot.operation_id
        ):
            raise RuntimeError("Task Execution 补偿 token 已失效")

    def _complete(
        self,
        snapshot: TaskSnapshot,
        output_json: dict,
        resource_refs: list[str],
    ) -> None:
        """在短事务中标记任务成功完成，并触发下一任务唤醒或聚合事件。"""
        with self._ports.uow_factory() as uow:
            plan, task, execution = self._lock_execution(uow, snapshot)
            now = datetime.now()
            task.status = TaskStatus.SUCCEEDED.value
            task.output_json = output_json
            task.last_error_code = None
            task.last_error_message = None
            task.completed_at = now
            execution.status = TaskExecutionStatus.SUCCEEDED.value
            execution.output_json = output_json
            execution.resource_refs_json = list(dict.fromkeys(resource_refs))
            execution.completed_at = now
            uow.plans.set_current_task(plan, None)

            pending_count = uow.tasks.count_by_plan_and_status(
                plan.plan_id, TaskStatus.PENDING.value
            )
            retry_count = uow.tasks.count_by_plan_and_status(
                plan.plan_id, TaskStatus.RETRY_WAIT.value
            )
            if pending_count or retry_count:
                uow.outbox.add(self._event(plan, RuntimeEventType.PLAN_WAKEUP))
            else:
                unfinished = [
                    item
                    for item in uow.tasks.list_by_plan_id(plan.plan_id)
                    if item.status != TaskStatus.SUCCEEDED.value
                ]
                if not unfinished:
                    plan.status = PlanStatus.COMPLETED.value
                    plan.completed_at = now
                    uow.outbox.add(
                        self._event(plan, RuntimeEventType.AGGREGATION_REQUESTED)
                    )
            uow.commit()

    def _fail(self, snapshot: TaskSnapshot, error: TaskExecutionError) -> None:
        """处理无副作用能力的失败落盘。"""
        with self._ports.uow_factory() as uow:
            plan, task, execution = self._lock_execution(uow, snapshot)
            now = datetime.now()
            execution.status = TaskExecutionStatus.FAILED.value
            execution.error_code = error.error_code
            execution.error_message = str(error)
            execution.retryable = error.retryable
            execution.blocked = error.blocked
            execution.completed_at = now
            task.last_error_code = error.error_code
            task.last_error_message = str(error)
            task.completed_at = now
            uow.plans.set_current_task(plan, None)

            if error.retryable and task.attempt_count < task.max_attempts:
                task.status = TaskStatus.RETRY_WAIT.value
                uow.outbox.add(
                    self._event(
                        plan,
                        RuntimeEventType.PLAN_WAKEUP,
                        available_at=now + timedelta(seconds=self._retry_delay_seconds),
                    )
                )
            else:
                task.status = (
                    TaskStatus.BLOCKED.value if error.blocked else TaskStatus.FAILED.value
                )
                plan.status = PlanStatus.REPLAN_PENDING.value
                plan.failure_code = error.error_code
                plan.failure_reason = str(error)
                uow.outbox.add(
                    self._event(
                        plan,
                        RuntimeEventType.REPLAN_REQUESTED,
                        payload={
                            "workflow_id": plan.workflow_id,
                            "conversation_id": self._turn_conversation_id(uow, plan.turn_id),
                            "root_turn_id": plan.turn_id,
                            "previous_plan_id": plan.plan_id,
                            "next_revision": plan.revision + 1,
                            "trigger_type": (
                                "task_blocked" if error.blocked else "task_terminal_failure"
                            ),
                            "source_task_id": task.task_id,
                            "error_code": error.error_code,
                            "error_message": str(error),
                        },
                    )
                )
            uow.commit()

    def _lock_execution(self, uow, snapshot: TaskSnapshot):
        """校验并锁定 Plan、Task 与 TaskExecution 三者状态及 ownership token。"""
        plan = uow.plans.get_by_id_for_update(snapshot.plan_id)
        task = uow.tasks.get_by_id_for_update(snapshot.task_id)
        execution = uow.task_executions.get_by_id_for_update(snapshot.execution_id)
        if plan is None or task is None or execution is None:
            raise RuntimeError("Task Runtime 状态记录不存在")
        if (
            plan.current_task_id != task.task_id
            or task.status != TaskStatus.RUNNING.value
            or execution.status != TaskExecutionStatus.RUNNING.value
            or execution.attempt != snapshot.attempt
            or execution.operation_id != snapshot.operation_id
            or execution.agent_run_id != snapshot.agent_run_id
        ):
            raise RuntimeError("Task Execution token 已失效")
        return plan, task, execution

    def _event(
        self,
        plan,
        event_type: RuntimeEventType,
        *,
        payload: dict | None = None,
        available_at: datetime | None = None,
    ):
        """构建 OutboxEvent 实体。"""
        return self._ports.outbox_event_factory(
            event_id=_new_id("event"),
            event_type=event_type.value,
            aggregate_type="plan",
            aggregate_id=plan.plan_id,
            payload_json=(
                payload
                or {"workflow_id": plan.workflow_id, "plan_id": plan.plan_id}
            ),
            status=OutboxEventStatus.PENDING.value,
            attempts=0,
            available_at=available_at or datetime.now(),
            published_at=None,
        )

    @staticmethod
    def _turn_conversation_id(uow, turn_id: str) -> str:
        """从 Turn 实体中获取所属 conversation_id。"""
        turn = uow.conversation_turns.get_by_id(turn_id)
        if turn is None:
            raise RuntimeError("Plan 对应 Turn 不存在")
        return turn.conversation_id

    @staticmethod
    def _failure_result(
        task: TaskSnapshot,
        error: TaskExecutionError,
    ) -> ExecutePlanResult:
        """生成无副作用失败时的 ExecutePlanResult。"""
        retry = error.retryable and task.attempt < task.max_attempts
        return ExecutePlanResult(
            plan_id=task.plan_id,
            outcome="retry_scheduled" if retry else "replan_requested",
            task_id=task.task_id,
            execution_id=task.execution_id,
        )
