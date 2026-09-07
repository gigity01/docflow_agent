"""人工恢复只唤醒现有状态机；补偿锁恢复时仍保留原 Operation。"""

from datetime import timedelta
from uuid import uuid4

from app.modules.messaging.application.delivery import RecoveryError
from app.modules.planning.application.recovery_policy import PLANNER_RUN_LEASE_SECONDS
from app.shared.time import utc_now


class RecoverPlanUseCase:
    def __init__(self, *, uow_factory, outbox_factory, capabilities):
        self._uow_factory = uow_factory
        self._outbox_factory = outbox_factory
        self._capabilities = capabilities

    def execute(self, plan_id: str) -> dict:
        with self._uow_factory() as uow:
            plan = uow.plans.get_by_id_for_update(plan_id)
            if plan is None:
                raise RecoveryError("Plan 不存在", 404)
            latest = uow.plans.get_latest_by_turn(plan.turn_id)
            if latest.plan_id != plan_id:
                return {"status": "superseded", "plan_id": latest.plan_id, "next_action": "查询最新计划"}
            if plan.status == "completed":
                return {"status": "completed", "plan_id": plan_id, "next_action": "读取现有执行结果"}
            if plan.status in {"unsupported", "cancelled", "needs_clarification"}:
                raise RecoveryError("计划已停止或等待澄清；请按状态查询中的下一步操作处理")
            turn = uow.conversation_turns.get_by_id(plan.turn_id)
            event_type = "runtime.plan_wakeup"
            payload = {"plan_id": plan_id, "workflow_id": plan.workflow_id}
            available_at = utc_now()
            outcome = "queued"
            if plan.status == "failed":
                if plan.failure_code == "clarification_limit_exceeded":
                    raise RecoveryError("澄清次数已耗尽；请携带完整信息重新提交请求")
                executions = uow.task_executions.list_by_plan_id(plan_id)
                if any(item.status in {"running", "compensation_required", "compensation_locked"} for item in executions):
                    raise RecoveryError("仍有未清理的执行副作用，不能重开计划")
                tasks = uow.tasks.list_by_plan_id(plan_id)
                if executions:
                    # 人工开启一轮有限重试；尝试编号继续递增，成功 Task 不变。
                    for task in tasks:
                        if task.status != "succeeded":
                            definition = self._capabilities.require(task.capability_code)
                            task.status = "pending"
                            task.max_attempts = task.attempt_count + definition.max_attempts
                    plan.status = "ready"
                else:
                    # 规划尚未发布有效任务，重新规划；新一轮依然受自动预算约束。
                    plan.status = "retry_pending"
                plan.failure_code, plan.failure_reason, plan.completed_at = None, None, None
                turn.status, turn.assistant_content = "processing", None
            if plan.current_task_id:
                task = uow.tasks.get_by_id_for_update(plan.current_task_id)
                execution = uow.task_executions.get_latest_by_task_for_update(task.task_id)
                if execution is None:
                    raise RecoveryError("当前任务缺少执行记录，不能安全恢复")
                payload.update(execution_id=execution.execution_id, operation_id=execution.operation_id)
                if execution.status == "compensation_locked":
                    payload["previous_compensation_attempts"] = execution.compensation_attempt_count
                    execution.status = "compensation_required"
                    execution.compensation_attempt_count = 0
                    execution.compensation_locked_at = None
                    execution.compensation_lock_reason = None
                    outcome = "compensation_queued"
                elif execution.status == "compensation_required":
                    outcome = "compensation_queued"
                elif execution.status == "running":
                    definition = self._capabilities.require(task.capability_code)
                    available_at = max(available_at, task.started_at + timedelta(seconds=definition.timeout_seconds))
                    outcome = "waiting_for_lease"
                else:
                    raise RecoveryError("执行记录与计划状态不一致，不能直接重跑")
            elif plan.status in {"planning", "retry_pending", "replan_pending"}:
                event_type = "planning.replan_requested"
                if plan.status == "planning":
                    available_at = max(available_at, plan.updated_at + timedelta(seconds=PLANNER_RUN_LEASE_SECONDS))
                    outcome = "waiting_for_lease"
                payload = {"workflow_id": plan.workflow_id, "conversation_id": turn.conversation_id,
                           "root_turn_id": turn.turn_id, "previous_plan_id": plan_id,
                           "next_revision": plan.revision + 1, "trigger_type": "manual_retry"}
            uow.outbox.add(self._outbox_factory(
                event_id="event_" + uuid4().hex, event_type=event_type,
                aggregate_type="plan", aggregate_id=plan_id, payload_json=payload,
                status="pending", attempts=0, published_at=None, available_at=available_at,
            ))
            uow.commit()
            return {"status": outcome, "plan_id": plan_id, "available_at": available_at.isoformat() + "Z"}
