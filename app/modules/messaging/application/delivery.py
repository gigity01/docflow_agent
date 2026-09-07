"""复用 Inbox 保存消费重试/死信，复用 Outbox 安排人工重放。"""

from datetime import timedelta
from hashlib import sha256
from uuid import uuid4

from pydantic import BaseModel

from app.modules.messaging.application.dto import RuntimeEvent
from app.shared.time import utc_now

DELIVERY_CONSUMER = "runtime.delivery"
MAX_DELIVERY_ATTEMPTS = 3


class RecoveryError(ValueError):
    def __init__(self, detail: str, status_code: int = 409):
        super().__init__(detail)
        self.status_code = status_code


class FailureView(BaseModel):
    failure_id: str
    stage: str
    event_id: str
    plan_id: str | None
    status: str
    attempts: int
    reason: str
    retryable: bool
    next_action: str


def delivery_view(row) -> FailureView:
    malformed = row.error_code == "invalid_message"
    waiting = row.status == "retry_pending"
    return FailureView(
        failure_id=row.inbox_id, stage="consume", event_id=row.event_id,
        plan_id=row.plan_id, status=row.status, attempts=row.attempts,
        reason=row.error_message or "等待正在执行的任务释放租约",
        retryable=not malformed and not waiting,
        next_action=("等待自动重试" if waiting else "修复输入后提交新请求" if malformed
                     else "检查服务与日志，修复后调用失败记录的 retry 接口"),
    )


class MessageDeliveryService:
    def __init__(self, *, uow_factory, inbox_factory, outbox_factory, retry_delay_seconds=30):
        self._uow_factory = uow_factory
        self._inbox_factory = inbox_factory
        self._outbox_factory = outbox_factory
        self._retry_delay = retry_delay_seconds

    def before(self, event_id: str) -> str:
        with self._uow_factory() as uow:
            row = uow.inbox.get_for_update(DELIVERY_CONSUMER, event_id)
            if row is None:
                return "execute"
            if row.status in {"processed", "dead_letter"}:
                return "ack"
            if row.available_at and utc_now() < row.available_at:
                return "wait"
            return "execute"

    def _record(self, uow, event: RuntimeEvent | None, message_id: str, fields: dict):
        event_id = event.event_id if event else "invalid_" + sha256(message_id.encode()).hexdigest()
        row = uow.inbox.get_for_update(DELIVERY_CONSUMER, event_id)
        if row is None:
            row = uow.inbox.add(self._inbox_factory(
                inbox_id="delivery_" + sha256(event_id.encode()).hexdigest(),
                consumer_name=DELIVERY_CONSUMER, event_id=event_id,
                processed_at=None, status="retry_pending", attempts=0,
                event_type=event.event_type if event else None,
                payload_json=event.payload if event else {str(k): str(v) for k, v in fields.items()},
                plan_id=(event.payload.get("plan_id") or event.payload.get("previous_plan_id")) if event else None,
                recovery_count=0,
            ))
        return row

    def defer(self, event: RuntimeEvent, available_at) -> None:
        with self._uow_factory() as uow:
            row = self._record(uow, event, "", {})
            if row.status not in {"processed", "dead_letter"}:
                row.available_at = available_at
                row.error_code = "execution_lease_active"
                row.error_message = "已有执行租约，系统会在租约到期后继续检查"
                uow.commit()

    def failed(self, event: RuntimeEvent | None, message_id: str, fields: dict, *, error_name: str) -> bool:
        """返回是否可以 ACK；持久化失败会抛出，原消息继续留在 PEL。"""
        with self._uow_factory() as uow:
            row = self._record(uow, event, message_id, fields)
            if row.status in {"processed", "dead_letter"}:
                return True
            row.attempts += 1
            row.error_code = "invalid_message" if event is None else "event_handler_failed"
            row.error_message = ("消息格式或事件类型不合法，不能自动重试" if event is None
                                 else f"任务事件处理失败（{error_name}），请检查服务配置与日志")
            dead = event is None or row.attempts >= MAX_DELIVERY_ATTEMPTS
            row.status = "dead_letter" if dead else "retry_pending"
            row.available_at = None if dead else utc_now() + timedelta(seconds=self._retry_delay * row.attempts)
            uow.commit()
            return dead

    def completed(self, event: RuntimeEvent) -> None:
        with self._uow_factory() as uow:
            row = self._record(uow, event, "", {})
            if row.status != "processed":
                row.status = "processed"
                row.attempts += 1
                row.processed_at = utc_now()
                row.available_at = None
                uow.commit()

    def list_failures(self, *, plan_id=None, limit=50, offset=0) -> list[FailureView]:
        with self._uow_factory() as uow:
            result = [delivery_view(row) for row in uow.inbox.list_delivery_failures(
                plan_id=plan_id, limit=limit, offset=offset)]
            result.extend(FailureView(
                failure_id="outbox:" + row.event_id, stage="publish", event_id=row.event_id,
                plan_id=row.aggregate_id, status=row.status, attempts=row.attempts,
                reason="消息发布至 Redis 失败，自动尝试次数已耗尽", retryable=True,
                next_action="恢复 Redis 后调用失败记录的 retry 接口",
            ) for row in uow.outbox.list_failed(plan_id=plan_id, limit=limit, offset=offset))
            return result

    def retry(self, failure_id: str) -> dict:
        with self._uow_factory() as uow:
            if failure_id.startswith("outbox:"):
                row = uow.outbox.get_by_id_for_update(failure_id.removeprefix("outbox:"))
                if row is None:
                    raise RecoveryError("失败记录不存在", 404)
                if row.status != "dead_letter":
                    return {"status": row.status, "event_id": row.event_id}
                row.status, row.attempts, row.available_at = "pending", 0, utc_now()
                uow.commit()
                return {"status": "queued", "event_id": row.event_id}
            row = uow.inbox.get_failure_for_update(failure_id)
            if row is None:
                raise RecoveryError("失败记录不存在", 404)
            if row.status != "dead_letter":
                return {"status": row.status, "event_id": row.event_id}
            if row.error_code == "invalid_message":
                raise RecoveryError("原消息格式不合法；请修复输入并提交新请求，不能原样重放")
            if row.plan_id:
                plan = uow.plans.get_by_id_for_update(row.plan_id)
                if plan is None:
                    raise RecoveryError("关联计划不存在，请重新提交请求")
                if plan.status in {"failed", "unsupported", "cancelled"}:
                    raise RecoveryError("计划已终止，请检查原因后使用计划 recover 接口")
                if plan.current_task_id:
                    execution = uow.task_executions.get_latest_by_task_for_update(plan.current_task_id)
                    if execution and execution.status == "compensation_locked":
                        raise RecoveryError("补偿已锁定；请先调用计划 recover 接口恢复补偿")
            event_id = "event_" + uuid4().hex
            uow.outbox.add(self._outbox_factory(
                event_id=event_id, origin_event_id=row.event_id,
                event_type=row.event_type, aggregate_type="plan", aggregate_id=row.plan_id or row.event_id,
                payload_json=row.payload_json, status="pending", attempts=0,
                available_at=utc_now(), published_at=None,
            ))
            row.status, row.attempts, row.available_at = "retry_pending", 0, utc_now()
            row.recovery_count += 1
            uow.commit()
            return {"status": "queued", "event_id": row.event_id}
