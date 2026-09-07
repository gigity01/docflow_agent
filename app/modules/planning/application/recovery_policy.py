"""规划恢复的有限预算与持久化超时唤醒。"""

from datetime import timedelta
from uuid import uuid4

from app.shared.time import utc_now

PLANNER_RUN_LEASE_SECONDS = 1800
MAX_SYSTEM_RETRIES = 2
MAX_CLARIFICATION_ROUNDS = 3


def add_planner_watchdog(uow, ports, plan, conversation_id: str) -> None:
    """规划进程退出后，Outbox 仍能发起下一次有限恢复。"""
    uow.outbox.add(ports.outbox_event_factory(
        event_id="event_" + uuid4().hex,
        event_type="planning.replan_requested", aggregate_type="plan", aggregate_id=plan.plan_id,
        payload_json={"workflow_id": plan.workflow_id, "conversation_id": conversation_id,
                      "root_turn_id": plan.turn_id, "previous_plan_id": plan.plan_id,
                      "next_revision": plan.revision + 1, "trigger_type": "planner_timeout"},
        status="pending", attempts=0, published_at=None,
        available_at=utc_now() + timedelta(seconds=PLANNER_RUN_LEASE_SECONDS),
    ))
