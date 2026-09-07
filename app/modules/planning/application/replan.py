"""Replan 事件的幂等新 revision 编排。

本模块负责在任务失败、受阻或澄清回答提交后，安全、幂等地生成 Plan 的新 revision：
1. 系统自动重试最多 2 次，用户澄清单独计数。
2. 使用 Inbox 机制实现事件处理幂等，防止 Redis 重投导致的重复 Replan。
3. 将上一版本 Plan 及未完成任务标记为 SUPERSEDED。
4. 创建下一版本 Plan 记录并调用 RunPlanningUseCase 重新进入规划。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4
from app.shared.time import utc_now
from app.modules.messaging.application.dto import DeferredEvent
from app.modules.planning.application.recovery_policy import (
    MAX_SYSTEM_RETRIES, PLANNER_RUN_LEASE_SECONDS, add_planner_watchdog,
)

from app.modules.context.domain.enums import ContextTurnStatus
from app.modules.messaging.application.inbox import record_inbox_once
from app.modules.planning.application.dto import (
    RunPlanningInput,
    RunPlanningResult,
)
from app.modules.planning.application.ports import PlanningApplicationPorts
from app.modules.planning.application.run_planning import RunPlanningUseCase
from app.modules.planning.domain.enums import PlanStatus, TaskStatus




@dataclass(frozen=True)
class ReplanRequested:
    """Replan 请求事件负载模型。

    Attributes:
        event_id: 事件全局唯一 ID（用于 Inbox 幂等去重）。
        workflow_id: 所属工作流 ID。
        conversation_id: 会话 ID。
        root_turn_id: 根 Conversation Turn ID。
        previous_plan_id: 触发 Replan 的上一版本 Plan ID。
        next_revision: 待创建的新 revision 版本号。
        trigger_type: 触发 Replan 的原因类型（如 task_terminal_failure, task_blocked, clarification_answered）。
        source_task_id: 触发 Replan 的源 Task ID（若适用）。
        error_code: 关联错误分类码（若适用）。
        error_message: 关联错误详细信息（若适用）。
    """

    event_id: str
    workflow_id: str
    conversation_id: str
    root_turn_id: str
    previous_plan_id: str
    next_revision: int
    trigger_type: str
    source_task_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None


class ReplanUseCase:
    """处理 Replan（重新规划）事件的用例。

    主流程：
    1. 校验系统自动重试预算，revision 仅记录版本历史。
    2. 使用 Inbox 实现事件幂等去重，防止重复触发 Replan。
    3. 行锁锁定前一 Plan，将未完成旧任务标记为 SUPERSEDED。
    4. 原子创建下一版本 Plan 记录（初始状态为 PLANNING）。
    5. 调用 RunPlanningUseCase.execute_existing 触发新一轮 Planner 规划。
    """

    CONSUMER_NAME = "planning.replan"

    def __init__(
        self,
        *,
        ports: PlanningApplicationPorts,
        run_planning: RunPlanningUseCase,
    ) -> None:
        """初始化 ReplanUseCase。

        Args:
            ports: 数据库能力集合。
            run_planning: 规划器运行主用例。
        """
        self._ports = ports
        self._run_planning = run_planning

    async def execute(
        self,
        event: ReplanRequested,
    ) -> RunPlanningResult | None:
        """执行 Replan 事件处理，生成新 revision Plan 并重新规划。

        Args:
            event: ReplanRequested 事件负载。

        Returns:
            规划执行结果；若事件已消费或达到修订上限则返回 None。
        """
        # 第一阶段：短事务准备新版本 Plan 实体并记录 Inbox
        prepared = self._prepare_revision(event)
        if prepared is None:
            return None
        plan_id, revision, parent_plan_id = prepared
        # 第二阶段：在事务外运行规划器
        return await self._run_planning.execute_existing(
            RunPlanningInput(
                conversation_id=event.conversation_id,
                turn_id=event.root_turn_id,
                revision=revision,
                workflow_id=event.workflow_id,
                parent_plan_id=parent_plan_id,
            ),
            plan_id,
        )

    def _prepare_revision(
        self,
        event: ReplanRequested,
    ) -> tuple[str, int, str] | None:
        """在短事务中安全创建新 revision Plan，处理并发冲突与幂等去重。

        Args:
            event: ReplanRequested 事件。

        Returns:
            (新 plan_id, revision, parent_plan_id) 或 None（无需继续执行）。

        Raises:
            ValueError: 当关联 Plan/Turn 不存在或上下文归属不一致时。
        """
        with self._ports.uow_factory() as uow:
            turn = uow.conversation_turns.get_by_id_for_update(event.root_turn_id)
            previous = uow.plans.get_by_id_for_update(event.previous_plan_id)
            if previous is None or turn is None:
                raise ValueError("Replan 关联的 Plan 或 Turn 不存在")
            if (previous.workflow_id != event.workflow_id or previous.turn_id != event.root_turn_id
                    or turn.conversation_id != event.conversation_id):
                raise ValueError("Replan 关联上下文不一致")
            if event.next_revision != previous.revision + 1:
                raise ValueError("Replan revision 与源计划不匹配")
            existing = uow.plans.get_by_workflow_and_revision_for_update(
                event.workflow_id, event.next_revision,
            )
            trigger = event.trigger_type
            if existing is not None:
                if existing.status != PlanStatus.PLANNING.value:
                    return None
                # 半成品规划不原地重跑，租约到期后用下一 revision 替代草稿。
                previous, trigger = existing, "planner_timeout"
            elif previous.status not in {
                PlanStatus.PLANNING.value, PlanStatus.RETRY_PENDING.value,
                PlanStatus.REPLAN_PENDING.value, PlanStatus.NEEDS_CLARIFICATION.value,
            }:
                return None
            latest = uow.plans.get_latest_by_turn(turn.turn_id)
            if latest is None or latest.plan_id != previous.plan_id:
                return None
            if previous.status == PlanStatus.PLANNING.value:
                available_at = previous.updated_at + timedelta(seconds=PLANNER_RUN_LEASE_SECONDS)
                if utc_now() < available_at:
                    raise DeferredEvent(available_at)
            if previous.status == PlanStatus.NEEDS_CLARIFICATION.value:
                clarification = uow.clarifications.get_by_plan_id_for_update(previous.plan_id)
                if trigger != "clarification_answered" or clarification is None or clarification.status != "answered":
                    return None
            system_retries = (0 if trigger == "manual_retry" else
                              previous.system_retry_count + (trigger != "clarification_answered"))
            if system_retries > MAX_SYSTEM_RETRIES:
                reason = "自动恢复次数已耗尽，请检查失败原因后重新提交请求"
                previous.status = PlanStatus.FAILED.value
                previous.failure_code = "system_retry_limit_exceeded"
                previous.failure_reason = reason
                previous.completed_at = utc_now()
                uow.tasks.set_unfinished_status(previous.plan_id, TaskStatus.FAILED.value)
                uow.conversation_turns.set_status(turn, ContextTurnStatus.FAILED.value)
                turn.assistant_content = reason
                record_inbox_once(uow, inbox_event_factory=self._ports.inbox_event_factory,
                    consumer_name=self.CONSUMER_NAME, event_id=event.event_id)
                uow.commit()
                return None
            revision = previous.revision + 1
            previous.status = PlanStatus.SUPERSEDED.value
            previous.completed_at = utc_now()
            uow.tasks.set_unfinished_status(previous.plan_id, TaskStatus.SUPERSEDED.value)
            plan_id = f"plan_{uuid4().hex}"
            plan = self._ports.plan_factory(
                plan_id=plan_id, workflow_id=event.workflow_id, turn_id=turn.turn_id,
                parent_plan_id=previous.plan_id, current_task_id=None,
                status=PlanStatus.PLANNING.value, revision=revision,
                system_retry_count=system_retries, failure_code=None, failure_reason=None,
                created_at=utc_now(), updated_at=utc_now(),
            )
            uow.plans.create(plan)
            add_planner_watchdog(uow, self._ports, plan, turn.conversation_id)
            record_inbox_once(uow, inbox_event_factory=self._ports.inbox_event_factory,
                consumer_name=self.CONSUMER_NAME, event_id=event.event_id)
            uow.commit()
            return plan_id, revision, previous.plan_id
