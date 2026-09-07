"""把回答写回源 Turn，并可靠请求同一 Turn 的新 Plan revision。"""

from datetime import datetime
from uuid import uuid4

from app.modules.clarification.application.errors import (
    ClarificationApplicationError,
)
from app.modules.clarification.domain.enums import ClarificationStatus
from app.modules.context.domain.enums import ContextTurnStatus
from app.modules.messaging.domain.enums import (
    OutboxEventStatus,
    RuntimeEventType,
)
from app.modules.planning.domain.enums import PlanStatus


class AnswerClarificationUseCase:
    """处理用户对澄清提问的回复用例。

    主流程（在单一短数据库事务中原子执行）：
    1. 校验用户回答字符串非空（去除空白符）。
    2. 通过行锁锁定源 ClarificationRequest、源 ConversationTurn 以及源 Plan。
    3. 核验会话归属一致性与当前生命周期状态（澄清请求必须为 OPEN，Turn 和 Plan 必须处于 NEEDS_CLARIFICATION）。
    4. 将澄清请求推进至 ANSWERED 状态，记录回答归属。
    5. 将补充信息回写至源 Turn 的 `clarification_input` 字段，并将 Turn 状态推进至 PROCESSING。
    6. 生成并向 Outbox 添加 `planning.replan_requested` 领域事件，触发同一 Turn、同一 Workflow 的下一 Revision 异步重规划。
    """

    def __init__(
        self,
        *,
        uow_factory,
        outbox_event_factory,
    ) -> None:
        """初始化 AnswerClarificationUseCase。

        Args:
            uow_factory: UnitOfWork 工厂，用于提供数据库事务上下文与仓储访问。
            outbox_event_factory: OutboxEvent 领域模型工厂函数。
        """
        self._uow_factory = uow_factory
        self._outbox_event_factory = outbox_event_factory

    def execute(
        self,
        *,
        conversation_id: str,
        source_turn_id: str,
        answer: str,
    ) -> str:
        """执行处理澄清回答，更新状态并发布 Replan Outbox 事件。

        Args:
            conversation_id: 会话唯一标识。
            source_turn_id: 发起澄清提问的源 Turn 标识。
            answer: 用户对澄清问题的回答内容。

        Returns:
            关联的 Plan ID。

        Raises:
            ClarificationApplicationError:
                - 400: 回答内容为空白字符串。
                - 404: 澄清请求不存在或会话 ID 不匹配。
                - 409: 关联状态不完整、澄清请求非 OPEN 状态、Turn/Plan 状态冲突或已存在澄清输入。
        """
        # 1. 基础输入校验：禁止纯空白回答
        normalized_answer = answer.strip()
        if not normalized_answer:
            raise ClarificationApplicationError(
                400,
                "Clarification 回答不能为空",
            )

        with self._uow_factory() as uow:
            # 2. 以行级排他锁锁定源澄清记录
            request = uow.clarifications.get_by_source_turn_id_for_update(
                source_turn_id
            )
            if request is None:
                raise ClarificationApplicationError(
                    404,
                    "Clarification 不存在",
                )

            # 3. 锁定关联的源 Turn 和源 Plan
            source_turn = uow.conversation_turns.get_by_id_for_update(
                source_turn_id
            )
            plan = uow.plans.get_by_id_for_update(request.source_plan_id)
            if source_turn is None or plan is None:
                raise ClarificationApplicationError(
                    409,
                    "Clarification 关联状态不完整",
                )

            # 4. 会话所属权校验（防止跨会话串改）
            if (
                source_turn.conversation_id != conversation_id
                or request.conversation_id != conversation_id
            ):
                raise ClarificationApplicationError(
                    404,
                    "Clarification 不存在",
                )

            # 5. 生命周期状态一致性校验（必须处于等待澄清中）
            if (
                plan.turn_id != source_turn_id
                or request.status != ClarificationStatus.OPEN.value
                or source_turn.status
                != ContextTurnStatus.NEEDS_CLARIFICATION.value
                or plan.status != PlanStatus.NEEDS_CLARIFICATION.value
            ):
                raise ClarificationApplicationError(
                    409,
                    "Clarification 当前状态不允许回答",
                )

            # 6. 防止重复回答冲突
            if source_turn.clarification_input is not None:
                raise ClarificationApplicationError(
                    409,
                    "Clarification 已澄清",
                )

            # 7. 推进 Clarification 状态为 ANSWERED
            request.status = ClarificationStatus.ANSWERED.value
            request.answer_turn_id = source_turn_id

            # 8. 将回答写回源 Turn 并推进 Turn 为 PROCESSING
            source_turn.clarification_input = normalized_answer
            uow.conversation_turns.set_status(
                source_turn,
                ContextTurnStatus.PROCESSING.value,
            )

            # 9. 写入 Outbox 事件，通知 Worker 进行 Replan（带有新的 revision）
            uow.outbox.add(
                self._outbox_event_factory(
                    event_id=f"event_{uuid4().hex}",
                    event_type=RuntimeEventType.REPLAN_REQUESTED.value,
                    aggregate_type="plan",
                    aggregate_id=plan.plan_id,
                    payload_json={
                        "workflow_id": plan.workflow_id,
                        "conversation_id": conversation_id,
                        "root_turn_id": source_turn_id,
                        "previous_plan_id": plan.plan_id,
                        "next_revision": plan.revision + 1,
                        "trigger_type": "clarification_answered",
                        "source_task_id": None,
                        "error_code": None,
                        "error_message": None,
                    },
                    status=OutboxEventStatus.PENDING.value,
                    attempts=0,
                    available_at=datetime.now(),
                    published_at=None,
                )
            )
            uow.commit()
            return plan.plan_id
