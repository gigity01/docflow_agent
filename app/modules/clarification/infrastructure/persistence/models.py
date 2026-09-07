"""ClarificationRequest ORM 模型定义。"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, JSON, String, Text
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.infrastructure.database.base import Base
from app.modules.clarification.domain.enums import ClarificationStatus


class ClarificationRequest(Base):
    """ClarificationRequest 持久化实体。

    记录一次跨 Turn 的用户澄清请求。当 Planner 在规划阶段发现信息缺口、歧义或多重解释时创建。
    同一 Turn 可以多轮澄清，每轮单独保留问题和答案。

    Attributes:
        clarification_id: 澄清请求主键 ID。
        conversation_id: 所属会话 ID。
        source_turn_id: 发起澄清提问的源 Turn 标识，与 round 联合唯一。
        source_plan_id: 发起澄清的源 Plan 标识（唯一约束）。
        kind: 澄清类型（如 ambiguous_target, missing_parameter, conflicting_intent）。
        reason: 发起澄清的技术原因说明。
        question: 向用户展示的具体澄清问题。
        required_information_json: 所需补充信息的结构化字段列表。
        known_resource_refs_json: 规划器已知并已锁定的候选资源引用列表。
        status: 澄清生命周期状态（open / answered / resolved / expired）。
        answer_turn_id: 用户提交回答所关联的 Turn 标识（通常为 source_turn_id）。
        created_at: 创建时间。
        resolved_at: 新 Plan 全部成功聚合后的解决时间。
    """

    __tablename__ = "clarification_requests"
    __table_args__ = (
        UniqueConstraint(
            "source_plan_id",
            name="uq_clarification_requests_source_plan",
        ),
        UniqueConstraint(
            "source_turn_id", "round",
            name="uq_clarification_requests_turn_round",
        ),
    )


    clarification_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    conversation_id: Mapped[str] = mapped_column(String(100), nullable=False)
    source_turn_id: Mapped[str] = mapped_column(
        String(100), ForeignKey("conversation_turns.turn_id"), nullable=False
    )
    source_plan_id: Mapped[str] = mapped_column(
        String(100), ForeignKey("plans.plan_id"), nullable=False
    )
    kind: Mapped[str] = mapped_column(String(30), nullable=False)
    round: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    answer_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    question: Mapped[str | None] = mapped_column(Text, nullable=True)
    required_information_json: Mapped[list] = mapped_column(JSON, nullable=False)
    known_resource_refs_json: Mapped[list] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default=ClarificationStatus.OPEN.value
    )
    answer_turn_id: Mapped[str | None] = mapped_column(
        String(100), ForeignKey("conversation_turns.turn_id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


Index(
    "idx_clarification_conversation_status",
    ClarificationRequest.conversation_id,
    ClarificationRequest.status,
)
