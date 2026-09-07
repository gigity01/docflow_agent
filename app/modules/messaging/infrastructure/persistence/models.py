"""可靠消息发件箱（Outbox）与收件箱（Inbox）ORM 持久化模型。

定义数据库表结构，包含：
- outbox_events: 存储待发布、已发布及死信状态的领域/运行时事件。
- inbox_events: 存储消费者已成功消费的事件记录，保障消费端幂等性。
"""

from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, JSON, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.infrastructure.database.base import Base
from app.modules.messaging.domain.enums import OutboxEventStatus


class OutboxEvent(Base):
    """发件箱事件（Transactional Outbox）持久化实体。

    在业务事务中同步写入，由独立的 OutboxPublisher 异步读取并投递至传输层。

    Attributes:
        event_id: 事件全局唯一标识（主键）。
        event_type: 事件类型（如 runtime.plan_wakeup, planning.replan_requested 等）。
        aggregate_type: 聚合根类型（如 plan, turn 等）。
        aggregate_id: 聚合根主键标识。
        payload_json: 事件负载数据的 JSON 结构。
        status: 事件生命周期状态（pending, published, dead_letter）。
        attempts: 尝试发布次数。
        available_at: 允许被消费/发布的生效时间戳。
        published_at: 成功投递至传输层的时间戳。
        created_at: 事件持久化创建时间。
    """

    __tablename__ = "outbox_events"

    event_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    aggregate_type: Mapped[str] = mapped_column(String(100), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(100), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        default=OutboxEventStatus.PENDING.value,
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    origin_event_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )


# 组合索引：加速针对状态和可投递时间的扫描轮询
Index("idx_outbox_pending_available", OutboxEvent.status, OutboxEvent.available_at)


class InboxEvent(Base):
    """收件箱事件（Transactional Inbox）持久化实体。

    用于保障特定消费者对特定事件的幂等消费，通过 (consumer_name, event_id) 唯一约束实现去重。

    Attributes:
        inbox_id: 收件箱记录主键 ID。
        consumer_name: 消费者名称标识（如 "runtime.dispatcher"）。
        event_id: 消费的事件唯一标识。
        processed_at: 事件处理完成的时间戳。
    """

    __tablename__ = "inbox_events"
    __table_args__ = (
        UniqueConstraint(
            "consumer_name", "event_id", name="uq_inbox_consumer_event"
        ),
    )

    inbox_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    consumer_name: Mapped[str] = mapped_column(String(100), nullable=False)
    event_id: Mapped[str] = mapped_column(String(100), nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # 业务 Inbox 默认仍表示处理完成；runtime.delivery 独立记录传输恢复状态。
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="processed", server_default="processed")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    available_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    event_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    payload_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    plan_id: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    recovery_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
