"""可靠消息发件箱（Outbox）与收件箱（Inbox）仓储实现。

提供基于 SQLAlchemy Session 的数据库持久化与行级锁查询能力：
- OutboxRepository: 批量查询时跳过已锁定行，也支持单条事件锁查询。
- InboxRepository: 支持基于联合主键/唯一约束的存在性检查与写入。
"""

from datetime import datetime

from sqlalchemy.orm import Session

from app.modules.messaging.infrastructure.persistence.models import (
    InboxEvent,
    OutboxEvent,
)


class OutboxRepository:
    """发件箱事件数据库仓储。

    管理 OutboxEvent 的插入、锁查询与状态流转。
    """

    def __init__(self, db: Session) -> None:
        """初始化 Outbox 仓储。

        Args:
            db: SQLAlchemy 数据库会话对象。
        """
        self.db = db

    def add(self, event: OutboxEvent) -> OutboxEvent:
        """将新的发件箱事件添加到当前数据库会话并刷新。

        Args:
            event: 待添加的 OutboxEvent 实体。

        Returns:
            已添加到会话中的实体实例。
        """
        self.db.add(event)
        self.db.flush()
        return event

    def list_available_for_update(
        self,
        *,
        status: str,
        now: datetime,
        limit: int,
    ) -> list[OutboxEvent]:
        """按状态和生效时间范围以悲观行锁方式批量查询待处理事件。

        SKIP LOCKED 跳过其他事务已锁定的行。锁仅在当前事务内有效，
        不保证后续网络发布不会重复；消费端仍需幂等处理。

        Args:
            status: 事件状态过滤（如 PENDING）。
            now: 当前时间阈值，仅获取 available_at <= now 的事件。
            limit: 最大拉取数量。

        Returns:
            锁定并获取的待发布事件列表。
        """
        return (
            self.db.query(OutboxEvent)
            .filter(
                OutboxEvent.status == status,
                OutboxEvent.available_at <= now,
            )
            .order_by(OutboxEvent.created_at.asc(), OutboxEvent.event_id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
            .all()
        )

    def get_by_id_for_update(self, event_id: str) -> OutboxEvent | None:
        """根据事件 ID 加排他行锁查询单条发件箱事件。

        Args:
            event_id: 事件全局唯一标识符。

        Returns:
            锁定的事件实体；若不存在则返回 None。
        """
        return (
            self.db.query(OutboxEvent)
            .filter(OutboxEvent.event_id == event_id)
            .with_for_update()
            .first()
        )


class InboxRepository:
    """收件箱事件数据库仓储。

    管理已消费事件的存在性查询与幂等登记。
    """

    def __init__(self, db: Session) -> None:
        """初始化 Inbox 仓储。

        Args:
            db: SQLAlchemy 数据库会话对象。
        """
        self.db = db

    def exists(self, consumer_name: str, event_id: str) -> bool:
        """检查特定消费者是否已经消费过指定事件。

        Args:
            consumer_name: 消费者标识（如 "runtime.dispatcher"）。
            event_id: 事件唯一标识。

        Returns:
            True 表示已记录存在（已处理），False 表示尚未消费。
        """
        return (
            self.db.query(InboxEvent)
            .filter(
                InboxEvent.consumer_name == consumer_name,
                InboxEvent.event_id == event_id,
            )
            .first()
            is not None
        )

    def add(self, event: InboxEvent) -> InboxEvent:
        """登记一条新的收件箱已消费记录并刷新会话。

        Args:
            event: 待添加的 InboxEvent 实体。

        Returns:
            登记后的实体实例。
        """
        self.db.add(event)
        self.db.flush()
        return event
