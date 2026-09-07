"""将数据库 Outbox 事件发布到消息队列，并单独回写投递状态。

发布与状态回写不属于同一事务，中途失败可能造成重复投递。
消费端需要按 event_id 幂等处理；投递失败达到上限后进入死信状态。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime

from app.modules.messaging.application.ports import MessagePublisherPort
from app.modules.messaging.domain.enums import OutboxEventStatus


@dataclass(frozen=True)
class _EventSnapshot:
    """发件箱事件内存快照，用于跨异步边界传递数据。

    Attributes:
        event_id: 事件全局唯一标识符。
        event_type: 事件类型。
        payload: 事件载荷字典。
    """

    event_id: str
    event_type: str
    payload: dict


class OutboxPublisher:
    """批量发布待发送事件，记录成功状态或失败尝试次数。"""

    def __init__(
        self,
        *,
        uow_factory,
        publisher: MessagePublisherPort,
        max_attempts: int = 10,
    ) -> None:
        """初始化 Outbox 发布器。

        Args:
            uow_factory: 工作单元工厂函数，每次调用产生独立短事务的 UoW。
            publisher: 消息发布端端口实例（如 RedisStreamPublisher）。
            max_attempts: 最大投递重试次数，达到后标记为死信。默认值为 10。
        """
        self._uow_factory = uow_factory
        self._publisher = publisher
        self._max_attempts = max_attempts

    async def publish_batch(self, limit: int = 100) -> int:
        """批量加载并发布待发送事件。

        在独立线程中通过短事务加载待发布快照，逐条发布到传输层；
        发布成功后在短事务中标记为 PUBLISHED，失败则在短事务中记录重试或标记死信。

        Args:
            limit: 单批最大处理条数，默认 100。

        Returns:
            本批次成功发布的事件数量。
        """
        # 将快照带出短事务，网络投递期间不持有数据库行锁。
        snapshots = await asyncio.to_thread(self._load_batch, limit)
        published = 0

        for event in snapshots:
            try:
                await self._publisher.publish(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    payload=event.payload,
                )
            except Exception:
                await asyncio.to_thread(self._mark_failed, event.event_id)
                continue

            # 此处失败时事件可能再次投递，不能依赖发布端实现恰好一次。
            await asyncio.to_thread(self._mark_published, event.event_id)
            published += 1
        return published

    def _load_batch(self, limit: int) -> list[_EventSnapshot]:
        """在独立短事务中行锁查询待投递事件快照。

        SKIP LOCKED 跳过当前已锁定的行；事务结束后不会保留领取权。

        Args:
            limit: 最大查询条数。

        Returns:
            待发布的事件快照列表。
        """
        with self._uow_factory() as uow:
            events = uow.outbox.list_available_for_update(
                status=OutboxEventStatus.PENDING.value,
                now=datetime.now(),
                limit=limit,
            )
            return [
                _EventSnapshot(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    payload=dict(event.payload_json),
                )
                for event in events
            ]

    def _mark_published(self, event_id: str) -> None:
        """在独立短事务中标记事件已成功发布。

        Args:
            event_id: 成功发布的事件唯一标识。
        """
        with self._uow_factory() as uow:
            event = uow.outbox.get_by_id_for_update(event_id)
            if event is None or event.status != OutboxEventStatus.PENDING.value:
                return
            event.status = OutboxEventStatus.PUBLISHED.value
            event.published_at = datetime.now()
            uow.commit()

    def _mark_failed(self, event_id: str) -> None:
        """在独立短事务中记录发布失败并递增重试次数；达到上限则转入死信状态。

        Args:
            event_id: 发布失败的事件唯一标识。
        """
        with self._uow_factory() as uow:
            event = uow.outbox.get_by_id_for_update(event_id)
            if event is None or event.status != OutboxEventStatus.PENDING.value:
                return
            event.attempts += 1
            if event.attempts >= self._max_attempts:
                event.status = OutboxEventStatus.DEAD_LETTER.value
            uow.commit()
