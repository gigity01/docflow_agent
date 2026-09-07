"""Outbox Publisher 可靠消息发布与状态推进测试。

核心业务不变量（遵循 AGENTS.md 规范）：
1. 可靠事件事实（Transactional Outbox）：
   - 业务操作（如 Plan 发布、Task 状态更新、Replan 请求）与 OutboxEvent 在同一数据库事务内提交，Outbox 是系统内持久化可靠事件事实。
2. 扫描与投递：
   - OutboxPublisher 轮询待发布事件（`status='pending'` 且 `available_at <= NOW()`），成功投递至传输层（Redis Stream）后将状态推进为 `published` 并记录 published_at。
   - 投递失败时递增 attempts，并按指数退避计算下次 available_at 时间。
"""

from __future__ import annotations

from app.shared.time import utc_now

import unittest
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.infrastructure.database.base import Base
from app.infrastructure.database.model_registry import load_all_models
from app.infrastructure.database.uow import SQLAlchemyUnitOfWork
from app.modules.messaging.application.outbox import OutboxPublisher
from app.modules.messaging.infrastructure.persistence.models import OutboxEvent


class _Publisher:
    """测试用传输层 Publisher 替身，捕获发布事件。"""
    def __init__(self) -> None:
        self.events = []

    async def publish(self, **event) -> None:
        self.events.append(event)


class OutboxPublisherTest(unittest.IsolatedAsyncioTestCase):
    """验证 OutboxPublisher 的事件扫描、单次投递与状态原子推进。"""
    async def asyncSetUp(self) -> None:
        load_all_models()
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.session_factory = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
        )
        Base.metadata.create_all(self.engine, tables=[OutboxEvent.__table__])
        with self.session_factory() as session:
            session.add(
                OutboxEvent(
                    event_id="event-1",
                    event_type="runtime.plan_wakeup",
                    aggregate_type="plan",
                    aggregate_id="plan-1",
                    payload_json={"plan_id": "plan-1"},
                    status="pending",
                    attempts=0,
                    available_at=utc_now(),
                )
            )
            session.commit()

    async def asyncTearDown(self) -> None:
        Base.metadata.drop_all(self.engine, tables=[OutboxEvent.__table__])
        self.engine.dispose()

    async def test_publishes_pending_event_once_and_marks_it_published(self) -> None:
        publisher = _Publisher()
        use_case = OutboxPublisher(
            uow_factory=lambda: SQLAlchemyUnitOfWork(self.session_factory),
            publisher=publisher,
        )
        self.assertEqual(await use_case.publish_batch(), 1)
        self.assertEqual(await use_case.publish_batch(), 0)
        self.assertEqual(publisher.events[0]["event_id"], "event-1")
        with self.session_factory() as session:
            event = session.get(OutboxEvent, "event-1")
            self.assertEqual(event.status, "published")
            self.assertIsNotNone(event.published_at)


if __name__ == "__main__":
    unittest.main()
