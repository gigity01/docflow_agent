"""坏消息隔离、延迟、持久化失败和人工重放的消费闭环。"""

import json
import unittest
from datetime import timedelta
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.infrastructure.database.base import Base
from app.infrastructure.database.uow import SQLAlchemyUnitOfWork
from app.modules.messaging.application.delivery import MessageDeliveryService, RecoveryError
from app.modules.messaging.application.dto import DeferredEvent
from app.modules.messaging.application.outbox import OutboxPublisher
from app.modules.messaging.infrastructure.persistence.models import InboxEvent, OutboxEvent
from app.modules.messaging.infrastructure.redis_streams import RedisStreamPublisher, RedisStreamWorker
from app.shared.time import utc_now


class MemoryStream:
    def __init__(self):
        self.new, self.pending, self.acks = [], [], []
        self.sequence = 0

    async def xgroup_create(self, *args, **kwargs):
        return True

    async def xautoclaim(self, *args, **kwargs):
        return ["0-0", []]

    async def xadd(self, stream, fields):
        self.sequence += 1
        message = (f"{self.sequence}-0", fields)
        self.new.append(message)
        return message[0]

    async def xreadgroup(self, group, consumer, streams, count=10, **kwargs):
        mode = next(iter(streams.values()))
        if mode == ">":
            rows, self.new = self.new[:count], self.new[count:]
            self.pending.extend(rows)
        else:
            cursor = int(mode.split("-")[0])
            rows = [row for row in self.pending if int(row[0].split("-")[0]) > cursor][:count]
        return [("agent-runtime", rows)] if rows else []

    async def xack(self, stream, group, message_id):
        self.acks.append(message_id)
        self.pending = [row for row in self.pending if row[0] != message_id]


class Handler:
    def __init__(self):
        self.calls, self.success = [], []
        self.failing = set()
        self.deferred_until = None

    async def handle(self, event):
        self.calls.append(event.event_id)
        if self.deferred_until:
            raise DeferredEvent(self.deferred_until)
        if event.event_id in self.failing:
            raise RuntimeError("temporary service failure")
        self.success.append(event.event_id)


class DeliveryRecoveryTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        Base.metadata.create_all(self.engine, tables=[InboxEvent.__table__, OutboxEvent.__table__])
        self.sessions = sessionmaker(self.engine, expire_on_commit=False)
        self.delivery = MessageDeliveryService(uow_factory=lambda: SQLAlchemyUnitOfWork(self.sessions),
            inbox_factory=InboxEvent, outbox_factory=OutboxEvent, retry_delay_seconds=0)
        self.stream, self.handler = MemoryStream(), Handler()
        self.worker = RedisStreamWorker(self.stream, dispatcher=self.handler, delivery=self.delivery, consumer_name="test")

    async def asyncTearDown(self):
        self.engine.dispose()

    async def publish(self, event_id, payload=None):
        await self.stream.xadd("agent-runtime", {"event_id": event_id, "event_type": "runtime.plan_wakeup",
            "payload": json.dumps({"plan_id": "p"}) if payload is None else payload})

    async def test_poison_does_not_block_valid_message_and_is_persisted_before_ack(self):
        await self.publish("bad", "not-json")
        await self.publish("good")
        await self.worker.run_once(block_milliseconds=1)
        self.assertEqual(self.handler.success, ["good"])
        self.assertEqual(len(self.stream.acks), 2)
        failures = self.delivery.list_failures()
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].status, "dead_letter")
        self.assertFalse(failures[0].retryable)
        with self.assertRaises(RecoveryError):
            self.delivery.retry(failures[0].failure_id)

    async def test_recording_failure_failure_keeps_pending_and_allows_other_messages(self):
        await self.publish("bad", "not-json")
        await self.publish("good")
        with patch.object(self.delivery, "failed", side_effect=RuntimeError("database unavailable")):
            await self.worker.run_once(block_milliseconds=1)
        self.assertEqual(self.handler.success, ["good"])
        self.assertEqual([message[0] for message in self.stream.pending], ["1-0"])

    async def test_three_attempts_then_replay_uses_original_business_event(self):
        self.handler.failing.add("retry")
        await self.publish("retry")
        for _ in range(6):
            await self.worker.run_once(block_milliseconds=1)
        self.assertEqual(self.handler.calls.count("retry"), 3)
        failure = self.delivery.list_failures()[0]
        self.assertEqual(failure.attempts, 3)
        # 本测试只验证传输层，清除不存在的模拟计划关联。
        with self.sessions() as session:
            session.get(InboxEvent, failure.failure_id).plan_id = None
            session.commit()
        self.delivery.retry(failure.failure_id)
        self.delivery.retry(failure.failure_id)
        with self.sessions() as session:
            row = session.query(OutboxEvent).one()
            self.assertEqual(row.origin_event_id, "retry")
        self.handler.failing.clear()
        publisher = OutboxPublisher(uow_factory=lambda: SQLAlchemyUnitOfWork(self.sessions),
            publisher=RedisStreamPublisher(self.stream))
        await publisher.publish_batch()
        await self.worker.run_once(block_milliseconds=1)
        self.assertEqual(self.handler.success, ["retry"])
        self.assertEqual(self.delivery.list_failures(), [])
        # 相同 event_id 的重复投递不能重新执行成功业务。
        await self.publish("retry")
        await self.worker.run_once(block_milliseconds=1)
        self.assertEqual(self.handler.success, ["retry"])

    async def test_lease_deferral_does_not_consume_attempt_budget(self):
        self.handler.deferred_until = utc_now() + timedelta(minutes=2)
        await self.publish("leased")
        await self.worker.run_once(block_milliseconds=1)
        self.assertEqual(self.stream.acks, [])
        with self.sessions() as session:
            row = session.query(InboxEvent).one()
            self.assertEqual(row.attempts, 0)
            row.available_at = utc_now() - timedelta(seconds=1)
            session.commit()
        self.handler.deferred_until = None
        for _ in range(2):
            await self.worker.run_once(block_milliseconds=1)
        self.assertEqual(self.handler.success, ["leased"])

    async def test_ack_failure_retries_ack_without_reexecuting_success(self):
        await self.publish("good")
        with patch.object(self.stream, "xack", side_effect=RuntimeError("Redis disconnected")):
            await self.worker.run_once(block_milliseconds=1)
        self.assertEqual(self.handler.success, ["good"])
        self.assertEqual(len(self.stream.pending), 1)
        await self.worker.run_once(block_milliseconds=1)
        self.assertEqual(self.handler.success, ["good"])
        self.assertEqual(self.stream.pending, [])

    async def test_pending_cursor_reaches_messages_beyond_deferred_first_page(self):
        for index in range(4):
            await self.publish(f"event-{index}")
        self.handler.deferred_until = utc_now() + timedelta(minutes=2)
        await self.worker.run_once(count=2, block_milliseconds=1)
        self.handler.deferred_until = None
        for _ in range(3):
            await self.worker.run_once(count=2, block_milliseconds=1)
        self.assertEqual(self.handler.success, ["event-2", "event-3"])
