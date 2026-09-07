"""在 CI 的隔离 MySQL/Redis 中验证真实 PEL、死信和重放，不调用模型。"""

import os
import unittest
from uuid import uuid4


@unittest.skipUnless(os.getenv("RUN_LIVE_RECOVERY_TESTS") == "1", "仅在隔离服务 CI 中运行")
class LiveDeliveryRecoveryTest(unittest.IsolatedAsyncioTestCase):
    async def test_poison_retry_exhaustion_and_manual_replay(self):
        from redis.asyncio import Redis
        from app.config.settings import REDIS_URL
        from app.infrastructure.database.session import session_local
        from app.infrastructure.database.uow import SQLAlchemyUnitOfWork
        from app.modules.context.infrastructure.persistence.models.conversation_turn import ConversationTurn
        from app.modules.planning.infrastructure.persistence.models import Plan
        from app.modules.messaging.infrastructure.persistence.models import InboxEvent, OutboxEvent
        from app.modules.messaging.application.delivery import MessageDeliveryService
        from app.modules.messaging.application.outbox import OutboxPublisher
        from app.modules.messaging.infrastructure.redis_streams import RedisStreamPublisher, RedisStreamWorker

        token = "verify_" + uuid4().hex
        redis = Redis.from_url(REDIS_URL, decode_responses=True)
        stream = token + "_stream"
        with session_local() as session:
            session.add(ConversationTurn(turn_id=token, conversation_id=token,
                user_input="isolated CI delivery test", task_ids=[], status="processing"))
            session.flush()
            session.add(Plan(plan_id=token, workflow_id=token, turn_id=token, revision=1, status="ready"))
            session.commit()
        delivery = MessageDeliveryService(uow_factory=SQLAlchemyUnitOfWork,
            inbox_factory=InboxEvent, outbox_factory=OutboxEvent, retry_delay_seconds=0)

        class Handler:
            failed = True
            calls = 0
            success = 0

            async def handle(self, event):
                self.calls += 1
                if self.failed:
                    raise RuntimeError("injected transient error")
                self.success += 1

        handler = Handler()
        publisher = RedisStreamPublisher(redis, stream_name=stream)
        worker = RedisStreamWorker(redis, dispatcher=handler, delivery=delivery,
            stream_name=stream, group_name=token, consumer_name=token)
        try:
            await redis.xadd(stream, {"event_id": token + "_bad", "event_type": "invalid", "payload": "invalid"})
            await publisher.publish(event_id=token, event_type="runtime.plan_wakeup", payload={"plan_id": token})
            for _ in range(6):
                await worker.run_once(block_milliseconds=1)
            self.assertEqual(handler.calls, 3)
            with session_local() as session:
                failure = session.query(InboxEvent).filter_by(consumer_name="runtime.delivery", event_id=token).one()
                failure_id = failure.inbox_id
                self.assertEqual(failure.status, "dead_letter")
            pending = await redis.xpending(stream, token)
            self.assertEqual(pending["pending"], 0)
            handler.failed = False
            delivery.retry(failure_id)
            outbox = OutboxPublisher(uow_factory=SQLAlchemyUnitOfWork, publisher=publisher)
            await outbox.publish_batch()
            await worker.run_once(block_milliseconds=1)
            self.assertEqual(handler.success, 1)
            await publisher.publish(event_id=token, event_type="runtime.plan_wakeup", payload={"plan_id": token})
            await worker.run_once(block_milliseconds=1)
            self.assertEqual(handler.success, 1)
            self.assertEqual(delivery.list_failures(plan_id=token), [])
        finally:
            await redis.delete(stream)
            await redis.aclose()
            # CI 数据库和流随作业销毁；保留失败现场供日志检查，不清理其他行。
