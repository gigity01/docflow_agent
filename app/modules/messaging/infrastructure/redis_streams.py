"""Redis Streams 消息发布与独立 Worker 消费适配器。

提供基于 Redis Streams 的异步消息通道：
- RedisStreamPublisher: 将事件序列化为 JSON 字符串并写入指定 Redis Stream。
- RedisStreamWorker: 在独立 Runtime Worker 进程中循环消费。通过 Consumer Group 机制消费消息，
  按优先级处理超时挂起消息（XAUTOCLAIM）、本消费者未 ACK 积压消息（XREADGROUP ID 0）以及新消息（XREADGROUP ID >）。
  在业务处理成功后显式调用 XACK 进行确认；消费失败时消息保持 pending 状态以支持后续重试或由其它 Worker 实例接管。
"""

import json

from redis.exceptions import ResponseError

from app.modules.messaging.application.dto import RuntimeEvent


class RedisStreamPublisher:
    """基于 Redis Streams 的消息发布实现。

    实现 MessagePublisherPort 协议，负责将事件载荷结构化序列化后调用 XADD 追加到 Stream 中。
    """

    def __init__(self, redis_client, *, stream_name: str = "agent-runtime") -> None:
        """初始化 Redis Streams 发布器。

        Args:
            redis_client: Redis 异步客户端（如 redis.asyncio.Redis）。
            stream_name: 目标 Redis Stream 队列名称，默认为 "agent-runtime"。
        """
        self._redis = redis_client
        self._stream_name = stream_name

    async def publish(
        self,
        *,
        event_id: str,
        event_type: str,
        payload: dict,
    ) -> None:
        """将事件数据写入 Redis Stream。

        Args:
            event_id: 事件全局唯一标识符。
            event_type: 事件类型名称。
            payload: 事件参数载荷字典。
        """
        await self._redis.xadd(
            self._stream_name,
            {
                "event_id": event_id,
                "event_type": event_type,
                "payload": json.dumps(
                    payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            },
        )


class RedisStreamWorker:
    """Redis Streams 消费者 Worker。

    由独立 Worker 进程循环调用，不挂载到 FastAPI lifespan。
    采用 XAUTOCLAIM 与 XREADGROUP 支持 pending 消息恢复与跨实例消费接管。
    """

    def __init__(
        self,
        redis_client,
        *,
        dispatcher,
        stream_name: str = "agent-runtime",
        group_name: str = "agent-runtime-workers",
        consumer_name: str,
        claim_min_idle_milliseconds: int = 60_000,
    ) -> None:
        """初始化 Redis Streams 消费 Worker。

        Args:
            redis_client: Redis 异步客户端。
            dispatcher: 事件分派器（实现 handle(event) 方法）。
            stream_name: 消费的 Redis Stream 名称。
            group_name: 消费者组名称。
            consumer_name: 当前消费者实例唯一标识（用于分布式认领与跟踪）。
            claim_min_idle_milliseconds: 自动认领超时挂起消息的最小空闲毫秒数（默认 60 秒）。
        """
        self._redis = redis_client
        self._dispatcher = dispatcher
        self._stream_name = stream_name
        self._group_name = group_name
        self._consumer_name = consumer_name
        self._claim_min_idle_milliseconds = claim_min_idle_milliseconds

    async def ensure_group(self) -> None:
        """确保目标 Stream 及其对应的消费者组（Consumer Group）已创建。

        若组已存在（BUSYGROUP 异常），则安全忽略。
        """
        try:
            await self._redis.xgroup_create(
                self._stream_name,
                self._group_name,
                id="0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def run_once(
        self,
        *,
        count: int = 10,
        block_milliseconds: int = 5_000,
    ) -> int:
        """执行单轮消息拉取与分派。

        优先级策略：
        1. 优先通过 XAUTOCLAIM 认领其它崩溃或超时 Worker 遗留的超时挂起消息。
        2. 其次读取当前 Consumer 自身历史尚未 ACK 的积压消息（stream_id="0"）。
        3. 最后阻塞等待并读取新到达的消息（stream_id=">"）。

        Args:
            count: 单次最大读取/处理条数。
            block_milliseconds: 读取新消息时的最大阻塞毫秒数。

        Returns:
            本轮成功分派并 ACK 的消息条数。
        """
        await self.ensure_group()

        # 阶段 1: 认领并处理超时挂起消息
        handled = await self._claim_and_dispatch(count=count)
        if handled:
            return handled

        # 阶段 2: 处理当前消费者未 ACK 的历史积压消息
        handled = await self._read_and_dispatch(stream_id="0", count=count)
        if handled:
            return handled

        # 阶段 3: 阻塞拉取全新到达的消息
        return await self._read_and_dispatch(
            stream_id=">",
            count=count,
            block_milliseconds=block_milliseconds,
        )

    async def _claim_and_dispatch(self, *, count: int) -> int:
        """自动认领其他 Consumer 空闲超时的 pending 消息并分派处理。

        Args:
            count: 本次认领的最大消息数量。

        Returns:
            成功处理的消息数量。
        """
        claimed = await self._redis.xautoclaim(
            self._stream_name,
            self._group_name,
            self._consumer_name,
            self._claim_min_idle_milliseconds,
            start_id="0-0",
            count=count,
        )
        messages = claimed[1] if len(claimed) > 1 else []
        return await self._dispatch_messages(messages)

    async def _read_and_dispatch(
        self,
        *,
        stream_id: str,
        count: int,
        block_milliseconds: int | None = None,
    ) -> int:
        """从消费者组中读取指定 stream_id 的消息并分派处理。

        Args:
            stream_id: 消息 ID 起点（"0" 表示未确认历史消息，">" 表示新消息）。
            count: 最大拉取数量。
            block_milliseconds: 阻塞毫秒数（为 None 时不阻塞）。

        Returns:
            成功处理的消息数量。
        """
        streams = await self._redis.xreadgroup(
            self._group_name,
            self._consumer_name,
            {self._stream_name: stream_id},
            count=count,
            block=block_milliseconds,
        )
        handled = 0
        for _, messages in streams:
            handled += await self._dispatch_messages(messages)
        return handled

    async def _dispatch_messages(self, messages) -> int:
        """逐条分派消息至业务分派器，并在业务处理成功后显式发送 XACK。

        若业务分派抛出异常，消息将不会被 ACK，保留在 PEL（Pending Entries List）中供后续重试。

        Args:
            messages: Redis Stream 返回的消息元组列表 [(message_id, fields), ...]。

        Returns:
            成功处理并确认的消息数量。
        """
        handled = 0
        for message_id, fields in messages:
            event = self._parse_event(fields)
            # 业务分派处理：失败抛出异常直接中断并保持 pending
            await self._dispatcher.handle(event)
            # 业务成功后显式确认消息
            await self._redis.xack(
                self._stream_name,
                self._group_name,
                message_id,
            )
            handled += 1
        return handled

    @staticmethod
    def _parse_event(fields: dict) -> RuntimeEvent:
        """解析 Redis Stream 字典字段为 RuntimeEvent 数据对象。

        Args:
            fields: Stream 消息中的键值对字典。

        Returns:
            RuntimeEvent: 解析后的运行时事件对象。

        Raises:
            ValueError: 当 payload 字段不是有效的 JSON Object 时抛出。
        """
        payload = json.loads(fields["payload"])
        if not isinstance(payload, dict):
            raise ValueError("Runtime Event payload 必须是 JSON Object")
        return RuntimeEvent(
            event_id=fields["event_id"],
            event_type=fields["event_type"],
            payload=payload,
        )
