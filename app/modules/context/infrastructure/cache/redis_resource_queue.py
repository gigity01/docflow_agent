"""Context Chain Redis 刷新式 FIFO 热资源队列基础设施。

维护结构：
- KEYS[1] (List): 保存 resource_key 的顺序列表，队头为最旧未访问，队尾为最新活跃。
- KEYS[2] (Hash): 保存 resource_key 到对应 ContextResourceRef JSON 的哈希映射。
- KEYS[3] (String): 保存当前缓存对应的 resource_version 版本号。
"""

from __future__ import annotations

from redis.asyncio import Redis

from app.modules.context.domain.models import (
    ContextResourceQueue,
    ContextResourceRef,
)


# Lua 脚本：原子执行版本 CAS 检查、增量刷新与淘汰、版本号推进
REFRESH_QUEUE_LUA = """
local max_size = tonumber(ARGV[1])
local database_version = ARGV[2]
local expected_previous_version = ARGV[3]
local current_version = redis.call("GET", KEYS[3])
-- 1. CAS 校验：当前缓存版本必须严格等于数据库变更前的一致版本
if current_version then
    if current_version ~= expected_previous_version then
        return -1
    end
elseif expected_previous_version ~= "0" then
    return -1
end

local refresh_count = tonumber(ARGV[4])
local index = 5

-- 2. 刷新式插入：旧位置移除后重新进入队尾 (RPUSH)，并更新 Hash 详情
for _ = 1, refresh_count do
    local resource_key = ARGV[index]
    local resource_json = ARGV[index + 1]
    redis.call("LREM", KEYS[1], 0, resource_key)
    redis.call("RPUSH", KEYS[1], resource_key)
    redis.call("HSET", KEYS[2], resource_key, resource_json)
    index = index + 2
end

-- 3. 显式移除失效或显式停用的资源
local remove_count = tonumber(ARGV[index])
index = index + 1
for _ = 1, remove_count do
    local resource_key = ARGV[index]
    redis.call("LREM", KEYS[1], 0, resource_key)
    redis.call("HDEL", KEYS[2], resource_key)
    index = index + 1
end

-- 4. 容量限制与淘汰：超过最大容量时从队头 (LPOP) 推出最久未再次使用的资源
local length = redis.call("LLEN", KEYS[1])
while length > max_size do
    local removed = redis.call("LPOP", KEYS[1])
    if removed then
        redis.call("HDEL", KEYS[2], removed)
    end
    length = length - 1
end

-- 5. 更新缓存版本号为数据库新版本
redis.call("SET", KEYS[3], database_version)
return length
""".strip()


REPLACE_QUEUE_LUA = """
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])

local item_count = tonumber(ARGV[1])
local index = 2
for _ = 1, item_count do
    local resource_key = ARGV[index]
    local resource_json = ARGV[index + 1]
    redis.call("RPUSH", KEYS[1], resource_key)
    redis.call("HSET", KEYS[2], resource_key, resource_json)
    index = index + 2
end

redis.call("SET", KEYS[3], ARGV[index])
return item_count
""".strip()


def _as_text(value: str | bytes | int) -> str:
    """将 Redis 返回的字符串、字节串或数值转换为 utf-8 字符串。

    Args:
        value: Redis 原始返回值。

    Returns:
        转换后的字符串。
    """
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


class ContextResourceQueueRepository:
    """使用 Redis List、Hash 和版本 Key 保存有界热资源队列的基础设施仓储。

    缓存键结构规范：
    - List (`{prefix}:queue`): 保存 resource_key 的双向列表，队头（Index 0）为最久未再次使用的资源，队尾为最新活跃资源。
    - Hash (`{prefix}:data`): 保存 resource_key 到 ContextResourceRef JSON 字符串的映射。
    - String (`{prefix}:version`): 保存当前缓存对应的数据库 resource_version。
    """

    def __init__(self, client: Redis, *, capacity: int) -> None:
        """初始化 ContextResourceQueueRepository。

        Args:
            client: 应用级 Redis 异步客户端实例。
            capacity: 热队列容量上限（必须大于等于 1，默认 16）。

        Raises:
            ValueError: 当 capacity 小于 1 时抛出。
        """
        if capacity < 1:
            raise ValueError("Context resource queue capacity must be positive")
        self._client = client
        self.capacity = capacity

    @staticmethod
    def _keys(conversation_id: str, chain_id: str) -> tuple[str, str, str]:
        """生成特定上下文链的 Redis 键三元组（Hash Tag 保证同一槽位以支持 Lua 事务）。

        Args:
            conversation_id: 会话 ID。
            chain_id: 上下文链 ID。

        Returns:
            tuple[str, str, str]: (queue_key, data_key, version_key)。
        """
        prefix = f"ctx:{{{conversation_id}}}:chain:{chain_id}:resource"
        return (
            f"{prefix}:queue",
            f"{prefix}:data",
            f"{prefix}:version",
        )

    async def get(
        self,
        *,
        conversation_id: str,
        chain_id: str,
        expected_version: int,
    ) -> ContextResourceQueue | None:
        """版本一致且 List/Hash 完整时返回热队列，否则返回未命中（None）。

        Args:
            conversation_id: 会话 ID。
            chain_id: 上下文链 ID。
            expected_version: 期望匹配的数据库资源版本号。

        Returns:
            命中的热资源队列模型；若版本不匹配、数据缺损或超出容量则返回 None。
        """
        queue_key, data_key, version_key = self._keys(
            conversation_id,
            chain_id,
        )
        # 1. 检查版本号是否完全一致
        raw_version = await self._client.get(version_key)
        if (
            raw_version is None
            or _as_text(raw_version) != str(expected_version)
        ):
            return None

        # 2. 读取 List 中的全部 resource_key
        raw_keys = await self._client.lrange(queue_key, 0, -1)
        resource_keys = [_as_text(item) for item in raw_keys]
        if not resource_keys:
            return ContextResourceQueue(capacity=self.capacity)
        if len(resource_keys) > self.capacity:
            return None

        # 3. 批量从 Hash 中按 key 顺序获取对应的 JSON 数据
        raw_items = await self._client.hmget(data_key, resource_keys)
        if len(raw_items) != len(resource_keys):
            return None

        # 4. 反序列化并核验 key 一致性
        items: list[ContextResourceRef] = []
        for resource_key, raw_item in zip(
            resource_keys,
            raw_items,
            strict=True,
        ):
            if raw_item is None:
                return None
            item = ContextResourceRef.model_validate_json(
                _as_text(raw_item)
            )
            if item.resource_key != resource_key:
                return None
            items.append(item)

        return ContextResourceQueue(
            capacity=self.capacity,
            items=items,
        )

    async def refresh(
        self,
        *,
        conversation_id: str,
        chain_id: str,
        resources: list[ContextResourceRef],
        removed_resource_keys: list[str],
        expected_previous_version: int,
        database_version: int,
    ) -> bool:
        """版本连续时通过 Lua 脚本原子刷新资源与版本号；缓存缺口或版本冲突返回 False。

        Args:
            conversation_id: 会话 ID。
            chain_id: 上下文链 ID。
            resources: 需加入或刷新活跃位置的资源列表。
            removed_resource_keys: 需显式移除的资源 Key 列表。
            expected_previous_version: 期望的 Redis 当前版本号。
            database_version: 数据库更新后的新版本号。

        Returns:
            增量应用成功返回 True，CAS 失败或缓存不存在返回 False。
        """
        keys = self._keys(conversation_id, chain_id)
        arguments: list[str | int] = [
            self.capacity,
            database_version,
            expected_previous_version,
            len(resources),
        ]
        for resource in resources:
            arguments.extend(
                [resource.resource_key, resource.model_dump_json()]
            )
        arguments.append(len(removed_resource_keys))
        arguments.extend(removed_resource_keys)
        result = await self._client.eval(
            REFRESH_QUEUE_LUA,
            len(keys),
            *keys,
            *arguments,
        )
        return int(result) >= 0

    async def replace(
        self,
        *,
        conversation_id: str,
        chain_id: str,
        resources: list[ContextResourceRef],
        database_version: int,
    ) -> None:
        """使用数据库最近资源通过 Lua 脚本原子替换/预热整个 Redis 队列。

        Args:
            conversation_id: 会话 ID。
            chain_id: 上下文链 ID。
            resources: 预热资源列表（按最旧到最新顺序）。
            database_version: 数据库版本号。
        """
        resources = resources[-self.capacity :]
        keys = self._keys(conversation_id, chain_id)
        arguments: list[str | int] = [len(resources)]
        for resource in resources:
            arguments.extend(
                [resource.resource_key, resource.model_dump_json()]
            )
        arguments.append(database_version)
        await self._client.eval(
            REPLACE_QUEUE_LUA,
            len(keys),
            *keys,
            *arguments,
        )

    async def invalidate(
        self,
        *,
        conversation_id: str,
        chain_id: str,
    ) -> None:
        """删除热队列（List、Hash、Version）；数据库资源事实不受影响。

        Args:
            conversation_id: 会话 ID。
            chain_id: 上下文链 ID。
        """
        await self._client.delete(
            *self._keys(conversation_id, chain_id)
        )
