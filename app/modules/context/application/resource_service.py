"""Context Chain 资源事实与 Redis 热队列的跨存储编排。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any
from uuid import uuid4

from starlette.concurrency import run_in_threadpool

from app.modules.context.application.dto import ChainTurnUpdate
from app.modules.context.application.ports import (
    ContextRecordFactoryPort,
    ResourceQueuePort,
    UnitOfWorkFactory,
)
from app.modules.context.domain.enums import ContextResourceAction
from app.modules.context.domain.models import (
    ContextResourceQueue,
    ContextResourceRef,
)


@dataclass(frozen=True)
class ContextResourceQueueRefresh:
    """数据库提交后需要异步应用到 Redis 的资源增量刷新载荷。

    Attributes:
        conversation_id: 所属会话 ID。
        chain_id: 上下文链 ID。
        resources: 本轮引入或更新的资源列表。
        removed_resource_keys: 本轮显式停用或移除的资源 Key 列表。
        expected_previous_version: 期望的 Redis 缓存当前前置版本号。
        database_version: 数据库已持久化递增后的最新版本号。
    """

    conversation_id: str
    chain_id: str
    resources: list[ContextResourceRef]
    removed_resource_keys: list[str]
    expected_previous_version: int
    database_version: int


def split_resource_key(resource_key: str) -> tuple[str, str]:
    """将规范的资源 Key（格式为 `resource_type:resource_id`）拆分为类型和 ID。

    Args:
        resource_key: 资源唯一标识字符串。

    Returns:
        (resource_type, resource_id) 二元组。

    Raises:
        ValueError: 资源 Key 格式非法时抛出。
    """
    resource_type, separator, resource_id = resource_key.partition(":")
    if (
        not separator
        or not resource_type
        or not resource_id
        or re.fullmatch(r"[a-z][a-z0-9_]*", resource_type) is None
        or len(resource_type) > 100
        or len(resource_id) > 400
    ):
        raise ValueError(f"Invalid Context resource key: {resource_key}")
    return resource_type, resource_id


class ContextResourceService:
    """以 MySQL 为事实来源，维护可丢弃并可重建的 Redis 热队列。

    双存储维护机制：
    - MySQL：全量事实层。保存资源当前状态（context_chain_resources）与全生命周期事件日志（context_chain_resource_events）。
    - Redis：热队列层。为每条上下文链维护固定容量（默认 16）的刷新式 FIFO 队列，用于快速为 Context Agent 注入最近活跃资源。
    - 事务一致性：事务内仅修改 MySQL 并递增 resource_version；MySQL 提交后才增量刷新 Redis；缓存不一致时通过 warm_up_queue 从 MySQL 重新拉取预热。
    """

    def __init__(
        self,
        *,
        queue_repository: ResourceQueuePort,
        uow_factory: UnitOfWorkFactory,
        record_factory: ContextRecordFactoryPort,
    ) -> None:
        """初始化 ContextResourceService。

        Args:
            queue_repository: ResourceQueuePort 缓存仓储实例。
            uow_factory: UnitOfWork 工厂。
            record_factory: ContextRecordFactoryPort ORM 工厂。
        """
        self._queue_repository = queue_repository
        self._uow_factory = uow_factory
        self._record_factory = record_factory

    @property
    def queue_capacity(self) -> int:
        """获取热资源队列的最大容量限制（默认 16）。"""
        return self._queue_repository.capacity

    def empty_queue(self) -> ContextResourceQueue:
        """构建空的资源队列实例。

        Returns:
            空队列实例。
        """
        return ContextResourceQueue(capacity=self.queue_capacity)

    def apply_in_transaction(
        self,
        *,
        repository: Any,
        chain: Any,
        update: ChainTurnUpdate | None,
        turn_id: str,
        now: datetime,
    ) -> ContextResourceQueueRefresh | None:
        """在外部调用方的数据库事务中追加事件、更新资源当前状态并准备 Redis 刷新载荷。

        Args:
            repository: 上下文仓储对象。
            chain: 上下文链 ORM 实体。
            update: 下游提交的本轮链更新增量（ChainTurnUpdate）。
            turn_id: 当前关联的 Turn ID。
            now: 当前操作时间戳。

        Returns:
            若有资源变更则返回待刷新至 Redis 的载荷，否则返回 None。
        """
        if update is None:
            return None

        refreshed_resources: list[ContextResourceRef] = []
        # 1. 遍历处理关联或更新的资源
        for resource_input in update.related_resources:
            resource, created = repository.upsert_chain_resource(
                chain_id=chain.chain_id,
                resource_key=resource_input.resource_key,
                resource_type=resource_input.resource_type,
                resource_id=resource_input.resource_id,
                relation=resource_input.relation,
                summary=resource_input.summary,
                turn_id=turn_id,
                seen_at=now,
            )
            repository.create_resource_event(
                self._record_factory.context_resource_event(
                    event_id=f"resource_event_{uuid4().hex}",
                    chain_id=chain.chain_id,
                    turn_id=turn_id,
                    resource_key=resource.resource_key,
                    resource_type=resource.resource_type,
                    resource_id=resource.resource_id,
                    action=(
                        ContextResourceAction.SEEN.value
                        if created
                        else ContextResourceAction.REFRESHED.value
                    ),
                    relation=resource.relation,
                    summary=resource.summary,
                    created_at=now,
                )
            )
            refreshed_resources.append(
                ContextResourceRef(
                    resource_key=resource.resource_key,
                    resource_type=resource.resource_type,
                    resource_id=resource.resource_id,
                    relation=resource.relation,
                    summary=resource.summary,
                    source_turn_id=turn_id,
                    last_seen_at=now,
                )
            )

        # 2. 遍历处理显式停用或移除的资源
        for resource_key in update.removed_resource_keys:
            resource_type, resource_id = split_resource_key(resource_key)
            existing = repository.deactivate_chain_resource(
                chain_id=chain.chain_id,
                resource_key=resource_key,
                removed_at=now,
            )
            repository.create_resource_event(
                self._record_factory.context_resource_event(
                    event_id=f"resource_event_{uuid4().hex}",
                    chain_id=chain.chain_id,
                    turn_id=turn_id,
                    resource_key=resource_key,
                    resource_type=(
                        existing.resource_type
                        if existing is not None
                        else resource_type
                    ),
                    resource_id=(
                        existing.resource_id
                        if existing is not None
                        else resource_id
                    ),
                    action=ContextResourceAction.REMOVED.value,
                    relation=(
                        existing.relation if existing is not None else None
                    ),
                    summary=(
                        existing.summary if existing is not None else None
                    ),
                    created_at=now,
                )
            )

        if not refreshed_resources and not update.removed_resource_keys:
            return None

        # 3. 递增链的 resource_version
        previous_version = chain.resource_version
        database_version = repository.increment_resource_version(chain)
        return ContextResourceQueueRefresh(
            conversation_id=chain.conversation_id,
            chain_id=chain.chain_id,
            resources=refreshed_resources,
            removed_resource_keys=list(update.removed_resource_keys),
            expected_previous_version=previous_version,
            database_version=database_version,
        )

    async def get_queue(
        self,
        *,
        conversation_id: str,
        chain_id: str,
        resource_version: int,
    ) -> ContextResourceQueue:
        """优先从 Redis 读取版本一致的热队列；未命中或版本不一致时从 MySQL 全量预热。

        Args:
            conversation_id: 会话 ID。
            chain_id: 上下文链 ID。
            resource_version: 数据库当前最新的资源版本号。

        Returns:
            校验或预热完成的热资源队列。
        """
        # 1. 尝试从 Redis 读取版本匹配的热队列
        try:
            cached = await self._queue_repository.get(
                conversation_id=conversation_id,
                chain_id=chain_id,
                expected_version=resource_version,
            )
        except Exception:
            cached = None
        if cached is not None:
            return cached

        # 2. Redis 未命中或版本不一致，从 MySQL 预热最近活跃的最多 N 个资源
        resources = await run_in_threadpool(
            self._load_resources_for_warmup,
            conversation_id,
            chain_id,
            resource_version,
        )
        try:
            await self._queue_repository.replace(
                conversation_id=conversation_id,
                chain_id=chain_id,
                resources=resources,
                database_version=resource_version,
            )
        except Exception:
            await self._best_effort_invalidate(
                conversation_id=conversation_id,
                chain_id=chain_id,
            )
        return ContextResourceQueue(
            capacity=self.queue_capacity,
            items=resources,
        )

    async def refresh_after_commit(
        self,
        refresh: ContextResourceQueueRefresh,
    ) -> None:
        """在数据库事务提交成功后，将资源增量刷新至 Redis；若刷新失败则安全失效缓存。

        Args:
            refresh: ContextResourceQueueRefresh 增量刷新载荷。
        """
        try:
            applied = await self._queue_repository.refresh(
                conversation_id=refresh.conversation_id,
                chain_id=refresh.chain_id,
                resources=refresh.resources,
                removed_resource_keys=refresh.removed_resource_keys,
                expected_previous_version=(
                    refresh.expected_previous_version
                ),
                database_version=refresh.database_version,
            )
            # 若因版本不连续等原因导致增量刷新未应用，则删除 Redis 缓存以便下次读取时从 MySQL 预热
            if not applied:
                await self._best_effort_invalidate(
                    conversation_id=refresh.conversation_id,
                    chain_id=refresh.chain_id,
                )
        except Exception:
            await self._best_effort_invalidate(
                conversation_id=refresh.conversation_id,
                chain_id=refresh.chain_id,
            )

    async def invalidate_chain(
        self,
        *,
        conversation_id: str,
        chain_id: str,
    ) -> None:
        """失效并删除指定链在 Redis 中的热资源队列缓存（数据库持久化历史保持不变）。

        Args:
            conversation_id: 会话 ID。
            chain_id: 上下文链 ID。
        """
        await self._queue_repository.invalidate(
            conversation_id=conversation_id,
            chain_id=chain_id,
        )

    def _load_resources_for_warmup(
        self,
        conversation_id: str,
        chain_id: str,
        resource_version: int,
    ) -> list[ContextResourceRef]:
        """从 MySQL 事实表读取指定链最近使用的 active 资源，并反转为最旧到最新的 FIFO 队列顺序。

        Args:
            conversation_id: 会话 ID。
            chain_id: 上下文链 ID。
            resource_version: 资源版本号。

        Returns:
            按最久未使用到最近使用排列的资源引用列表。

        Raises:
            RuntimeError: 链不存在、会话归属不一致或版本已变化。
        """
        with self._uow_factory() as uow:
            chain = uow.context.get_chain(chain_id)
            if chain is None:
                raise RuntimeError(f"Context Chain 不存在: {chain_id}")
            if chain.conversation_id != conversation_id:
                raise RuntimeError(
                    f"Context Chain 会话归属不一致: {chain_id}"
                )
            if chain.resource_version != resource_version:
                raise RuntimeError(
                    f"Context Chain 资源版本已变化: {chain_id}"
                )

            # 查询最近活跃的最多 capacity 个资源（最新在前）
            newest_first = uow.context.list_resources_for_warmup(
                chain_id,
                limit=self.queue_capacity,
            )
            # 反转为队头最旧、队尾最新的 FIFO 队列排列
            return [
                ContextResourceRef(
                    resource_key=resource.resource_key,
                    resource_type=resource.resource_type,
                    resource_id=resource.resource_id,
                    relation=resource.relation,
                    summary=resource.summary,
                    source_turn_id=resource.last_seen_turn_id,
                    last_seen_at=resource.last_seen_at,
                )
                for resource in reversed(newest_first)
            ]

    async def _best_effort_invalidate(
        self,
        *,
        conversation_id: str,
        chain_id: str,
    ) -> None:
        """尽力删除 Redis 热队列 Key，忽略网络与连接异常。"""
        try:
            await self._queue_repository.invalidate(
                conversation_id=conversation_id,
                chain_id=chain_id,
            )
        except Exception:
            pass
