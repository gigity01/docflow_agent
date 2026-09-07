"""Context Application 层外部能力抽象接口（Port / Protocol）。"""

from __future__ import annotations

from collections.abc import Callable
from types import TracebackType
from typing import Any, AsyncContextManager, Protocol

from app.modules.context.application.dto import ContextAgentInput
from app.modules.context.domain.models import (
    ContextChain,
    ContextResourceQueue,
    ContextResourceRef,
    ContextSelectionDecision,
)


class ContextRouterPort(Protocol):
    """历史 Context Read Set 选择器抽象端口。"""

    async def route(
        self,
        agent_input: ContextAgentInput,
    ) -> ContextSelectionDecision:
        """选择 Planner 理解当前消息所需的历史链读取集合（Read Set）。

        Args:
            agent_input: 包含当前用户输入与候选未归档链的完整输入。

        Returns:
            历史读取集合选择结果。
        """


class ConversationLockPort(Protocol):
    """会话级并发短锁抽象端口。"""

    def hold(
        self,
        conversation_id: str,
    ) -> AsyncContextManager[None]:
        """获取指定会话的异步分布式短锁，用于串行化 Context Selection 与完成写回。

        Args:
            conversation_id: 会话唯一标识。

        Returns:
            异步上下文管理器。
        """


class ResourceQueuePort(Protocol):
    """热资源队列缓存存储（Redis）抽象端口。"""

    capacity: int

    async def get(
        self,
        *,
        conversation_id: str,
        chain_id: str,
        expected_version: int,
    ) -> ContextResourceQueue | None:
        """从 Redis 读取指定版本一致的热资源队列。

        Args:
            conversation_id: 会话 ID。
            chain_id: 上下文链 ID。
            expected_version: 期望匹配的数据库资源版本号。

        Returns:
            缓存命中且版本一致时返回队列模型，否则返回 None。
        """

    async def replace(
        self,
        *,
        conversation_id: str,
        chain_id: str,
        resources: list[ContextResourceRef],
        database_version: int,
    ) -> None:
        """以数据库全量事实整体预热/替换 Redis 热资源队列。

        Args:
            conversation_id: 会话 ID。
            chain_id: 上下文链 ID。
            resources: 经过排序的最新活跃资源列表。
            database_version: 当前数据库事实版本号。
        """

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
        """按版本号原子应用资源增量刷新（基于 Lua 脚本）。

        Args:
            conversation_id: 会话 ID。
            chain_id: 上下文链 ID。
            resources: 本轮引入或更新的资源列表。
            removed_resource_keys: 本轮停用或移除的资源 Key 列表。
            expected_previous_version: 期望的 Redis 当前前置版本。
            database_version: 数据库更新后的新版本。

        Returns:
            增量应用成功返回 True，版本不一致或缓存缺失返回 False。
        """

    async def invalidate(
        self,
        *,
        conversation_id: str,
        chain_id: str,
    ) -> None:
        """失效并删除指定链在 Redis 中的热队列相关 Key。

        Args:
            conversation_id: 会话 ID。
            chain_id: 上下文链 ID。
        """


class ContextUnitOfWorkPort(Protocol):
    """Context 模块 UnitOfWork 事务抽象端口。"""

    context: Any

    def __enter__(self) -> "ContextUnitOfWorkPort":
        """进入事务上下文。"""
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """退出事务上下文。"""
        ...

    def commit(self) -> None:
        """提交当前事务。"""
        ...


UnitOfWorkFactory = Callable[[], ContextUnitOfWorkPort]


class ContextChainMapperPort(Protocol):
    """上下文链 ORM 实体到领域模型映射器接口。"""

    def __call__(
        self,
        chain: Any,
        *,
        resource_queue: ContextResourceQueue,
    ) -> ContextChain:
        """将持久化 ContextChain 实体映射为包含热资源队列的完整 ContextChain 领域模型。

        Args:
            chain: 持久化 ORM ContextChain 对象。
            resource_queue: 注入的 ContextResourceQueue 对象。

        Returns:
            组装完毕的领域模型对象。
        """


class ContextRecordFactoryPort(Protocol):
    """Context ORM 实体工厂接口。"""

    def conversation_turn(self, **values: Any) -> Any:
        """创建 ConversationTurn ORM 实体实例。"""
        ...

    def context_selection_record(self, **values: Any) -> Any:
        """创建 ContextSelectionRecord ORM 实体实例。"""
        ...

    def context_chain(self, **values: Any) -> Any:
        """创建 ContextChain ORM 实体实例。"""
        ...

    def context_chain_node(self, **values: Any) -> Any:
        """创建 ContextChainNode ORM 实体实例。"""
        ...

    def context_resource_event(self, **values: Any) -> Any:
        """创建 ContextResourceEvent ORM 实体实例。"""
        ...
