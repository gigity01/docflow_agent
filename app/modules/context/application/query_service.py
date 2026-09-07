"""Context 持久化事实的只读查询服务实现。"""

from __future__ import annotations

from app.modules.context.application.errors import ContextQueryError
from app.modules.context.application.ports import UnitOfWorkFactory
from app.modules.context.application.query_dto import (
    ContextChainListResult,
    ContextChainNodeListResult,
    ContextChainNodeQueryResult,
    ContextChainNodeSearchQuery,
    ContextChainQueryResult,
    ContextChainResourceListResult,
    ContextChainResourceQueryResult,
    ContextChainResourceSearchQuery,
    ContextChainSearchQuery,
    ContextSelectionRecordListResult,
    ContextSelectionRecordQueryResult,
    ContextSelectionRecordSearchQuery,
    ConversationTurnListResult,
    ConversationTurnQueryResult,
    ConversationTurnSearchQuery,
)


class ContextQueryService:
    """提供纯只读查询能力，不改变 Chain 的活跃时间或资源版本号。"""

    def __init__(self, *, uow_factory: UnitOfWorkFactory) -> None:
        """初始化 ContextQueryService。

        Args:
            uow_factory: UnitOfWork 工厂，用于提供只读数据库事务会话。
        """
        self._uow_factory = uow_factory

    def get_conversation_turn(
        self,
        turn_id: str,
    ) -> ConversationTurnQueryResult:
        """读取指定 Conversation Turn 记录。

        Args:
            turn_id: Turn 唯一标识。

        Returns:
            命中的 Turn 查询结果 DTO。

        Raises:
            ContextQueryError: 当 Turn 不存在时抛出 404。
        """
        with self._uow_factory() as uow:
            turn = uow.context.get_turn(turn_id)
            if turn is None:
                raise ContextQueryError(404, "Context Turn 不存在")
            return ConversationTurnQueryResult.model_validate(turn)

    def list_conversation_turns(
        self,
        query: ConversationTurnSearchQuery,
    ) -> ConversationTurnListResult:
        """分页搜索 Conversation Turn 列表。

        Args:
            query: 搜索过滤与分页参数。

        Returns:
            包含结果列表与分页总数。
        """
        with self._uow_factory() as uow:
            items = uow.context.search_turns(query)
            return ConversationTurnListResult(
                items=[
                    ConversationTurnQueryResult.model_validate(item)
                    for item in items
                ],
                total=uow.context.count_turns(query),
                limit=query.limit,
                offset=query.offset,
            )

    def get_context_chain(self, chain_id: str) -> ContextChainQueryResult:
        """读取指定 Context Chain 记录。

        Args:
            chain_id: Context Chain 唯一标识。

        Returns:
            命中的 Chain 查询结果 DTO。

        Raises:
            ContextQueryError: 当 Chain 不存在时抛出 404。
        """
        with self._uow_factory() as uow:
            chain = uow.context.get_chain(chain_id)
            if chain is None:
                raise ContextQueryError(404, "Context Chain 不存在")
            return ContextChainQueryResult.model_validate(chain)

    def list_context_chains(
        self,
        query: ContextChainSearchQuery,
    ) -> ContextChainListResult:
        """分页搜索 Context Chain 列表。

        Args:
            query: 搜索过滤与分页参数。

        Returns:
            包含结果列表与分页总数。
        """
        with self._uow_factory() as uow:
            items = uow.context.search_chains(query)
            return ContextChainListResult(
                items=[
                    ContextChainQueryResult.model_validate(item)
                    for item in items
                ],
                total=uow.context.count_chains(query),
                limit=query.limit,
                offset=query.offset,
            )

    def list_context_chain_nodes(
        self,
        query: ContextChainNodeSearchQuery,
    ) -> ContextChainNodeListResult:
        """分页搜索 Context Chain Node 列表。

        Args:
            query: 搜索过滤与分页参数。

        Returns:
            包含结果列表与分页总数。
        """
        with self._uow_factory() as uow:
            items = uow.context.search_nodes(query)
            return ContextChainNodeListResult(
                items=[
                    ContextChainNodeQueryResult.model_validate(item)
                    for item in items
                ],
                total=uow.context.count_nodes(query),
                limit=query.limit,
                offset=query.offset,
            )

    def list_context_chain_resources(
        self,
        query: ContextChainResourceSearchQuery,
    ) -> ContextChainResourceListResult:
        """分页搜索 Context Chain Resource 列表。

        Args:
            query: 搜索过滤与分页参数。

        Returns:
            包含结果列表与分页总数。
        """
        with self._uow_factory() as uow:
            items = uow.context.search_resources(query)
            return ContextChainResourceListResult(
                items=[
                    ContextChainResourceQueryResult.model_validate(item)
                    for item in items
                ],
                total=uow.context.count_resources(query),
                limit=query.limit,
                offset=query.offset,
            )

    def list_context_selection_records(
        self,
        query: ContextSelectionRecordSearchQuery,
    ) -> ContextSelectionRecordListResult:
        """分页搜索 Context SelectionRecord 决策记录列表。

        Args:
            query: 搜索过滤与分页参数。

        Returns:
            包含结果列表与分页总数。
        """
        with self._uow_factory() as uow:
            items = uow.context.search_selection_records(query)
            return ContextSelectionRecordListResult(
                items=[
                    ContextSelectionRecordQueryResult.model_validate(item)
                    for item in items
                ],
                total=uow.context.count_selection_records(query),
                limit=query.limit,
                offset=query.offset,
            )
