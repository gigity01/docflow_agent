"""Context 持久化事实的显式只读 Use Case 实现。"""

from __future__ import annotations

from app.modules.context.application.query_dto import (
    ContextChainListResult,
    ContextChainNodeListResult,
    ContextChainNodeSearchQuery,
    ContextChainQueryResult,
    ContextChainResourceListResult,
    ContextChainResourceSearchQuery,
    ContextChainSearchQuery,
    ContextSelectionRecordListResult,
    ContextSelectionRecordSearchQuery,
    ConversationTurnListResult,
    ConversationTurnQueryResult,
    ConversationTurnSearchQuery,
)
from app.modules.context.application.query_service import ContextQueryService


class GetConversationTurnUseCase:
    """读取指定 Conversation Turn 记录详情的只读用例。"""

    def __init__(self, query_service: ContextQueryService) -> None:
        self._query_service = query_service

    def execute(self, turn_id: str) -> ConversationTurnQueryResult:
        """执行查询指定 Turn 详情。

        Args:
            turn_id: Turn 唯一标识。

        Returns:
            Turn 查询结果。
        """
        return self._query_service.get_conversation_turn(turn_id)


class ListConversationTurnsUseCase:
    """分页查询并筛选 Conversation Turn 列表的只读用例。"""

    def __init__(self, query_service: ContextQueryService) -> None:
        self._query_service = query_service

    def execute(
        self,
        query: ConversationTurnSearchQuery,
    ) -> ConversationTurnListResult:
        """执行分页查询 Conversation Turn 列表。

        Args:
            query: 查询筛选与分页参数。

        Returns:
            分页查询结果。
        """
        return self._query_service.list_conversation_turns(query)


class GetContextChainUseCase:
    """读取指定 Context Chain 及其最新状态的只读用例。"""

    def __init__(self, query_service: ContextQueryService) -> None:
        self._query_service = query_service

    def execute(self, chain_id: str) -> ContextChainQueryResult:
        """执行查询指定 Context Chain 详情。

        Args:
            chain_id: Context Chain 唯一标识。

        Returns:
            上下文链查询结果。
        """
        return self._query_service.get_context_chain(chain_id)


class ListContextChainsUseCase:
    """分页查询 Context Chain 列表的只读用例。"""

    def __init__(self, query_service: ContextQueryService) -> None:
        self._query_service = query_service

    def execute(
        self,
        query: ContextChainSearchQuery,
    ) -> ContextChainListResult:
        """执行分页查询 Context Chain 列表。

        Args:
            query: 查询筛选与分页参数。

        Returns:
            分页查询结果。
        """
        return self._query_service.list_context_chains(query)


class ListContextChainNodesUseCase:
    """分页查询 Context Chain Node 列表的只读用例。"""

    def __init__(self, query_service: ContextQueryService) -> None:
        self._query_service = query_service

    def execute(
        self,
        query: ContextChainNodeSearchQuery,
    ) -> ContextChainNodeListResult:
        """执行分页查询 Context Chain Node 列表。

        Args:
            query: 查询筛选与分页参数。

        Returns:
            分页查询结果。
        """
        return self._query_service.list_context_chain_nodes(query)


class ListContextChainResourcesUseCase:
    """分页查询 Context Chain Resource 列表的只读用例。"""

    def __init__(self, query_service: ContextQueryService) -> None:
        self._query_service = query_service

    def execute(
        self,
        query: ContextChainResourceSearchQuery,
    ) -> ContextChainResourceListResult:
        """执行分页查询 Context Chain Resource 列表。

        Args:
            query: 查询筛选与分页参数。

        Returns:
            分页查询结果。
        """
        return self._query_service.list_context_chain_resources(query)


class ListContextSelectionRecordsUseCase:
    """分页查询 Context SelectionRecord 决策记录列表的只读用例。"""

    def __init__(self, query_service: ContextQueryService) -> None:
        self._query_service = query_service

    def execute(
        self,
        query: ContextSelectionRecordSearchQuery,
    ) -> ContextSelectionRecordListResult:
        """执行分页查询 Context SelectionRecord 决策记录列表。

        Args:
            query: 查询筛选与分页参数。

        Returns:
            分页查询结果。
        """
        return self._query_service.list_context_selection_records(query)
