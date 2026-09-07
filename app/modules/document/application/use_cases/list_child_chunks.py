"""可向量化子块（Child Chunk）条件过滤与分页查询用例。

支持按文档 ID、父块 ID、知识库 ID、向量化状态（pending/indexing/indexed/failed）、
活跃状态、章节路径、表格行号及正文关键字进行综合检索。
"""

from collections.abc import Callable
from typing import Any

from app.modules.document.application.dto import (
    ChildChunkResult,
    ChildChunkSearchQuery,
    ListChildChunksResult,
)


class ListChildChunksUseCase:
    """根据查询条件检索子切块列表并返回分页结果的用例。"""

    def __init__(self, *, uow_factory: Callable[[], Any]) -> None:
        """初始化子切块列表查询用例。

        Args:
            uow_factory: 数据库工作单元工厂。
        """
        self._uow_factory = uow_factory

    def execute(self, query: ChildChunkSearchQuery) -> ListChildChunksResult:
        """执行子块条件检索。

        Args:
            query: 子块查询过滤条件 DTO。

        Returns:
            包含子块列表与分页元数据的 DTO。
        """
        with self._uow_factory() as uow:
            chunks = uow.child_chunks.search(query)
            return ListChildChunksResult(
                items=[
                    ChildChunkResult.model_validate(chunk)
                    for chunk in chunks
                ],
                total=uow.child_chunks.count_search(query),
                limit=query.limit,
                offset=query.offset,
            )
