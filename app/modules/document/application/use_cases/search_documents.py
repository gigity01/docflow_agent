"""文档多条件高级检索用例。

支持面向 Agent 及管理端的多知识库、多状态轴、时间范围、关键字模糊匹配、排序与分页的复合查询。
"""

from collections.abc import Callable
from typing import Any

from app.modules.document.application.dto import (
    DocumentListItem,
    DocumentSearchQuery,
    SearchDocumentsResult,
)


class SearchDocumentsUseCase:
    """使用受限白名单字段过滤条件执行多维文档查询并返回稳定分页结果的用例。"""

    def __init__(self, *, uow_factory: Callable[[], Any]) -> None:
        """初始化文档高级检索用例。

        Args:
            uow_factory: 数据库工作单元工厂。
        """
        self._uow_factory = uow_factory

    def execute(self, query: DocumentSearchQuery) -> SearchDocumentsResult:
        """根据高级多条件查询参数检索文档。

        Args:
            query: 包含知识库 ID、状态轴、时间范围、关键字、排序与分页等参数的 DTO。

        Returns:
            包含文档摘要列表与总命中数的分页结果 DTO。
        """
        with self._uow_factory() as uow:
            documents = uow.documents.search(query)
            return SearchDocumentsResult(
                items=[
                    DocumentListItem.model_validate(document)
                    for document in documents
                ],
                total=uow.documents.count_search(query),
                limit=query.limit,
                offset=query.offset,
            )
