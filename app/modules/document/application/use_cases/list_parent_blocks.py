"""父级语义块（Parent Block）条件过滤与分页查询用例。

支持按文档 ID 列表、父块 ID 列表、知识库 ID 列表、块类型（如 section/paragraph_group）、
状态、章节路径模糊匹配及正文关键字进行综合检索。
"""

from collections.abc import Callable
from typing import Any

from app.modules.document.application.dto import (
    ListParentBlocksResult,
    ParentBlockResult,
    ParentBlockSearchQuery,
)


class ListParentBlocksUseCase:
    """根据查询条件检索父级语义块列表并返回分页结果的用例。"""

    def __init__(self, *, uow_factory: Callable[[], Any]) -> None:
        """初始化父语义块列表查询用例。

        Args:
            uow_factory: 数据库工作单元工厂。
        """
        self._uow_factory = uow_factory

    def execute(self, query: ParentBlockSearchQuery) -> ListParentBlocksResult:
        """执行父块条件检索。

        Args:
            query: 父块查询过滤条件 DTO。

        Returns:
            包含父块列表与分页元数据的 DTO。
        """
        with self._uow_factory() as uow:
            blocks = uow.parent_blocks.search(query)
            return ListParentBlocksResult(
                items=[
                    ParentBlockResult.model_validate(block)
                    for block in blocks
                ],
                total=uow.parent_blocks.count_search(query),
                limit=query.limit,
                offset=query.offset,
            )
