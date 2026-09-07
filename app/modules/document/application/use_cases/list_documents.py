"""按知识库与基本状态筛选文档列表的查询用例。

支持按知识库 ID、流水线处理状态、源文件格式与业务生命周期状态进行过滤，
返回轻量级稳定分页的文档摘要列表。
"""

from collections.abc import Callable
from typing import Any

from app.modules.document.application.dto import (
    DocumentListItem,
    DocumentListQuery,
    ListDocumentsResult,
)


class ListDocumentsUseCase:
    """按知识库与状态返回稳定分页文档摘要列表的用例。"""

    def __init__(self, *, uow_factory: Callable[[], Any]) -> None:
        """初始化文档列表查询用例。

        Args:
            uow_factory: 数据库工作单元工厂。
        """
        self._uow_factory = uow_factory

    def execute(self, query: DocumentListQuery) -> ListDocumentsResult:
        """根据基础查询条件获取文档分页摘要列表。

        Args:
            query: 包含 kb_id、status、source_type、lifecycle_status 及分页限制的查询 DTO。

        Returns:
            包含文档摘要项与总记录数的分页结果 DTO。
        """
        with self._uow_factory() as uow:
            documents = uow.documents.list_filtered(
                kb_id=query.kb_id,
                status=query.status,
                source_type=query.source_type,
                lifecycle_status=query.lifecycle_status,
                limit=query.limit,
                offset=query.offset,
            )
            total = uow.documents.count_filtered(
                kb_id=query.kb_id,
                status=query.status,
                source_type=query.source_type,
                lifecycle_status=query.lifecycle_status,
            )
            return ListDocumentsResult(
                items=[
                    DocumentListItem.model_validate(document)
                    for document in documents
                ],
                total=total,
                limit=query.limit,
                offset=query.offset,
            )
