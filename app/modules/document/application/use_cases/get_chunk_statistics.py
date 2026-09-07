"""文档父子块与向量状态统计查询用例。

提供单份文档内父块数量、子块数量、父子块状态分布、子块类型分布以及 Qdrant Point ID 关联情况的只读聚合统计。
"""

from collections.abc import Callable
from typing import Any

from app.modules.document.application.dto import DocumentChunkStatisticsResult
from app.modules.document.application.errors import DocumentApplicationError


class GetDocumentChunkStatisticsUseCase:
    """统计指定文档的父块、子块类型与向量状态分布的只读用例。"""

    def __init__(self, *, uow_factory: Callable[[], Any]) -> None:
        """初始化切块统计用例。

        Args:
            uow_factory: 数据库工作单元工厂。
        """
        self._uow_factory = uow_factory

    def execute(self, document_id: int) -> DocumentChunkStatisticsResult:
        """执行统计并返回文档切块与向量状态分布详情。

        Args:
            document_id: 目标文档 ID。

        Returns:
            统计结果 DTO。

        Raises:
            DocumentApplicationError: 文档不存在时抛出 404。
        """
        with self._uow_factory() as uow:
            document = uow.documents.get_by_id(document_id)
            if document is None:
                raise DocumentApplicationError(404, "文档不存在")

            # 统计父块状态分布
            parent_status_counts = (
                uow.parent_blocks.count_by_status_for_document(document_id)
            )
            # 统计子块状态分布
            child_status_counts = (
                uow.child_chunks.count_by_status_for_document(document_id)
            )
            # 统计子块向量化状态（pending / indexing / indexed / failed）分布
            vector_status_counts = (
                uow.child_chunks.count_all_by_vector_status_for_document(
                    document_id
                )
            )
            # 统计已关联 Qdrant Point ID 与未关联的子块数
            chunks_with_vector_id, chunks_without_vector_id = (
                uow.child_chunks.count_vector_id_presence_for_document(
                    document_id
                )
            )
            return DocumentChunkStatisticsResult(
                document_id=document.id,
                doc_code=document.doc_code,
                parent_count=sum(parent_status_counts.values()),
                child_count=sum(child_status_counts.values()),
                parent_status_counts=parent_status_counts,
                child_status_counts=child_status_counts,
                vector_status_counts=vector_status_counts,
                chunk_type_counts=(
                    uow.child_chunks.count_by_chunk_type_for_document(
                        document_id
                    )
                ),
                chunks_with_vector_id=chunks_with_vector_id,
                chunks_without_vector_id=chunks_without_vector_id,
            )
