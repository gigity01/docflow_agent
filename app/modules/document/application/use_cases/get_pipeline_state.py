"""读取文档处理、切块和向量索引流水线进度的查询用例。

综合汇总单份文档的三状态轴（技术处理状态、业务生命周期状态、底层存储状态）
以及父级语义块数量、子块向量化状态统计分布与索引完成时间。
"""

from collections.abc import Callable
from typing import Any

from app.modules.document.application.dto import DocumentPipelineStateResult
from app.modules.document.application.errors import DocumentApplicationError


class GetDocumentPipelineStateUseCase:
    """汇总文档流水线技术状态、业务状态、存储状态以及父子块切块/向量化进度的只读用例。"""

    def __init__(self, *, uow_factory: Callable[[], Any]) -> None:
        """初始化流水线状态查询用例。

        Args:
            uow_factory: 数据库工作单元工厂。
        """
        self._uow_factory = uow_factory

    def execute(self, document_id: int) -> DocumentPipelineStateResult:
        """执行查询并返回文档处理流水线状态快照。

        Args:
            document_id: 目标文档 ID。

        Returns:
            文档流水线综合状态结果 DTO。

        Raises:
            DocumentApplicationError: 文档不存在时抛出 404。
        """
        with self._uow_factory() as uow:
            document = uow.documents.get_by_id(document_id)
            if document is None:
                raise DocumentApplicationError(404, "文档不存在")

            # 统计子块向量状态分布
            vector_status_counts = (
                uow.child_chunks.count_by_vector_status_for_document(
                    document_id
                )
            )
            return DocumentPipelineStateResult(
                document_id=document.id,
                doc_code=document.doc_code,
                source_type=document.source_type,
                source_uri=document.source_uri,
                cleaned_uri=document.cleaned_uri,
                document_status=document.status,
                lifecycle_status=document.lifecycle_status,
                storage_status=document.storage_status,
                parent_count=(
                    uow.parent_blocks.count_active_by_doc_id(document_id)
                ),
                child_count=sum(vector_status_counts.values()),
                vector_status_counts=vector_status_counts,
                indexed_at=document.indexed_at,
            )
