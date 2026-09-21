"""单篇文档全景概览查询用例。

复用现有底层仓储查询能力，在单次工作单元（Unit of Work）内聚合获取文档核心信息、流水线状态快照与切块统计指标，
并对内部流转状态（processing, chunking, indexing）对外统一收敛为 in_progress。
"""

from collections.abc import Callable
from typing import Any

from app.modules.document.application.dto import (
    DocumentChunkStatisticsResult,
    DocumentOverviewResult,
    DocumentPipelineStateResult,
    DocumentResult,
)
from app.modules.document.application.errors import DocumentApplicationError
from app.modules.document.domain.enums import DocumentStatus


IN_PROGRESS_STATUSES = {
    DocumentStatus.PROCESSING.value,
    DocumentStatus.CHUNKING.value,
    DocumentStatus.INDEXING.value,
}


class GetDocumentOverviewUseCase:
    """按主键 ID 获取文档全景概览视图的只读用例。"""

    def __init__(self, *, uow_factory: Callable[[], Any]) -> None:
        """初始化文档全景概览查询用例。

        Args:
            uow_factory: 数据库工作单元工厂。
        """
        self._uow_factory = uow_factory

    def execute(self, document_id: int) -> DocumentOverviewResult:
        """执行查询并返回聚合了文档信息、流水线状态快照与切块统计的全景概览 DTO。

        Args:
            document_id: 待查询的目标文档 ID。

        Returns:
            文档全景概览结果 DTO。

        Raises:
            DocumentApplicationError: 当文档不存在时抛出 404 异常。
        """
        with self._uow_factory() as uow:
            document = uow.documents.get_by_id(document_id)
            if document is None:
                raise DocumentApplicationError(404, "文档不存在")

            raw_doc_status = document.status
            is_in_progress = raw_doc_status in IN_PROGRESS_STATUSES
            exposed_status = "in_progress" if is_in_progress else raw_doc_status

            # 1. 组装 Document 核心视图，流转中状态收敛为 in_progress
            doc_result = DocumentResult.model_validate(document)
            if is_in_progress:
                doc_result = doc_result.model_copy(update={"status": "in_progress"})

            # 2. 统计子块向量状态分布与父块活跃数，组装流水线状态视图
            vector_status_counts = (
                uow.child_chunks.count_by_vector_status_for_document(
                    document_id
                )
            )
            parent_count_active = uow.parent_blocks.count_active_by_doc_id(
                document_id
            )
            pipeline_state = DocumentPipelineStateResult(
                document_id=document.id,
                doc_code=document.doc_code,
                source_type=document.source_type,
                source_uri=document.source_uri,
                cleaned_uri=document.cleaned_uri,
                document_status=exposed_status,
                lifecycle_status=document.lifecycle_status,
                storage_status=document.storage_status,
                parent_count=parent_count_active,
                child_count=sum(vector_status_counts.values()),
                vector_status_counts=vector_status_counts,
                indexed_at=document.indexed_at,
            )

            # 3. 统计父子块全量状态与向量分布指标，组装切块统计视图
            parent_status_counts = (
                uow.parent_blocks.count_by_status_for_document(document_id)
            )
            child_status_counts = (
                uow.child_chunks.count_by_status_for_document(document_id)
            )
            all_vector_status_counts = (
                uow.child_chunks.count_all_by_vector_status_for_document(
                    document_id
                )
            )
            chunks_with_vector_id, chunks_without_vector_id = (
                uow.child_chunks.count_vector_id_presence_for_document(
                    document_id
                )
            )
            chunk_type_counts = (
                uow.child_chunks.count_by_chunk_type_for_document(
                    document_id
                )
            )
            chunk_statistics = DocumentChunkStatisticsResult(
                document_id=document.id,
                doc_code=document.doc_code,
                parent_count=sum(parent_status_counts.values()),
                child_count=sum(child_status_counts.values()),
                parent_status_counts=parent_status_counts,
                child_status_counts=child_status_counts,
                vector_status_counts=all_vector_status_counts,
                chunk_type_counts=chunk_type_counts,
                chunks_with_vector_id=chunks_with_vector_id,
                chunks_without_vector_id=chunks_without_vector_id,
            )

            return DocumentOverviewResult(
                document=doc_result,
                pipeline_state=pipeline_state,
                chunk_statistics=chunk_statistics,
            )
