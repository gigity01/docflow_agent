"""文档模块 HTTP 路由注册与控制器实现。

包含：
- `/admin/documents`: 文档上传、详情、检索、流水线状态及四阶段处理命令（upload, process, build-chunks, index-vectors）。
- `/admin/document-artifacts`: 派生产物检索接口。
- `/admin/parent-blocks`: 父级语义块检索接口。
- `/admin/child-chunks`: 可向量化子切块检索接口。
- `/admin/knowledge-bases`: 知识库宏观统计接口。
"""

from typing import Annotated

from fastapi import APIRouter, Depends, File, Path, UploadFile

from app.modules.document.application.use_cases.build_chunks import (
    BuildChunksUseCase,
)
from app.modules.document.application.use_cases.get_chunk_statistics import (
    GetDocumentChunkStatisticsUseCase,
)
from app.modules.document.application.use_cases.get_document import (
    GetDocumentUseCase,
)
from app.modules.document.application.use_cases.get_document_overview import (
    GetDocumentOverviewUseCase,
)
from app.modules.document.application.use_cases.get_knowledge_base_statistics import (
    GetKnowledgeBaseStatisticsUseCase,
)
from app.modules.document.application.use_cases.get_pipeline_state import (
    GetDocumentPipelineStateUseCase,
)
from app.modules.document.application.use_cases.index_vectors import (
    IndexVectorsUseCase,
)
from app.modules.document.application.use_cases.list_artifacts import (
    ListDocumentArtifactsUseCase,
)
from app.modules.document.application.use_cases.list_child_chunks import (
    ListChildChunksUseCase,
)
from app.modules.document.application.use_cases.list_parent_blocks import (
    ListParentBlocksUseCase,
)
from app.modules.document.application.use_cases.process_document import (
    ProcessDocumentUseCase,
)
from app.modules.document.application.use_cases.search_artifacts import (
    SearchDocumentArtifactsUseCase,
)
from app.modules.document.application.use_cases.search_documents import (
    SearchDocumentsUseCase,
)
from app.modules.document.application.use_cases.upload_document import (
    UploadDocumentUseCase,
)
from app.modules.document.presentation.dependencies import (
    document_upload_form,
    get_build_chunks_use_case,
    get_document_chunk_statistics_use_case,
    get_document_operation_context,
    get_document_overview_use_case,
    get_document_pipeline_state_use_case,
    get_document_use_case,
    get_index_vectors_use_case,
    get_knowledge_base_statistics_use_case,
    get_list_child_chunks_use_case,
    get_list_document_artifacts_use_case,
    get_list_parent_blocks_use_case,
    get_process_document_use_case,
    get_search_document_artifacts_use_case,
    get_search_documents_use_case,
    get_upload_document_use_case,
)
from app.modules.document.presentation.schemas import (
    BuildChunksResponse,
    ChildChunkSearchRequest,
    ChildChunkSearchResponse,
    DocumentArtifactSearchRequest,
    DocumentArtifactSearchResponse,
    DocumentArtifactsResponse,
    DocumentChunkStatisticsResponse,
    DocumentOverviewResponse,
    DocumentPipelineStateResponse,
    DocumentProcessResponse,
    DocumentResponse,
    DocumentSearchRequest,
    DocumentSearchResponse,
    DocumentUploadFormData,
    KnowledgeBaseStatisticsResponse,
    ParentBlockSearchRequest,
    ParentBlockSearchResponse,
    VectorIndexingResponse,
)
from app.shared.observability.correlation import DocumentOperationContext

router = APIRouter(prefix="/admin/documents", tags=["documents"])
artifact_router = APIRouter(
    prefix="/admin/document-artifacts",
    tags=["document-artifacts"],
)
parent_block_router = APIRouter(
    prefix="/admin/parent-blocks",
    tags=["parent-blocks"],
)
child_chunk_router = APIRouter(
    prefix="/admin/child-chunks",
    tags=["child-chunks"],
)
knowledge_base_router = APIRouter(
    prefix="/admin/knowledge-bases",
    tags=["knowledge-bases"],
)

PositiveId = Annotated[int, Path(gt=0)]


@router.post(
    "/upload",
    response_model=DocumentResponse,
)
async def upload_document(
    file: UploadFile = File(...),
    meta: DocumentUploadFormData = Depends(document_upload_form),
    use_case: UploadDocumentUseCase = Depends(
        get_upload_document_use_case
    ),
    operation_context: DocumentOperationContext = Depends(
        get_document_operation_context
    ),
):
    """阶段 1：接收原始文件并创建处于 uploaded 状态的文档记录。"""
    return await use_case.execute(
        file=file,
        meta=meta,
        operation_context=operation_context,
    )


@router.get(
    "/{document_id}",
    response_model=DocumentResponse,
)
def get_document(
    document_id: PositiveId,
    use_case: GetDocumentUseCase = Depends(get_document_use_case),
):
    """读取一份文档的完整元数据和当前状态。"""
    return use_case.execute(document_id)


@router.post(
    "/search",
    response_model=DocumentSearchResponse,
)
def search_documents(
    request: DocumentSearchRequest,
    use_case: SearchDocumentsUseCase = Depends(
        get_search_documents_use_case
    ),
):
    """按受限业务字段、状态轴和时间范围高级检索文档。"""
    return use_case.execute(request)


@router.get(
    "/{document_id}/pipeline-state",
    response_model=DocumentPipelineStateResponse,
)
def get_document_pipeline_state(
    document_id: PositiveId,
    use_case: GetDocumentPipelineStateUseCase = Depends(
        get_document_pipeline_state_use_case
    ),
):
    """读取文档流水线三阶段状态（处理/切块/向量索引）。"""
    return use_case.execute(document_id)


@router.get(
    "/{document_id}/artifacts",
    response_model=DocumentArtifactsResponse,
)
def list_document_artifacts(
    document_id: PositiveId,
    use_case: ListDocumentArtifactsUseCase = Depends(
        get_list_document_artifacts_use_case
    ),
):
    """列出指定文档的全部派生产物。"""
    return use_case.execute(document_id)


@router.get(
    "/{document_id}/chunk-statistics",
    response_model=DocumentChunkStatisticsResponse,
)
def get_document_chunk_statistics(
    document_id: PositiveId,
    use_case: GetDocumentChunkStatisticsUseCase = Depends(
        get_document_chunk_statistics_use_case
    ),
):
    """读取文档父块、子块和向量状态统计指标。"""
    return use_case.execute(document_id)


@router.get(
    "/{document_id}/overview",
    response_model=DocumentOverviewResponse,
)
def get_document_overview(
    document_id: PositiveId,
    use_case: GetDocumentOverviewUseCase = Depends(
        get_document_overview_use_case
    ),
):
    """读取文档全景概览（聚合元数据、流水线状态快照与切块统计）。"""
    return use_case.execute(document_id)



@router.post(
    "/{document_id}/process",
    response_model=DocumentProcessResponse,
)
def trigger_document_processing(
    document_id: int,
    use_case: ProcessDocumentUseCase = Depends(
        get_process_document_use_case
    ),
    operation_context: DocumentOperationContext = Depends(
        get_document_operation_context
    ),
):
    """阶段 2：触发指定文档的清洗或外部格式转换流程（推进至 processed）。"""
    return use_case.execute(
        document_id,
        operation_context=operation_context,
    )


@router.post(
    "/{document_id}/build-chunks",
    response_model=BuildChunksResponse,
)
def trigger_build_chunks(
    document_id: int,
    use_case: BuildChunksUseCase = Depends(get_build_chunks_use_case),
    operation_context: DocumentOperationContext = Depends(
        get_document_operation_context
    ),
):
    """阶段 3：基于已清洗的文本重建父块和子块（推进至 chunked）。"""
    return use_case.execute(
        document_id,
        operation_context=operation_context,
    )


@router.post(
    "/{document_id}/index-vectors",
    response_model=VectorIndexingResponse,
)
def trigger_vector_indexing(
    document_id: int,
    use_case: IndexVectorsUseCase = Depends(get_index_vectors_use_case),
    operation_context: DocumentOperationContext = Depends(
        get_document_operation_context
    ),
):
    """阶段 4：为尚未索引的子块生成向量并写入 Qdrant 向量库（推进至 indexed）。"""
    return use_case.execute(
        document_id,
        operation_context=operation_context,
    )


@artifact_router.post(
    "/search",
    response_model=DocumentArtifactSearchResponse,
)
def search_document_artifacts(
    request: DocumentArtifactSearchRequest,
    use_case: SearchDocumentArtifactsUseCase = Depends(
        get_search_document_artifacts_use_case
    ),
):
    """按产物类型、角色、状态和时间范围高级检索派生产物。"""
    return use_case.execute(request)


@parent_block_router.post(
    "/search",
    response_model=ParentBlockSearchResponse,
)
def search_parent_blocks(
    request: ParentBlockSearchRequest,
    use_case: ListParentBlocksUseCase = Depends(
        get_list_parent_blocks_use_case
    ),
):
    """按文档、知识库、章节路径和关键词高级检索父级语义块。"""
    return use_case.execute(request)


@child_chunk_router.post(
    "/search",
    response_model=ChildChunkSearchResponse,
)
def search_child_chunks(
    request: ChildChunkSearchRequest,
    use_case: ListChildChunksUseCase = Depends(
        get_list_child_chunks_use_case
    ),
):
    """按向量状态、章节路径和 CSV 行范围高级检索可向量化子块。"""
    return use_case.execute(request)


@knowledge_base_router.get(
    "/{kb_id}/statistics",
    response_model=KnowledgeBaseStatisticsResponse,
)
def get_knowledge_base_statistics(
    kb_id: PositiveId,
    use_case: GetKnowledgeBaseStatisticsUseCase = Depends(
        get_knowledge_base_statistics_use_case
    ),
):
    """读取知识库文档、父子块和向量状态宏观统计数据。"""
    return use_case.execute(kb_id)
