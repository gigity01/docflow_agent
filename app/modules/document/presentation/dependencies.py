"""文档 Presentation 层的 FastAPI 依赖注入项。

提供：
1. DocumentOperationContext 链路与审计追踪上下文提取与响应 Header 注入。
2. multipart/form-data 表单参数提取与 Schema 组装。
3. 从全局 AppContainer 解析获取 13 个 Application UseCase 实例的依赖函数。
"""

from datetime import datetime
from typing import Literal

from fastapi import Depends, Form, Header, Response

from app.bootstrap.container import AppContainer
from app.bootstrap.dependencies import get_container
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
from app.modules.document.presentation.schemas import DocumentUploadFormData
from app.shared.observability.correlation import DocumentOperationContext


def get_document_operation_context(
    response: Response,
    workflow_id: str | None = Header(
        default=None,
        alias="X-Workflow-ID",
        min_length=1,
        max_length=200,
    ),
) -> DocumentOperationContext:
    """在 HTTP 请求边界创建或解析 DocumentOperationContext 操作上下文，并将关联 ID 注入响应头。

    Args:
        response: FastAPI 响应对象。
        workflow_id: 可选从请求头获取的链路工作流 ID。

    Returns:
        DocumentOperationContext: 初始化的文档操作上下文对象。
    """
    operation_context = DocumentOperationContext.create(workflow_id=workflow_id)
    response.headers["X-Workflow-ID"] = operation_context.workflow_id
    response.headers["X-Operation-ID"] = operation_context.operation_id
    response.headers["X-Operation-Attempt"] = str(operation_context.attempt)
    return operation_context


def document_upload_form(
    title: str = Form(..., min_length=1, max_length=255),
    kb_id: int = Form(..., gt=0),
    domain_code: str = Form(..., min_length=1, max_length=255),
    business_scene: str | None = Form(None, min_length=1, max_length=255),
    risk_level: Literal["low", "medium", "high", "critical"] = Form("low"),
    effective_at: datetime | None = Form(None),
    expired_at: datetime | None = Form(None),
) -> DocumentUploadFormData:
    """从 multipart/form-data 表单中解析并组装上传业务元数据 Schema。

    Args:
        title: 文档标题。
        kb_id: 目标知识库 ID。
        domain_code: 业务领域编码。
        business_scene: 业务场景编码。
        risk_level: 风险等级。
        effective_at: 生效时间。
        expired_at: 过期时间。

    Returns:
        DocumentUploadFormData: 组装好的上传表单数据对象。
    """
    return DocumentUploadFormData(
        title=title,
        kb_id=kb_id,
        domain_code=domain_code,
        business_scene=business_scene,
        risk_level=risk_level,
        effective_at=effective_at,
        expired_at=expired_at,
    )


def get_upload_document_use_case(
    container: AppContainer = Depends(get_container),
) -> UploadDocumentUseCase:
    """依赖注入获取文档上传用例。"""
    return container.upload_document


def get_process_document_use_case(
    container: AppContainer = Depends(get_container),
) -> ProcessDocumentUseCase:
    """依赖注入获取文档清洗转换用例。"""
    return container.process_document


def get_build_chunks_use_case(
    container: AppContainer = Depends(get_container),
) -> BuildChunksUseCase:
    """依赖注入获取父子切块构建用例。"""
    return container.build_chunks


def get_index_vectors_use_case(
    container: AppContainer = Depends(get_container),
) -> IndexVectorsUseCase:
    """依赖注入获取向量索引生成用例。"""
    return container.index_vectors


def get_document_use_case(
    container: AppContainer = Depends(get_container),
) -> GetDocumentUseCase:
    """依赖注入获取单文档详情查询用例。"""
    return container.get_document


def get_search_documents_use_case(
    container: AppContainer = Depends(get_container),
) -> SearchDocumentsUseCase:
    """依赖注入获取文档高级多维检索用例。"""
    return container.search_documents


def get_document_pipeline_state_use_case(
    container: AppContainer = Depends(get_container),
) -> GetDocumentPipelineStateUseCase:
    """依赖注入获取文档流水线三阶段状态查询用例。"""
    return container.get_document_pipeline_state


def get_list_document_artifacts_use_case(
    container: AppContainer = Depends(get_container),
) -> ListDocumentArtifactsUseCase:
    """依赖注入获取单文档派生产物列表查询用例。"""
    return container.list_document_artifacts


def get_search_document_artifacts_use_case(
    container: AppContainer = Depends(get_container),
) -> SearchDocumentArtifactsUseCase:
    """依赖注入获取派生产物高级检索用例。"""
    return container.search_document_artifacts


def get_list_parent_blocks_use_case(
    container: AppContainer = Depends(get_container),
) -> ListParentBlocksUseCase:
    """依赖注入获取父级语义块高级检索用例。"""
    return container.list_parent_blocks


def get_list_child_chunks_use_case(
    container: AppContainer = Depends(get_container),
) -> ListChildChunksUseCase:
    """依赖注入获取可向量化子块高级检索用例。"""
    return container.list_child_chunks


def get_document_chunk_statistics_use_case(
    container: AppContainer = Depends(get_container),
) -> GetDocumentChunkStatisticsUseCase:
    """依赖注入获取文档切块统计查询用例。"""
    return container.get_document_chunk_statistics


def get_knowledge_base_statistics_use_case(
    container: AppContainer = Depends(get_container),
) -> GetKnowledgeBaseStatisticsUseCase:
    """依赖注入获取知识库整体统计查询用例。"""
    return container.get_knowledge_base_statistics


def get_document_overview_use_case(
    container: AppContainer = Depends(get_container),
) -> GetDocumentOverviewUseCase:
    """依赖注入获取文档全景概览查询用例。"""
    return container.get_document_overview

