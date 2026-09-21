"""文档模块 HTTP 控制器层的请求与响应 Pydantic Schema 定义。

继承并重用 Application 层 DTO，为 Presentation 路由提供统一的 OpenAPI 输入输出契约定义。
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from app.modules.document.application.dto import (
    BuildChunksResult,
    ChildChunkSearchQuery,
    DocumentArtifactSearchQuery,
    DocumentChunkStatisticsResult,
    DocumentOverviewResult,
    DocumentPipelineStateResult,
    DocumentResult,
    DocumentSearchQuery,
    IndexVectorsResult,
    KnowledgeBaseStatisticsResult,
    ListChildChunksResult,
    ListDocumentArtifactsResult,
    ListParentBlocksResult,
    ParentBlockSearchQuery,
    ProcessDocumentResult,
    SearchDocumentArtifactsResult,
    SearchDocumentsResult,
)

RiskLevel = Literal["low", "medium", "high", "critical"]


class DocumentUploadFormData(BaseModel):
    """从 multipart/form-data 表单中提取解析出的文档业务元数据。"""

    title: str = Field(..., min_length=1, max_length=255, description="文档标题")
    kb_id: int = Field(..., gt=0, description="所属知识库 ID")
    domain_code: str = Field(..., min_length=1, max_length=255, description="业务领域编码")
    business_scene: str | None = Field(None, min_length=1, max_length=255, description="业务场景编码")
    risk_level: RiskLevel = Field("low", description="风险等级（low, medium, high, critical）")
    effective_at: datetime | None = Field(None, description="文档生效时间")
    expired_at: datetime | None = Field(None, description="文档过期时间")


class DocumentResponse(DocumentResult):
    """上传成功或详情查询返回的文档完整信息响应 Schema。"""


class DocumentProcessResponse(ProcessDocumentResult):
    """文档清洗或 Docling 转换完成后的状态响应 Schema。"""


class BuildChunksResponse(BuildChunksResult):
    """父子切块重建完成后的数量与状态响应 Schema。"""


class VectorIndexingResponse(IndexVectorsResult):
    """向量索引生成与 Qdrant 写入完成后的数量与状态响应 Schema。"""


class DocumentSearchRequest(DocumentSearchQuery):
    """文档高级检索 HTTP 请求体 Schema。"""


class DocumentSearchResponse(SearchDocumentsResult):
    """文档高级检索 HTTP 响应体 Schema。"""


class DocumentPipelineStateResponse(DocumentPipelineStateResult):
    """文档流水线三阶段（处理/切块/索引）状态查询响应 Schema。"""


class DocumentArtifactsResponse(ListDocumentArtifactsResult):
    """单文档关联派生产物列表响应 Schema。"""


class DocumentArtifactSearchRequest(DocumentArtifactSearchQuery):
    """派生产物高级检索 HTTP 请求体 Schema。"""


class DocumentArtifactSearchResponse(SearchDocumentArtifactsResult):
    """派生产物高级检索 HTTP 响应体 Schema。"""


class ParentBlockSearchRequest(ParentBlockSearchQuery):
    """父级语义块高级检索 HTTP 请求体 Schema。"""


class ParentBlockSearchResponse(ListParentBlocksResult):
    """父级语义块高级检索 HTTP 响应体 Schema。"""


class ChildChunkSearchRequest(ChildChunkSearchQuery):
    """可向量化子块高级检索 HTTP 请求体 Schema。"""


class ChildChunkSearchResponse(ListChildChunksResult):
    """可向量化子块高级检索 HTTP 响应体 Schema。"""


class DocumentChunkStatisticsResponse(DocumentChunkStatisticsResult):
    """文档父子块与向量化状态统计分布响应 Schema。"""


class KnowledgeBaseStatisticsResponse(KnowledgeBaseStatisticsResult):
    """知识库整体文档与切块宏观统计响应 Schema。"""


class DocumentOverviewResponse(DocumentOverviewResult):
    """单篇文档全景概览响应 Schema。"""

