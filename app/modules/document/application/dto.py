"""文档应用层数据传输对象（DTO）定义。

包含用例输入命令、查询参数、执行结果以及用于 API/Agent 交互的数据结构。
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, model_validator


class DocumentResult(BaseModel):
    """上传、查询或生命周期状态变更操作返回的完整文档详情视图。"""

    id: int
    doc_code: str
    kb_id: int
    domain_code: str
    business_scene: str | None
    title: str
    original_filename: str | None
    file_size: int | None
    source_type: str
    source_uri: str
    cleaned_uri: str | None
    content_hash: str
    active_content_hash: str | None
    lifecycle_status: str
    storage_status: str
    version: int
    status: str
    replaced_by: int | None
    risk_level: str | None
    effective_at: datetime | None
    expired_at: datetime | None
    created_by_actor_code: str | None
    created_at: datetime
    updated_at: datetime
    indexed_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class ProcessDocumentResult(BaseModel):
    """文档文本清洗与格式转换（Process）用例的执行结果。"""

    document_id: int
    doc_code: str
    source_type: str
    source_uri: str
    cleaned_uri: str
    status: str


class BuildChunksResult(BaseModel):
    """文档切块（Build Chunks）用例的执行结果。"""

    document_id: int
    doc_code: str
    source_type: str
    parent_count: int
    child_count: int
    status: str


class IndexVectorsResult(BaseModel):
    """文档向量生成与 Qdrant 写入（Index Vectors）用例的执行结果。"""

    document_id: int
    total_chunks: int
    indexed_chunks: int
    failed_chunks: int
    status: str


class DocumentListQuery(BaseModel):
    """基础文档列表查询条件参数。"""

    kb_id: int = Field(..., gt=0, description="归属知识库 ID")
    status: str | None = Field(default=None, description="流水线技术状态过滤")
    source_type: str | None = Field(default=None, description="源文件格式过滤")
    lifecycle_status: str | None = Field(default=None, description="业务生命周期状态过滤")
    limit: int = Field(default=50, ge=1, le=100, description="每页条数限制")
    offset: int = Field(default=0, ge=0, description="分页偏移量")


class DocumentListItem(BaseModel):
    """文档列表查询中的单条文档摘要视图。"""

    id: int
    doc_code: str
    kb_id: int
    title: str
    source_type: str
    lifecycle_status: str
    storage_status: str
    status: str
    version: int
    created_at: datetime
    updated_at: datetime
    indexed_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class ListDocumentsResult(BaseModel):
    """基础文档列表查询的稳定分页结果。"""

    items: list[DocumentListItem]
    total: int
    limit: int
    offset: int


class DocumentSearchQuery(BaseModel):
    """Agent 与管理端文档高级多条件检索查询参数。

    支持多知识库、多状态、多标签、时间范围与关键字综合过滤。
    """

    kb_ids: list[PositiveInt] = Field(default_factory=list, description="知识库 ID 列表")
    document_ids: list[PositiveInt] = Field(default_factory=list, description="文档 ID 列表")
    doc_codes: list[str] = Field(default_factory=list, description="文档业务编码列表")
    domain_codes: list[str] = Field(default_factory=list, description="业务领域编码列表")
    business_scenes: list[str] = Field(default_factory=list, description="业务场景列表")
    statuses: list[str] = Field(default_factory=list, description="流水线状态列表")
    lifecycle_statuses: list[str] = Field(default_factory=list, description="生命周期状态列表")
    storage_statuses: list[str] = Field(default_factory=list, description="存储状态列表")
    source_types: list[str] = Field(default_factory=list, description="文件类型列表")
    risk_levels: list[str] = Field(default_factory=list, description="风险等级列表")
    keyword: str | None = Field(default=None, description="标题或关键字模糊搜索")
    original_filename: str | None = Field(default=None, description="原始文件名过滤")
    created_by_actor_code: str | None = Field(default=None, description="创建人编码")
    created_from: datetime | None = Field(default=None, description="创建时间起")
    created_to: datetime | None = Field(default=None, description="创建时间止")
    updated_from: datetime | None = Field(default=None, description="更新时间起")
    updated_to: datetime | None = Field(default=None, description="更新时间止")
    indexed_from: datetime | None = Field(default=None, description="索引时间起")
    indexed_to: datetime | None = Field(default=None, description="索引时间止")
    effective_at_before: datetime | None = Field(default=None, description="生效时间早于")
    expired_at_before: datetime | None = Field(default=None, description="过期时间早于")
    has_cleaned_output: bool | None = Field(default=None, description="是否已有清洗产物")
    has_active_content_hash: bool | None = Field(default=None, description="是否已有激活内容哈希")
    replaced_by: int | None = Field(default=None, gt=0, description="被替换的文档 ID")
    sort_by: Literal[
        "id",
        "created_at",
        "updated_at",
        "indexed_at",
        "title",
    ] = Field(default="id", description="排序字段")
    sort_order: Literal["asc", "desc"] = Field(default="desc", description="排序方向")
    limit: int = Field(default=50, ge=1, le=100, description="返回条数限制")
    offset: int = Field(default=0, ge=0, description="偏移量")

    @model_validator(mode="after")
    def validate_time_ranges(self) -> "DocumentSearchQuery":
        """校验起止时间范围的合法性，防止起始时间晚于截止时间。"""
        for start_name, end_name in (
            ("created_from", "created_to"),
            ("updated_from", "updated_to"),
            ("indexed_from", "indexed_to"),
        ):
            start = getattr(self, start_name)
            end = getattr(self, end_name)
            if start is not None and end is not None and start > end:
                raise ValueError(f"{start_name} 不能晚于 {end_name}")
        return self


class SearchDocumentsResult(ListDocumentsResult):
    """文档高级多条件查询的分页返回结果。"""


class DocumentPipelineStateResult(BaseModel):
    """文档三状态轴及切块、向量索引进度的综合只读快照。"""

    document_id: int
    doc_code: str
    source_type: str
    source_uri: str
    cleaned_uri: str | None
    document_status: str
    lifecycle_status: str
    storage_status: str
    parent_count: int
    child_count: int
    vector_status_counts: dict[str, int]
    indexed_at: datetime | None


class DocumentArtifactResult(BaseModel):
    """文档派生产物（Artifact）详情视图。"""

    id: int
    document_id: int
    artifact_code: str
    artifact_type: str
    artifact_role: str
    artifact_format: str
    artifact_uri: str
    artifact_hash: str | None
    hash_algorithm: str | None
    provider: str | None
    processor: str | None
    file_size: int | None
    char_count: int | None
    line_count: int | None
    status: str
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        validation_alias="metadata_json",
    )
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class ListDocumentArtifactsResult(BaseModel):
    """指定文档关联的全部派生产物列表视图。"""

    document_id: int
    source_uri: str
    source_type: str
    original_filename: str | None
    items: list[DocumentArtifactResult]


class DocumentArtifactSearchQuery(BaseModel):
    """派生产物多条件检索查询参数。"""

    document_ids: list[PositiveInt] = Field(default_factory=list, description="文档 ID 列表")
    artifact_types: list[str] = Field(default_factory=list, description="产物类型列表")
    artifact_roles: list[str] = Field(default_factory=list, description="产物角色列表")
    artifact_formats: list[str] = Field(default_factory=list, description="产物格式列表")
    statuses: list[str] = Field(default_factory=list, description="产物状态列表")
    providers: list[str] = Field(default_factory=list, description="提供方列表")
    processors: list[str] = Field(default_factory=list, description="处理程序列表")
    created_from: datetime | None = Field(default=None, description="创建时间起")
    created_to: datetime | None = Field(default=None, description="创建时间止")
    active_only: bool | None = Field(default=None, description="是否仅查询 active 状态产物")
    limit: int = Field(default=50, ge=1, le=100, description="条数限制")
    offset: int = Field(default=0, ge=0, description="偏移量")

    @model_validator(mode="after")
    def validate_created_range(self) -> "DocumentArtifactSearchQuery":
        """校验产物创建起止时间范围。"""
        if (
            self.created_from is not None
            and self.created_to is not None
            and self.created_from > self.created_to
        ):
            raise ValueError("created_from 不能晚于 created_to")
        return self


class SearchDocumentArtifactsResult(BaseModel):
    """派生产物多条件检索的分页返回结果。"""

    items: list[DocumentArtifactResult]
    total: int
    limit: int
    offset: int


class ParentBlockSearchQuery(BaseModel):
    """父级语义块（Parent Block）检索查询参数。"""

    document_ids: list[PositiveInt] = Field(default_factory=list, description="所属文档 ID 列表")
    parent_ids: list[PositiveInt] = Field(default_factory=list, description="父块 ID 列表")
    kb_ids: list[PositiveInt] = Field(default_factory=list, description="知识库 ID 列表")
    block_types: list[str] = Field(default_factory=list, description="父块类型列表")
    statuses: list[str] = Field(default_factory=list, description="状态列表")
    section_path_contains: str | None = Field(default=None, description="章节路径包含关键字")
    keyword: str | None = Field(default=None, description="父块正文关键字搜索")
    limit: int = Field(default=50, ge=1, le=100, description="返回条数")
    offset: int = Field(default=0, ge=0, description="偏移量")


class ParentBlockResult(BaseModel):
    """父级语义块实体详情视图。"""

    id: int
    parent_code: str
    kb_id: int
    doc_id: int
    domain_code: str
    business_scene: str | None
    block_type: str
    title: str | None
    section_path: list[str] | None
    content: str
    content_hash: str | None
    block_index: int
    semantic_group_index: int
    segment_index: int
    status: str
    version: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ListParentBlocksResult(BaseModel):
    """父级语义块检索的分页返回结果。"""

    items: list[ParentBlockResult]
    total: int
    limit: int
    offset: int


class ChildChunkSearchQuery(BaseModel):
    """可向量化子块（Child Chunk）检索查询参数。"""

    document_id: int | None = Field(default=None, gt=0, description="所属文档 ID")
    parent_id: int | None = Field(default=None, gt=0, description="所属父块 ID")
    kb_id: int | None = Field(default=None, gt=0, description="所属知识库 ID")
    vector_statuses: list[str] = Field(default_factory=list, description="向量化状态过滤")
    statuses: list[str] = Field(default_factory=list, description="子块活跃状态过滤")
    section_path_contains: str | None = Field(default=None, description="章节路径匹配")
    source_row_from: int | None = Field(default=None, ge=0, description="表格起始行号")
    source_row_to: int | None = Field(default=None, ge=0, description="表格截止行号")
    has_vector_id: bool | None = Field(default=None, description="是否已有 Qdrant point ID")
    keyword: str | None = Field(default=None, description="正文关键字搜索")
    limit: int = Field(default=50, ge=1, le=100, description="返回条数")
    offset: int = Field(default=0, ge=0, description="偏移量")

    @model_validator(mode="after")
    def validate_source_row_range(self) -> "ChildChunkSearchQuery":
        """校验表格源行号起止范围。"""
        if (
            self.source_row_from is not None
            and self.source_row_to is not None
            and self.source_row_from > self.source_row_to
        ):
            raise ValueError("source_row_from 不能大于 source_row_to")
        return self


class ChildChunkResult(BaseModel):
    """可向量化子块实体详情视图。"""

    id: int
    chunk_code: str
    parent_id: int
    doc_id: int
    kb_id: int
    domain_code: str
    business_scene: str | None
    chunk_index: int
    chunk_type: str
    section_path: list[str] | None
    source_row_index: int | None
    content: str
    embedding_text: str
    token_count: int | None
    vector_status: str
    qdrant_point_id: str | None
    status: str
    version: int
    created_at: datetime
    updated_at: datetime
    indexed_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class ListChildChunksResult(BaseModel):
    """可向量化子块检索的分页返回结果。"""

    items: list[ChildChunkResult]
    total: int
    limit: int
    offset: int


class DocumentChunkStatisticsResult(BaseModel):
    """单篇文档的切块与向量状态统计结果视图。"""

    document_id: int
    doc_code: str
    parent_count: int
    child_count: int
    parent_status_counts: dict[str, int]
    child_status_counts: dict[str, int]
    vector_status_counts: dict[str, int]
    chunk_type_counts: dict[str, int]
    chunks_with_vector_id: int
    chunks_without_vector_id: int


class KnowledgeBaseStatisticsResult(BaseModel):
    """知识库全局宏观统计视图（包含文档总数、状态分布、父子块与向量统计）。"""

    kb_id: int
    kb_code: str
    name: str
    domain_code: str
    business_scene: str | None
    status: str
    visibility: str
    document_count: int
    active_document_count: int
    failed_document_count: int
    indexed_document_count: int
    parent_count: int
    child_count: int
    vector_status_counts: dict[str, int]


class DocumentOverviewResult(BaseModel):
    """单篇文档全景概览结果视图（聚合文档主表、流水线状态与切块统计）。"""

    document: DocumentResult
    pipeline_state: DocumentPipelineStateResult
    chunk_statistics: DocumentChunkStatisticsResult

    model_config = ConfigDict(from_attributes=True)



class DocumentArtifactCreate(BaseModel):
    """创建并持久化文档派生产物所需的输入模型。"""

    document_id: int
    artifact_code: str
    artifact_type: str
    artifact_role: str
    artifact_format: str
    artifact_uri: str
    artifact_hash: str | None = None
    hash_algorithm: str | None = "sha256"
    provider: str | None = None
    processor: str | None = None
    file_size: int | None = None
    char_count: int | None = None
    line_count: int | None = None
    status: str = "active"
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_by_actor_code: str | None = None
