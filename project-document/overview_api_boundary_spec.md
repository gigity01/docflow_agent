# 文档总览接口（Overview API）边界与技术规范文档

## 1. 需求与目标

### 1.1 现状与痛点
目前客户端/前端需要获取单篇文档的全局概览信息时，必须分别调用 3 个独立的只读接口：
1. `GET /api/admin/documents/{document_id}`
   - 职责：获取文档主表的元数据与技术/生命周期/存储三状态（`DocumentResponse`）。
2. `GET /api/admin/documents/{document_id}/pipeline-state`
   - 职责：获取文档处理、切块与向量索引流水线的阶段状态快照（`DocumentPipelineStateResponse`）。
3. `GET /api/admin/documents/{document_id}/chunk-statistics`
   - 职责：获取文档下父块、子块类型与向量状态的详细分布统计（`DocumentChunkStatisticsResponse`）。

该方式存在 3 次网络请求开销，且在并发处理时多次请求可能获取到不一致的数据快照。

### 1.2 目标
新增统一的 `overview` 只读接口，在单次请求内复用现有的底层查询能力，聚合返回文档信息、流水线状态和切块统计三部分数据：
- 严格限定在需求范围之内，坚决不包含 `artifacts`。
- 保留现有的 3 个独立接口不变。
- 规范非法参数处理。
- 保证三状态轴完整性，并对“文档在流程中”的状态做合理收敛（返回 `"in_progress"`），不暴露内部流转细节状态。

---

## 2. 核心边界与设计规范（已确认冻结）

### 边界 1：接口契约与路由
- **HTTP 方法**：`GET`
- **请求路径**：`/api/admin/documents/{document_id}/overview`
- **路径参数**：
  - `document_id: int`，必须为大于 0 的正整数（FastAPI 路由校验：`Path(gt=0)`）。
- **查询参数（Query Parameters）**：无。
- **请求体（Request Body）**：无。

### 边界 2：非法参数与异常处理规范
- **非法路径参数（HTTP 422）**：
  - 场景：`document_id <= 0`（如 `/api/admin/documents/-1/overview`）或非整数类型（如 `/api/admin/documents/abc/overview`）。
  - 处理：由 FastAPI 路由层标准参数校验直接拦截并返回 HTTP `422 Unprocessable Entity`。
- **文档不存在（HTTP 404）**：
  - 场景：`document_id` 为合法正整数但在数据库中无记录（如 `/api/admin/documents/999999/overview`）。
  - 处理：应用层抛出 `DocumentApplicationError(404, "文档不存在")`，返回 HTTP `404 Not Found`，响应体 `{"detail": "文档不存在"}`（与现有 3 个接口完全一致）。
- **文档处于初始状态（HTTP 200）**：
  - 场景：文档仅上传完成，尚未生成切块或向量。
  - 处理：正常返回 HTTP `200`，`pipeline_state` 与 `chunk_statistics` 中的数值字段为 0，字典为 `{}`，不抛出任何异常。

### 边界 3：数据范围边界（严格限定）
- **包含内容（仅限三项）**：
  1. `document`：文档核心信息（对应现有 `DocumentResponse` / `DocumentResult` 结构）。
  2. `pipeline_state`：流水线状态与进度（对应现有 `DocumentPipelineStateResponse` / `DocumentPipelineStateResult` 结构）。
  3. `chunk_statistics`：父子切块与向量状态分布统计（对应现有 `DocumentChunkStatisticsResponse` / `DocumentChunkStatisticsResult` 结构）。
- **排除内容（严格禁止超纲）**：
  - ❌ **不包含** 派生产物列表（`artifacts`）。
  - ❌ **不包含** 父块/子块的正文全文、分段内容或向量向量值。
  - ❌ **不包含** 知识库全局统计。
  - ❌ **不包含** 任何修改、重试或异步任务触发操作（纯只读 GET 接口）。

### 边界 4：三状态轴保证与“流程中”状态收敛规则
系统完整维护文档的三状态轴：
1. **状态轴 1：技术流水线状态（`status` / `document_status`）**
   - 内部流转状态集合：`IN_PROGRESS_STATUSES = {"processing", "chunking", "indexing"}`。
   - **收敛规则**：当文档处于内部流转中（即 `status` 为 `processing`、`chunking` 或 `indexing`）时，对外统一暴露状态值 `"in_progress"`，不暴露内部切块中或向量化中等细节。
   - **终态/静止态**：当文档处于非流转中状态时，如实返回（如 `uploaded`, `processed`, `chunked`, `indexed`, `failed`）。
2. **状态轴 2：业务生命周期状态（`lifecycle_status`）**
   - 如实返回当前业务状态（`scheduled` / `active` / `expired` / `replaced` / `deleted`）。
3. **状态轴 3：底层物理存储状态（`storage_status`）**
   - 如实返回当前存储状态（`active` / `archiving` / `archived` / `deleted`）。

### 边界 5：顶级响应结构与 JSON 样例
顶级 JSON 响应严格使用以下三段式结构：

```json
{
  "document": {
    "id": 1,
    "doc_code": "doc_20260921_001",
    "kb_id": 1,
    "domain_code": "legal",
    "business_scene": "contract",
    "title": "合同原件.pdf",
    "original_filename": "contract.pdf",
    "file_size": 1048576,
    "source_type": "pdf",
    "source_uri": "minio://raw/contract.pdf",
    "cleaned_uri": "minio://cleaned/contract.md",
    "content_hash": "a1b2c3d4...",
    "active_content_hash": "a1b2c3d4...",
    "lifecycle_status": "active",
    "storage_status": "active",
    "version": 1,
    "status": "in_progress",
    "replaced_by": null,
    "risk_level": "low",
    "effective_at": null,
    "expired_at": null,
    "created_by_actor_code": "admin",
    "created_at": "2026-09-21T10:00:00",
    "updated_at": "2026-09-21T10:05:00",
    "indexed_at": null
  },
  "pipeline_state": {
    "document_id": 1,
    "doc_code": "doc_20260921_001",
    "source_type": "pdf",
    "source_uri": "minio://raw/contract.pdf",
    "cleaned_uri": "minio://cleaned/contract.md",
    "document_status": "in_progress",
    "lifecycle_status": "active",
    "storage_status": "active",
    "parent_count": 0,
    "child_count": 0,
    "vector_status_counts": {},
    "indexed_at": null
  },
  "chunk_statistics": {
    "document_id": 1,
    "doc_code": "doc_20260921_001",
    "parent_count": 0,
    "child_count": 0,
    "parent_status_counts": {},
    "child_status_counts": {},
    "vector_status_counts": {},
    "chunk_type_counts": {},
    "chunks_with_vector_id": 0,
    "chunks_without_vector_id": 0
  }
}
```

### 边界 6：系统实现与现有接口兼容性
- **现有接口完全保留**：
  - `GET /api/admin/documents/{document_id}`
  - `GET /api/admin/documents/{document_id}/pipeline-state`
  - `GET /api/admin/documents/{document_id}/chunk-statistics`
  以上 3 个端点维持现有行为不变，继续提供独立查询能力。
- **能力复用与数据一致性**：
  - 在应用层新增 `GetDocumentOverviewUseCase`，通过单次工作单元（Unit of Work）复用已有 Repository 查询能力完成数据获取与状态组装，保证三部分数据在读取时具备一致的事务快照。

---

## 3. 边界确认结论

所有边界已全部明确并通过确认，无任何遗留待定项。
各边界已冻结，后续开发实施将严格遵循此规范文档执行。
