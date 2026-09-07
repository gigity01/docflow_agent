"""应用统一 API Router 汇聚模块。

职责说明：
- 统一挂载所有领域 presentation 层暴露的 HTTP Router，并统一赋予 `/api` 路径前缀。
- 包含路由分支：
  - Conversation 模块路由 (`/api/conversations/*`): 接收用户消息、执行 Context 路由与 Planner，发布异步 Plan 并查询 Turn 状态。
  - Context 兼容路由 (`/api/context/*`): 提供向后兼容的 Context 路由与下游 Turn 完成端点。
  - Document 核心操作路由 (`/api/admin/documents/*`): 提供上传原件、转换清洗、构建切块与向量索引等四个独立步骤。
  - Document 查询路由 (`/api/admin/documents/*` 的 artifacts、parent-blocks、child-chunks、knowledge-base 统计等只读视图）。
"""

from fastapi import APIRouter
from app.modules.messaging.presentation.router import router as runtime_recovery_router

from app.modules.context.presentation.router import (
    legacy_router as context_legacy_router,
)
from app.modules.conversation.presentation.router import (
    router as conversation_router,
)
from app.modules.document.presentation.router import (
    artifact_router as document_artifacts_router,
    child_chunk_router,
    knowledge_base_router,
    parent_block_router,
    router as documents_router,
)

# 创建全局 API Router 并指定 /api 统一前缀
api_router = APIRouter(prefix="/api")
api_router.include_router(runtime_recovery_router)

# 1. 注册会话消息编排与 Turn 状态查询路由
api_router.include_router(conversation_router)

# 2. 注册 Context 兼容端点路由（部分已标记 deprecated）
api_router.include_router(context_legacy_router)

# 3. 注册 Document 主干处理与生命周期管理路由
api_router.include_router(documents_router)

# 4. 注册 Document 产物查询路由
api_router.include_router(document_artifacts_router)

# 5. 注册父级语义块 (ParentBlock) 查询路由
api_router.include_router(parent_block_router)

# 6. 注册子级切块 (ChildChunk) 查询路由
api_router.include_router(child_chunk_router)

# 7. 注册知识库统计查询路由
api_router.include_router(knowledge_base_router)
