"""应用级依赖注入对象容器模块。

职责说明：
- 定义集中持有应用生命周期内全局共享的外部客户端（Redis、DeepSeek）、Agent 运行时、领域 Service 与 Use Case 实例的 `AppContainer` 数据类。
- 提供优雅关闭方法 `aclose()`，确保外部资源与连接池按依赖拓扑安全释放。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from redis.asyncio import Redis

from app.infrastructure.llm.deepseek.provider import DeepSeekModelProvider
from app.infrastructure.redis.client import close_redis_client
from app.modules.context.infrastructure.llm.deepseek_router import (
    DeepSeekContextRouter,
)
from app.modules.context.infrastructure.locking.redis_conversation_lock import (
    ConversationRouteLockManager,
)
from app.modules.context.application.context_service import ContextService
from app.modules.context.application.resource_service import (
    ContextResourceService,
)
from app.modules.context.application.query_service import ContextQueryService
from app.modules.context.application.use_cases import (
    GetContextChainUseCase,
    GetConversationTurnUseCase,
    ListContextChainNodesUseCase,
    ListContextChainResourcesUseCase,
    ListContextChainsUseCase,
    ListContextSelectionRecordsUseCase,
    ListConversationTurnsUseCase,
)
from app.modules.document.application.use_cases.build_chunks import (
    BuildChunksUseCase,
)
from app.modules.document.application.use_cases.index_vectors import (
    IndexVectorsUseCase,
)
from app.modules.document.application.use_cases.get_document import (
    GetDocumentUseCase,
)
from app.modules.document.application.use_cases.get_chunk_statistics import (
    GetDocumentChunkStatisticsUseCase,
)
from app.modules.document.application.use_cases.get_knowledge_base_statistics import (
    GetKnowledgeBaseStatisticsUseCase,
)
from app.modules.document.application.use_cases.get_pipeline_state import (
    GetDocumentPipelineStateUseCase,
)
from app.modules.document.application.use_cases.list_artifacts import (
    ListDocumentArtifactsUseCase,
)
from app.modules.document.application.use_cases.list_child_chunks import (
    ListChildChunksUseCase,
)
from app.modules.document.application.use_cases.list_documents import (
    ListDocumentsUseCase,
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
from app.modules.operations.application.query_service import OperationsQueryService
from app.modules.operations.application.use_cases import (
    GetDocumentOperationTimelineUseCase,
    GetDocumentWorkflowTimelineUseCase,
    QueryDocumentLogEventsUseCase,
)
from app.modules.planning.application.use_cases import PlanningUseCases
from app.modules.planning.application.run_planning import RunPlanningUseCase
from app.modules.aggregation.application.aggregate_plan import (
    AggregatePlanUseCase,
)
from app.modules.clarification.application.answer import (
    AnswerClarificationUseCase,
)
from app.modules.conversation.application.get_turn_status import (
    GetTurnStatusUseCase,
)
from app.modules.conversation.application.send_message import (
    SendConversationMessageUseCase,
)
from app.modules.messaging.application.outbox import OutboxPublisher
from app.modules.messaging.application.delivery import MessageDeliveryService
from app.modules.task_runtime.application.recover_plan import RecoverPlanUseCase
from app.modules.messaging.worker.dispatcher import RuntimeEventDispatcher
from app.modules.planning.application.replan import ReplanUseCase
from app.modules.task_runtime.application.runtime import TaskRuntimeService


if TYPE_CHECKING:
    from app.agents.collectors import CollectorAgentSet
    from app.agents.document_executors import DocumentExecutorAgentSet
    from app.agents.planner import PlannerAgentRunner


@dataclass
class AppContainer:
    """集中持有应用生命周期内共享的外部客户端、Agent 运行时与 Use Case 对象容器。

    字段说明：
    - 外部客户端：Redis 客户端、DeepSeek LLM Provider。
    - Context 模块：路由 Agent、并发锁、热队列、ContextService 及只读用例。
    - Operations 模块：日志与时间线查询服务及用例。
    - Planning & Agent：Collector 集合、PlannerRunner、执行器集合、Planning 用例与 Replan 用例。
    - Task Runtime & Messaging：任务运行时服务、Plan 结果聚合器、Outbox 发布器、事件分派器、澄清回答用例。
    - Conversation 模块：用户消息编排用例、Turn 状态查询用例。
    - Document 模块：上传、清洗处理、切块、向量索引及全量只读查询用例。
    """

    # --- 基础客户端与外部提供者 ---
    redis_client: Redis
    deepseek_provider: DeepSeekModelProvider | None

    # --- Context 模块（上下文路由、并发锁、热资源队列与查询）---
    context_agent_router: DeepSeekContextRouter | None
    context_route_lock_manager: ConversationRouteLockManager
    context_resource_service: ContextResourceService
    context_service: ContextService
    context_query_service: ContextQueryService
    get_conversation_turn: GetConversationTurnUseCase
    list_conversation_turns: ListConversationTurnsUseCase
    get_context_chain: GetContextChainUseCase
    list_context_chains: ListContextChainsUseCase
    list_context_chain_nodes: ListContextChainNodesUseCase
    list_context_chain_resources: ListContextChainResourcesUseCase
    list_context_selection_records: ListContextSelectionRecordsUseCase

    # --- Operations 运维日志与时间线查询 ---
    operations_query_service: OperationsQueryService
    query_document_log_events: QueryDocumentLogEventsUseCase
    get_document_operation_timeline: GetDocumentOperationTimelineUseCase
    get_document_workflow_timeline: GetDocumentWorkflowTimelineUseCase

    # --- Agent 与 Planner 编排组件 ---
    collector_agents: CollectorAgentSet | None
    planner_agent_runner: PlannerAgentRunner | None
    document_executor_agents: DocumentExecutorAgentSet | None
    planning_use_cases: PlanningUseCases
    run_planning: RunPlanningUseCase | None
    replan: ReplanUseCase | None

    # --- 异步 Task Runtime、消息事件分派与结果聚合 ---
    task_runtime: TaskRuntimeService
    aggregate_plan: AggregatePlanUseCase
    outbox_publisher: OutboxPublisher
    runtime_event_dispatcher: RuntimeEventDispatcher
    answer_clarification: AnswerClarificationUseCase

    # --- 面向用户的会话编排与状态查询 ---
    send_conversation_message: SendConversationMessageUseCase | None
    get_turn_status: GetTurnStatusUseCase

    # --- 文档领域核心写操作 Use Cases ---
    upload_document: UploadDocumentUseCase
    process_document: ProcessDocumentUseCase
    build_chunks: BuildChunksUseCase
    index_vectors: IndexVectorsUseCase

    # --- 文档只读与统计查询 Use Cases ---
    get_document: GetDocumentUseCase
    list_documents: ListDocumentsUseCase
    search_documents: SearchDocumentsUseCase
    get_document_pipeline_state: GetDocumentPipelineStateUseCase
    list_document_artifacts: ListDocumentArtifactsUseCase
    search_document_artifacts: SearchDocumentArtifactsUseCase
    list_parent_blocks: ListParentBlocksUseCase
    list_child_chunks: ListChildChunksUseCase
    get_document_chunk_statistics: GetDocumentChunkStatisticsUseCase
    get_knowledge_base_statistics: GetKnowledgeBaseStatisticsUseCase
    message_delivery: MessageDeliveryService | None = None
    recover_plan: RecoverPlanUseCase | None = None

    async def aclose(self) -> None:
        """按依赖倒序安全关闭外部客户端与底层连接池。"""
        try:
            if self.deepseek_provider is not None:
                await self.deepseek_provider.aclose()
        finally:
            await close_redis_client(self.redis_client)
