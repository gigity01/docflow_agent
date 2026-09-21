"""FastAPI 应用生命周期与全局依赖对象装配模块。"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from qdrant_client.models import PointStruct
from sqlalchemy.exc import IntegrityError

from app.config.settings import (
    AGENT_TOOL_LOG_DIR,
    CONTEXT_RESOURCE_QUEUE_MAX_SIZE,
    CONTEXT_ROUTE_LOCK_BLOCKING_TIMEOUT_SECONDS,
    CONTEXT_ROUTE_LOCK_TIMEOUT_SECONDS,
    CLEANED_STORAGE_DIR,
    DEFAULT_CREATED_BY_ACTOR_CODE,
    DEFAULT_DOCUMENT_STATUS,
    DEFAULT_DOCUMENT_VERSION,
    DEEPSEEK_API_KEY,
    DOCUMENT_CODE_PREFIX,
    DOCUMENT_CODE_RANDOM_LENGTH,
    DOCUMENT_CHUNK_LOG_DIR,
    DOCUMENT_INDEX_LOG_DIR,
    DOCUMENT_PROCESS_LOG_DIR,
    DOCUMENT_UPLOAD_LOG_DIR,
    EMBEDDING_BATCH_SIZE,
    EMBEDDING_MODEL_NAME,
    EMBEDDING_VECTOR_SIZE,
    MAX_UPLOAD_FILE_SIZE,
    PROCESSING_STAGING_STORAGE_DIR,
    RAW_EXTERNAL_STORAGE_DIR,
    RAW_LOCAL_STORAGE_DIR,
    REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS,
    REDIS_SOCKET_TIMEOUT_SECONDS,
    REDIS_URL,
    SECONDARY_TEXT_STORAGE_DIR,
)
from app.bootstrap.container import AppContainer
from app.agent_runtime.context import (
    ContextToolServices,
    DocumentToolServices,
    OperationsToolServices,
)
from app.infrastructure.database.named_lock import MySQLNamedLockManager
from app.infrastructure.database.session import engine
from app.infrastructure.database.uow import SQLAlchemyUnitOfWork
from app.infrastructure.llm.deepseek.provider import DeepSeekModelProvider
from app.infrastructure.redis.client import (
    close_redis_client,
    create_redis_client,
    ping_redis_client,
)
from app.modules.context.infrastructure.cache.redis_resource_queue import (
    ContextResourceQueueRepository,
)
from app.modules.context.infrastructure.llm.deepseek_router import (
    DeepSeekContextRouter,
)
from app.modules.context.infrastructure.locking.redis_conversation_lock import (
    ConversationRouteLockManager,
)
from app.modules.context.infrastructure.persistence.mapper import (
    SQLAlchemyContextRecordFactory,
    build_context_chain,
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
from app.modules.document.application.ports import (
    DocumentApplicationPorts,
)
from app.modules.document.agent_tools.command_tools import (
    DOCUMENT_BUILD_CHUNKS_PERMISSION,
    DOCUMENT_INDEX_VECTORS_PERMISSION,
    DOCUMENT_PROCESS_PERMISSION,
)
from app.modules.document.agent_tools.query_tools import (
    DOCUMENT_READ_PERMISSION,
)
from app.modules.document.agent_tools.schemas import (
    BuildDocumentChunksToolOutput,
    IndexDocumentVectorsToolOutput,
    ProcessDocumentToolOutput,
)
from app.modules.document.application.settings import (
    DocumentIndexingSettings,
    DocumentProcessingSettings,
    DocumentUploadSettings,
)
from app.modules.document.application.use_cases.build_chunks import (
    BuildChunksCompensator,
    BuildChunksUseCase,
)
from app.modules.document.application.use_cases.index_vectors import (
    IndexVectorsCompensator,
    IndexVectorsUseCase,
)
from app.modules.document.application.use_cases.get_document import (
    GetDocumentUseCase,
)
from app.modules.document.application.use_cases.get_document_overview import (
    GetDocumentOverviewUseCase,
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
    ProcessDocumentCompensator,
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
from app.modules.document.infrastructure.chunking.factory import get_chunker
from app.modules.document.infrastructure.embedding.dashscope import (
    EmbeddingService,
)
from app.modules.document.infrastructure.parsing.docling_client import (
    DoclingClient,
)
from app.modules.document.infrastructure.parsing.factory import get_processor
from app.modules.document.infrastructure.persistence.models.child_chunk import (
    ChildChunk,
)
from app.modules.document.infrastructure.persistence.models.document import (
    Document,
)
from app.modules.document.infrastructure.persistence.models.parent_block import (
    ParentBlock,
)
from app.modules.document.infrastructure.storage.local import (
    calculate_file_hash,
    cleanup_file,
    get_safe_extension,
    validate_content_type,
)
from app.modules.document.infrastructure.vector_store.qdrant import (
    QdrantVectorStore,
)
from app.modules.operations.application.query_service import OperationsQueryService
from app.modules.operations.application.use_cases import (
    GetDocumentOperationTimelineUseCase,
    GetDocumentWorkflowTimelineUseCase,
    QueryDocumentLogEventsUseCase,
)
from app.modules.operations.infrastructure.jsonl_repository import (
    JsonlLogRepository,
    JsonlLogSource,
)
from app.modules.planning.application.ports import PlanningApplicationPorts
from app.modules.planning.application.run_planning import RunPlanningUseCase
from app.modules.planning.application.use_cases import (
    build_planning_use_cases,
)
from app.modules.planning.infrastructure.persistence.models import Plan, Task
from app.modules.planning.infrastructure.persistence.models import TaskDependency
from app.modules.messaging.infrastructure.persistence.models import (
    InboxEvent,
    OutboxEvent,
)
from app.modules.clarification.infrastructure.persistence.models import (
    ClarificationRequest,
)
from app.modules.task_runtime.infrastructure.persistence.models import (
    TaskExecution,
)
from app.modules.task_runtime.application.ports import (
    CompensatorRegistry,
    ExecutorRegistry,
    TaskRuntimePorts,
)
from app.modules.task_runtime.application.registry import (
    build_capability_registry,
)
from app.modules.task_runtime.application.runtime import TaskRuntimeService
from app.modules.task_runtime.infrastructure.executors import (
    AgentTaskExecutor,
    DeterministicBuildDocumentChunksExecutor,
    DeterministicIndexDocumentVectorsExecutor,
    DeterministicProcessDocumentExecutor,
    adapt_build_document_chunks_output,
    adapt_index_document_vectors_output,
    adapt_process_document_output,
)
from app.modules.task_runtime.infrastructure.compensators import (
    BuildDocumentChunksOperationCompensator,
    IndexDocumentVectorsOperationCompensator,
    ProcessDocumentOperationCompensator,
)
from app.modules.messaging.application.outbox import OutboxPublisher
from app.modules.messaging.application.delivery import MessageDeliveryService
from app.modules.task_runtime.application.recover_plan import RecoverPlanUseCase
from app.modules.messaging.infrastructure.redis_streams import (
    RedisStreamPublisher,
)
from app.modules.messaging.worker.dispatcher import RuntimeEventDispatcher
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
from app.modules.planning.application.replan import ReplanUseCase
from app.shared.observability.context_logger import ContextEventLogger


async def build_container() -> AppContainer:
    """初始化底层客户端并装配应用级共享依赖对象容器。"""
    # 1. 初始化 Redis 客户端并执行健康探测
    redis_client = create_redis_client(
        REDIS_URL,
        socket_connect_timeout_seconds=(
            REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS
        ),
        socket_timeout_seconds=REDIS_SOCKET_TIMEOUT_SECONDS,
    )
    deepseek_provider = None

    try:
        if not await ping_redis_client(redis_client):
            raise RuntimeError("Redis PING 未返回成功")

        # 2. 装配 Context 领域组件
        context_event_logger = ContextEventLogger()
        route_lock_manager = ConversationRouteLockManager(
            redis_client,
            lock_timeout_seconds=CONTEXT_ROUTE_LOCK_TIMEOUT_SECONDS,
            blocking_timeout_seconds=(
                CONTEXT_ROUTE_LOCK_BLOCKING_TIMEOUT_SECONDS
            ),
            event_logger=context_event_logger,
        )
        queue_repository = ContextResourceQueueRepository(
            redis_client,
            capacity=CONTEXT_RESOURCE_QUEUE_MAX_SIZE,
        )
        record_factory = SQLAlchemyContextRecordFactory()
        resource_service = ContextResourceService(
            queue_repository=queue_repository,
            uow_factory=SQLAlchemyUnitOfWork,
            record_factory=record_factory,
        )
        # 3. 初始化 DeepSeek 提供者与 Context 智能路由
        deepseek_provider = (
            DeepSeekModelProvider.create()
            if DEEPSEEK_API_KEY is not None
            else None
        )
        agent_router = (
            DeepSeekContextRouter(
                deepseek_provider,
                event_logger=context_event_logger,
            )
            if deepseek_provider is not None
            else None
        )
        context_service = ContextService(
            agent_router=agent_router,
            route_lock_manager=route_lock_manager,
            resource_service=resource_service,
            uow_factory=SQLAlchemyUnitOfWork,
            record_factory=record_factory,
            chain_mapper=build_context_chain,
            event_logger=context_event_logger,
        )
        context_query_service = ContextQueryService(
            uow_factory=SQLAlchemyUnitOfWork
        )

        # 4. 装配 Operations 日志查询服务
        operations_query_service = OperationsQueryService(
            document_logs=JsonlLogRepository(
                (
                    JsonlLogSource(
                        "upload",
                        DOCUMENT_UPLOAD_LOG_DIR,
                        "upload",
                    ),
                    JsonlLogSource(
                        "process",
                        DOCUMENT_PROCESS_LOG_DIR,
                        "process",
                    ),
                    JsonlLogSource(
                        "chunk",
                        DOCUMENT_CHUNK_LOG_DIR,
                        "chunk",
                    ),
                    JsonlLogSource(
                        "index",
                        DOCUMENT_INDEX_LOG_DIR,
                        "index",
                    ),
                )
            ),
            agent_tool_logs=JsonlLogRepository(
                (
                    JsonlLogSource(
                        "agent_tool",
                        AGENT_TOOL_LOG_DIR,
                        "agent-tool",
                    ),
                )
            ),
        )
        get_conversation_turn = GetConversationTurnUseCase(
            context_query_service
        )
        list_conversation_turns = ListConversationTurnsUseCase(
            context_query_service
        )
        get_context_chain = GetContextChainUseCase(context_query_service)
        list_context_chains = ListContextChainsUseCase(
            context_query_service
        )
        list_context_chain_nodes = ListContextChainNodesUseCase(
            context_query_service
        )
        list_context_chain_resources = ListContextChainResourcesUseCase(
            context_query_service
        )
        list_context_selection_records = ListContextSelectionRecordsUseCase(
            context_query_service
        )
        query_document_log_events = QueryDocumentLogEventsUseCase(
            operations_query_service
        )
        get_document_operation_timeline = (
            GetDocumentOperationTimelineUseCase(operations_query_service)
        )
        get_document_workflow_timeline = (
            GetDocumentWorkflowTimelineUseCase(operations_query_service)
        )

        # 5. 构建 Collector Agent 集合
        collector_agents = (
            _build_collector_agents(deepseek_provider)
            if deepseek_provider is not None
            else None
        )
        planning_ports = PlanningApplicationPorts(
            uow_factory=SQLAlchemyUnitOfWork,
            plan_factory=Plan,
            task_factory=Task,
            task_dependency_factory=TaskDependency,
            outbox_event_factory=OutboxEvent,
            inbox_event_factory=InboxEvent,
            clarification_request_factory=ClarificationRequest,
            integrity_error_type=IntegrityError,
        )
        planning_use_cases = build_planning_use_cases(planning_ports)

        # 6. 装配 Document 领域 Ports 与 Use Cases
        document_ports = DocumentApplicationPorts(
            uow_factory=SQLAlchemyUnitOfWork,
            document_factory=Document,
            parent_block_factory=ParentBlock,
            child_chunk_factory=ChildChunk,
            processor_factory=get_processor,
            chunker_factory=get_chunker,
            embedding_factory=EmbeddingService,
            vector_store_factory=QdrantVectorStore,
            external_effect_fence=MySQLNamedLockManager(engine),
            docling_factory=DoclingClient,
            point_factory=PointStruct,
            validate_content_type=validate_content_type,
            get_safe_extension=get_safe_extension,
            calculate_file_hash=calculate_file_hash,
            cleanup_file=cleanup_file,
            integrity_error_type=IntegrityError,
        )
        upload_document = UploadDocumentUseCase(
            ports=document_ports,
            settings=DocumentUploadSettings(
                raw_local_storage_dir=RAW_LOCAL_STORAGE_DIR,
                raw_external_storage_dir=RAW_EXTERNAL_STORAGE_DIR,
                max_upload_file_size=MAX_UPLOAD_FILE_SIZE,
                default_document_status=DEFAULT_DOCUMENT_STATUS,
                default_document_version=DEFAULT_DOCUMENT_VERSION,
                default_created_by_actor_code=(
                    DEFAULT_CREATED_BY_ACTOR_CODE
                ),
                document_code_prefix=DOCUMENT_CODE_PREFIX,
                document_code_random_length=(
                    DOCUMENT_CODE_RANDOM_LENGTH
                ),
            ),
        )
        processing_settings = DocumentProcessingSettings(
            cleaned_storage_dir=CLEANED_STORAGE_DIR,
            secondary_text_storage_dir=SECONDARY_TEXT_STORAGE_DIR,
            staging_storage_dir=PROCESSING_STAGING_STORAGE_DIR,
        )
        process_document = ProcessDocumentUseCase(
            ports=document_ports,
            settings=processing_settings,
        )
        build_chunks = BuildChunksUseCase(ports=document_ports)
        get_document = GetDocumentUseCase(
            uow_factory=SQLAlchemyUnitOfWork
        )
        list_documents = ListDocumentsUseCase(
            uow_factory=SQLAlchemyUnitOfWork
        )
        search_documents = SearchDocumentsUseCase(
            uow_factory=SQLAlchemyUnitOfWork
        )
        get_document_pipeline_state = GetDocumentPipelineStateUseCase(
            uow_factory=SQLAlchemyUnitOfWork
        )
        list_document_artifacts = ListDocumentArtifactsUseCase(
            uow_factory=SQLAlchemyUnitOfWork
        )
        search_document_artifacts = SearchDocumentArtifactsUseCase(
            uow_factory=SQLAlchemyUnitOfWork
        )
        list_parent_blocks = ListParentBlocksUseCase(
            uow_factory=SQLAlchemyUnitOfWork
        )
        list_child_chunks = ListChildChunksUseCase(
            uow_factory=SQLAlchemyUnitOfWork
        )
        get_document_chunk_statistics = (
            GetDocumentChunkStatisticsUseCase(
                uow_factory=SQLAlchemyUnitOfWork
            )
        )
        get_knowledge_base_statistics = (
            GetKnowledgeBaseStatisticsUseCase(
                uow_factory=SQLAlchemyUnitOfWork
            )
        )
        get_document_overview = (
            GetDocumentOverviewUseCase(
                uow_factory=SQLAlchemyUnitOfWork
            )
        )
        index_vectors = IndexVectorsUseCase(
            ports=document_ports,
            settings=DocumentIndexingSettings(
                embedding_batch_size=EMBEDDING_BATCH_SIZE,
                embedding_model_name=EMBEDDING_MODEL_NAME,
                embedding_vector_size=EMBEDDING_VECTOR_SIZE,
            ),
        )
        document_services = DocumentToolServices(
            get_document=get_document,
            list_documents=list_documents,
            search_documents=search_documents,
            get_document_pipeline_state=get_document_pipeline_state,
            list_document_artifacts=list_document_artifacts,
            search_document_artifacts=search_document_artifacts,
            list_parent_blocks=list_parent_blocks,
            list_child_chunks=list_child_chunks,
            get_document_chunk_statistics=get_document_chunk_statistics,
            get_knowledge_base_statistics=get_knowledge_base_statistics,
            process_document=process_document,
            build_chunks=build_chunks,
            index_vectors=index_vectors,
        )

        # 7. 装配 Planner Agent Runner 与 RunPlanningUseCase
        planner_agent_runner = (
            _build_planner_agent(deepseek_provider, collector_agents)
            if deepseek_provider is not None and collector_agents is not None
            else None
        )
        run_planning = (
            RunPlanningUseCase(
                ports=planning_ports,
                planning_use_cases=planning_use_cases,
                planner_runner=planner_agent_runner,
                document_services=document_services,
                context_services=ContextToolServices(
                    query_service=context_query_service,
                    get_conversation_turn=get_conversation_turn,
                    list_conversation_turns=list_conversation_turns,
                    get_context_chain=get_context_chain,
                    list_context_chains=list_context_chains,
                    list_context_chain_nodes=list_context_chain_nodes,
                    list_context_chain_resources=(
                        list_context_chain_resources
                    ),
                    list_context_selection_records=(
                        list_context_selection_records
                    ),
                ),
                context_resource_service=resource_service,
                context_chain_mapper=build_context_chain,
                operations_services=OperationsToolServices(
                    query_service=operations_query_service,
                    query_document_log_events=query_document_log_events,
                    get_document_operation_timeline=(
                        get_document_operation_timeline
                    ),
                    get_document_workflow_timeline=(
                        get_document_workflow_timeline
                    ),
                ),
            )
            if planner_agent_runner is not None
            else None
        )

        # 8. 装配 Task 执行器（优先使用 Agent Executor，降级为确定性 Executor）与补偿器
        document_executor_agents = (
            _build_document_executor_agents(deepseek_provider)
            if deepseek_provider is not None
            else None
        )
        if document_executor_agents is not None:
            task_executors = {
                "document.process": AgentTaskExecutor(
                    agent=document_executor_agents.process,
                    executor_code="document.process",
                    primary_tool_name="process_document",
                    tool_output_model=ProcessDocumentToolOutput,
                    output_adapter=adapt_process_document_output,
                    document_services=document_services,
                    permissions=frozenset(
                        {
                            DOCUMENT_READ_PERMISSION,
                            DOCUMENT_PROCESS_PERMISSION,
                        }
                    ),
                    run_config=document_executor_agents.run_config,
                ),
                "document.build_chunks": AgentTaskExecutor(
                    agent=document_executor_agents.build_chunks,
                    executor_code="document.build_chunks",
                    primary_tool_name="build_document_chunks",
                    tool_output_model=BuildDocumentChunksToolOutput,
                    output_adapter=adapt_build_document_chunks_output,
                    document_services=document_services,
                    permissions=frozenset(
                        {
                            DOCUMENT_READ_PERMISSION,
                            DOCUMENT_BUILD_CHUNKS_PERMISSION,
                        }
                    ),
                    run_config=document_executor_agents.run_config,
                ),
                "document.index_vectors": AgentTaskExecutor(
                    agent=document_executor_agents.index_vectors,
                    executor_code="document.index_vectors",
                    primary_tool_name="index_document_vectors",
                    tool_output_model=IndexDocumentVectorsToolOutput,
                    output_adapter=adapt_index_document_vectors_output,
                    document_services=document_services,
                    permissions=frozenset(
                        {
                            DOCUMENT_READ_PERMISSION,
                            DOCUMENT_INDEX_VECTORS_PERMISSION,
                        }
                    ),
                    run_config=document_executor_agents.run_config,
                ),
            }
        else:
            task_executors = {
                "document.process": DeterministicProcessDocumentExecutor(
                    process_document
                ),
                "document.build_chunks": (
                    DeterministicBuildDocumentChunksExecutor(build_chunks)
                ),
                "document.index_vectors": (
                    DeterministicIndexDocumentVectorsExecutor(index_vectors)
                ),
            }
        task_compensators = {
            "document.process": ProcessDocumentOperationCompensator(
                ProcessDocumentCompensator(
                    ports=document_ports,
                    settings=processing_settings,
                )
            ),
            "document.build_chunks": (
                BuildDocumentChunksOperationCompensator(
                    BuildChunksCompensator(ports=document_ports)
                )
            ),
            "document.index_vectors": (
                IndexDocumentVectorsOperationCompensator(
                    IndexVectorsCompensator(ports=document_ports)
                )
            ),
        }

        # 9. 装配 Task Runtime、事件发布分派与聚合
        task_runtime = TaskRuntimeService(
            ports=TaskRuntimePorts(
                uow_factory=SQLAlchemyUnitOfWork,
                task_execution_factory=TaskExecution,
                outbox_event_factory=OutboxEvent,
                inbox_event_factory=InboxEvent,
            ),
            capabilities=build_capability_registry(),
            executors=ExecutorRegistry(task_executors),
            compensators=CompensatorRegistry(task_compensators),
        )
        aggregate_plan = AggregatePlanUseCase(
            uow_factory=SQLAlchemyUnitOfWork,
            context_service=context_service,
        )
        outbox_publisher = OutboxPublisher(
            uow_factory=SQLAlchemyUnitOfWork,
            publisher=RedisStreamPublisher(redis_client),
        )
        answer_clarification = AnswerClarificationUseCase(
            uow_factory=SQLAlchemyUnitOfWork,
            outbox_event_factory=OutboxEvent,
        )
        get_turn_status = GetTurnStatusUseCase(SQLAlchemyUnitOfWork)
        replan = (
            ReplanUseCase(
                ports=planning_ports,
                run_planning=run_planning,
            )
            if run_planning is not None
            else None
        )
        send_conversation_message = (
            SendConversationMessageUseCase(
                context_service=context_service,
                run_planning=run_planning,
                answer_clarification=answer_clarification,
            )
            if run_planning is not None
            else None
        )
        runtime_event_dispatcher = RuntimeEventDispatcher(
            uow_factory=SQLAlchemyUnitOfWork,
            inbox_event_factory=InboxEvent,
            runtime=task_runtime,
            replan=replan,
            aggregate_plan=aggregate_plan,
        )
        return AppContainer(
            message_delivery=MessageDeliveryService(uow_factory=SQLAlchemyUnitOfWork,
                inbox_factory=InboxEvent, outbox_factory=OutboxEvent),
            recover_plan=RecoverPlanUseCase(uow_factory=SQLAlchemyUnitOfWork,
                outbox_factory=OutboxEvent, capabilities=build_capability_registry()),
            redis_client=redis_client,
            deepseek_provider=deepseek_provider,
            context_agent_router=agent_router,
            context_route_lock_manager=route_lock_manager,
            context_resource_service=resource_service,
            context_service=context_service,
            context_query_service=context_query_service,
            get_conversation_turn=get_conversation_turn,
            list_conversation_turns=list_conversation_turns,
            get_context_chain=get_context_chain,
            list_context_chains=list_context_chains,
            list_context_chain_nodes=list_context_chain_nodes,
            list_context_chain_resources=list_context_chain_resources,
            list_context_selection_records=list_context_selection_records,
            operations_query_service=operations_query_service,
            query_document_log_events=query_document_log_events,
            get_document_operation_timeline=(
                get_document_operation_timeline
            ),
            get_document_workflow_timeline=(
                get_document_workflow_timeline
            ),
            collector_agents=collector_agents,
            planner_agent_runner=planner_agent_runner,
            document_executor_agents=document_executor_agents,
            planning_use_cases=planning_use_cases,
            run_planning=run_planning,
            replan=replan,
            task_runtime=task_runtime,
            aggregate_plan=aggregate_plan,
            outbox_publisher=outbox_publisher,
            runtime_event_dispatcher=runtime_event_dispatcher,
            answer_clarification=answer_clarification,
            send_conversation_message=send_conversation_message,
            get_turn_status=get_turn_status,
            upload_document=upload_document,
            get_document=get_document,
            list_documents=list_documents,
            search_documents=search_documents,
            get_document_pipeline_state=get_document_pipeline_state,
            list_document_artifacts=list_document_artifacts,
            search_document_artifacts=search_document_artifacts,
            list_parent_blocks=list_parent_blocks,
            list_child_chunks=list_child_chunks,
            get_document_chunk_statistics=get_document_chunk_statistics,
            get_knowledge_base_statistics=get_knowledge_base_statistics,
            get_document_overview=get_document_overview,
            process_document=process_document,
            build_chunks=build_chunks,
            index_vectors=index_vectors,
        )
    except Exception:
        # 装配失败时确保释放已创建的资源
        try:
            if deepseek_provider is not None:
                await deepseek_provider.aclose()
        finally:
            await close_redis_client(redis_client)
        raise


def _build_collector_agents(provider: DeepSeekModelProvider):
    """仅在启用 Agent 时加载 Agents SDK Collector 定义。"""
    from app.agents.collectors import build_collector_agents

    return build_collector_agents(
        model=provider.model,
        model_settings=provider.model_settings,
    )


def _build_planner_agent(provider, collectors):
    """使用同一 DeepSeek Provider 装配 Planner 与并发 Tool 配置。"""
    from app.agents.planner import build_planner_agent

    return build_planner_agent(
        model=provider.model,
        model_settings=provider.model_settings,
        collectors=collectors,
    )


def _build_document_executor_agents(provider):
    """使用同一 DeepSeek Provider 装配受限 Document Executor Agents。"""
    from app.agents.document_executors import (
        build_document_executor_agents,
    )

    return build_document_executor_agents(
        model=provider.model,
        model_settings=provider.model_settings,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI 应用生命周期管理器。"""
    container = await build_container()
    app.state.container = container

    try:
        yield
    finally:
        await container.aclose()
