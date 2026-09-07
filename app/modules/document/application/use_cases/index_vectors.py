"""索引文档向量应用用例：以短事务编排领取、执行与结果登记。

流水线阶段 4：Index Vectors
负责将可向量化子块（ChildChunk）计算 DashScope / Qwen Embedding 向量并写入 Qdrant 向量数据库：
1. Claim 短事务：以行锁锁定 Document，仅查询 status='active' 且 vector_status in ('pending', 'failed') 的子块，
   将 Document 与这批子块推进至 indexing 状态，写入当前 operation_id 作为 ownership token 并提交。
2. 事务外执行（在 document:index:{document_id} 命名锁围栏内）：
   - 确保 Qdrant Collection 存在并就绪
   - 按 EMBEDDING_BATCH_SIZE 分批调用 Embedding API，严格校验返回向量数量与维度（EMBEDDING_VECTOR_SIZE）
   - Qdrant Point ID 与 ChildChunk.id 一一对应（整数 ID），实现幂等 upsert
   - 在围栏锁内复核 operation ownership 后写入 Qdrant
3. Finalize 短事务：再次以行锁锁定 Document 与本次子块，复核状态与 ownership，
   将子块 vector_status 置为 indexed，若全部子块均已索引则将 Document 标记为 indexed，
   更新 indexed_at 并释放 ownership。

若失败，由 IndexVectorsCompensator 校验 ownership，在 document:index:{document_id} 命名锁围栏内
从数据库当前 indexing 子块的稳定 ID 独立推导 Qdrant Point 并删除，删除成功后将子块置为 failed 并释放 ownership。
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from app.modules.document.application.dto import IndexVectorsResult
from app.modules.document.application.errors import DocumentApplicationError
from app.modules.document.application.failure_state import (
    IndexFailureStateResult,
    NO_INDEX_FAILURE_STATE_CHANGE,
)
from app.modules.document.application.ports import DocumentApplicationPorts
from app.modules.document.application.settings import (
    DocumentIndexingSettings,
)
from app.modules.document.domain.enums import (
    DocumentLifecycleStatus,
    DocumentStatus,
    DocumentStorageStatus,
)
from app.shared.observability.document_index_logger import DocumentIndexLogger
from app.shared.observability.correlation import DocumentOperationContext
from app.shared.time import now_ms

# 允许执行向量索引的业务生命周期状态集合
INDEXABLE_LIFECYCLE_STATUSES = frozenset(
    {
        DocumentLifecycleStatus.ACTIVE.value,
        DocumentLifecycleStatus.SCHEDULED.value,
    }
)
# 允许被索引领取的子块向量状态集合（仅处理待处理与既往失败的子块）
INDEXABLE_VECTOR_STATUSES = frozenset({"pending", "failed"})


class EmbeddingClient(Protocol):
    """索引编排所需的最小 Embedding 客户端契约协议。"""

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """将文本批次转换为浮点向量列表。

        Args:
            texts: 输入富文本字符串列表。

        Returns:
            二维浮点向量列表。
        """
        ...


class VectorStoreClient(Protocol):
    """索引编排所需的最小向量存储（Qdrant）客户端契约协议。"""

    def ensure_collection(self) -> None:
        """确保 Qdrant 集合已存在且配置正确。"""
        ...

    def upsert_points(self, points: list[Any]) -> None:
        """批量向 Qdrant 集合写入或更新向量点（PointStruct）。

        Args:
            points: 包含 ID、向量与 Payload 的点列表。
        """
        ...

    def delete_points(self, point_ids: list[int]) -> None:
        """根据 Point ID 列表从 Qdrant 集合中物理删除向量点。

        Args:
            point_ids: 待删除的点 ID 列表（与 ChildChunk.id 对应）。
        """
        ...


class IndexingAbortedError(RuntimeError):
    """表示索引执行或完成期间文档/子块状态或 ownership 发生非预期变化，索引结果不得登记。"""


class IndexingExecutionError(RuntimeError):
    """携带失败操作位置和用于诊断的 Point ID 集合的索引执行异常。"""

    def __init__(
        self,
        message: str,
        *,
        operation: str,
        batch_index: int | None,
        batch_size: int | None,
        confirmed_point_ids: tuple[int, ...],
        uncertain_point_ids: tuple[int, ...],
    ) -> None:
        """初始化索引执行异常。

        Args:
            message: 错误描述。
            operation: 发生失败的操作阶段名称（如 'embedding', 'qdrant_upsert'）。
            batch_index: 发生失败的批次序号。
            batch_size: 批次大小。
            confirmed_point_ids: 既往批次已确认写入成功的 Point ID 列表。
            uncertain_point_ids: 本次失败批次处于不确定状态的 Point ID 列表。
        """
        super().__init__(message)
        self.operation = operation
        self.batch_index = batch_index
        self.batch_size = batch_size
        self.confirmed_point_ids = confirmed_point_ids
        self.uncertain_point_ids = uncertain_point_ids


@dataclass(frozen=True)
class IndexingChunkInput:
    """事务外生成向量与构造 Qdrant Point 所需的不可变子块数据快照。

    Attributes:
        chunk_id: 子块自增主键 ID（直接用作 Qdrant Point ID）。
        chunk_code: 子块业务编码。
        embedding_text: 送入 Embedding 模型的富文本正文。
        parent_id: 所属父块 ID。
        doc_id: 所属文档 ID。
        kb_id: 所属知识库 ID。
        domain_code: 业务领域编码。
        business_scene: 业务场景标识。
        chunk_index: 子块序号。
        section_path: 章节层级路径。
        source_row_index: 表格源行号。
    """

    chunk_id: int
    chunk_code: str
    embedding_text: str
    parent_id: int
    doc_id: int
    kb_id: int
    domain_code: str
    business_scene: str | None
    chunk_index: int
    section_path: list[str] | None
    source_row_index: int | None


@dataclass(frozen=True)
class IndexingContext:
    """领取事务提交后传递给事务外索引执行阶段的不可变上下文快照。

    Attributes:
        document_id: 文档 ID。
        source_type: 原始文件类型。
        title: 文档标题。
        original_filename: 原始文件名。
        chunks: 本次待索引的子块快照元组。
        doc_code: 文档业务编码。
        kb_id: 知识库 ID。
        domain_code: 业务领域编码。
        business_scene: 业务场景标识。
        status_before: 领取前状态。
        pending_count: 本批次中 pending 状态子块数。
        retry_count: 本批次中 failed 重试子块数。
        operation_id: 本次操作 ID。
    """

    document_id: int
    source_type: str
    title: str
    original_filename: str | None
    chunks: tuple[IndexingChunkInput, ...]
    doc_code: str | None = None
    kb_id: int | None = None
    domain_code: str | None = None
    business_scene: str | None = None
    status_before: str | None = None
    pending_count: int = 0
    retry_count: int = 0
    operation_id: str = ""


@dataclass(frozen=True)
class IndexingExecutionResult:
    """事务外 Embedding 与 Qdrant upsert 完成后等待数据库登记的执行结果。

    Attributes:
        context: 索引上下文。
        point_ids: 本次已成功写入 Qdrant 的全部 Point ID 元组。
    """

    context: IndexingContext
    point_ids: tuple[int, ...]


@dataclass(frozen=True)
class IndexingCompensationSnapshot:
    """准备补偿阶段捕获的不可变文档与子块快照。

    Attributes:
        document_id: 文档 ID。
        chunk_ids: 处于 indexing 状态的子块 ID 元组。
        status_before: 补偿前文档状态。
    """

    document_id: int
    chunk_ids: tuple[int, ...]
    status_before: str


class IndexVectorsUseCase:
    """在短事务与外部副作用围栏之间编排 Embedding 计算与 Qdrant 索引写入的用例入口。"""

    def __init__(
        self,
        *,
        ports: DocumentApplicationPorts,
        settings: DocumentIndexingSettings,
    ) -> None:
        """初始化向量索引用例。

        Args:
            ports: 外部依赖端口容器。
            settings: 向量批次与维度配置。
        """
        self._ports = ports
        self._settings = settings

    def execute(
        self,
        document_id: int,
        *,
        operation_context: DocumentOperationContext | None = None,
        embedding_client: EmbeddingClient | None = None,
        vector_store: VectorStoreClient | None = None,
    ) -> IndexVectorsResult:
        """同步执行文档向量索引流水线（Claim -> Execute -> Finalize）。

        Args:
            document_id: 待索引的文档 ID。
            operation_context: 可选的操作上下文追踪信息。
            embedding_client: 可选显式指定的 Embedding 客户端替身。
            vector_store: 可选显式指定的 VectorStore 客户端替身。

        Returns:
            索引执行统计 DTO。

        Raises:
            DocumentApplicationError: 状态不合法（409）、未找到（404）或索引异常（500）。
        """
        return _index_document_vectors(
            document_id,
            ports=self._ports,
            settings=self._settings,
            operation_context=operation_context,
            embedding_client=embedding_client,
            vector_store=vector_store,
        )


def _index_document_vectors(
    document_id: int,
    *,
    ports: DocumentApplicationPorts,
    settings: DocumentIndexingSettings,
    operation_context: DocumentOperationContext | None = None,
    embedding_client: EmbeddingClient | None = None,
    vector_store: VectorStoreClient | None = None,
) -> IndexVectorsResult:
    """领取索引任务，在事务外写 Qdrant，再以短事务登记结果。"""
    logger_kwargs = {"document_id": document_id}
    if operation_context is not None:
        logger_kwargs["operation_context"] = operation_context
    index_logger = DocumentIndexLogger(**logger_kwargs)
    operation_id = index_logger.operation_context.operation_id
    context: IndexingContext | None = None
    confirmed_point_ids: tuple[int, ...] = ()
    uncertain_point_ids: tuple[int, ...] = ()
    phase = "claim"

    try:
        # 阶段 1：短事务领取索引权（行锁锁定 Document 与待索引子块，标记为 indexing）
        context = _claim_indexing(
            document_id,
            operation_id=operation_id,
            ports=ports,
        )
        index_logger.claimed(context)

        # 阶段 2：事务外分批调用 Embedding API 并在 document:index:{id} 围栏内 upsert Qdrant
        phase = "execute"
        resolved_embedding_client = (
            embedding_client or ports.embedding_factory()
        )
        resolved_vector_store = vector_store or ports.vector_store_factory()
        execution_result = _execute_indexing(
            context,
            embedding_client=resolved_embedding_client,
            vector_store=resolved_vector_store,
            index_logger=index_logger,
            ports=ports,
            settings=settings,
        )
        confirmed_point_ids = execution_result.point_ids

        # 阶段 3：短事务完成登记（子块标记 indexed，若全完成则 Document 推进至 indexed）
        phase = "finalize"
        response = _complete_indexing(execution_result, ports=ports)
        index_logger.completed(response)
        return response
    except IndexingExecutionError as exc:
        confirmed_point_ids = exc.confirmed_point_ids
        uncertain_point_ids = exc.uncertain_point_ids
        _handle_indexing_failure(
            context=context,
            phase=phase,
            confirmed_point_ids=confirmed_point_ids,
            uncertain_point_ids=uncertain_point_ids,
            error=exc,
            index_logger=index_logger,
            operation=exc.operation,
            batch_index=exc.batch_index,
            batch_size=exc.batch_size,
        )
        raise DocumentApplicationError(
            status_code=500,
            detail="向量索引失败，请稍后重试或联系管理员",
        ) from exc
    except IndexingAbortedError as exc:
        _handle_indexing_failure(
            context=context,
            phase=phase,
            confirmed_point_ids=confirmed_point_ids,
            uncertain_point_ids=uncertain_point_ids,
            error=exc,
            index_logger=index_logger,
        )
        raise DocumentApplicationError(
            status_code=409,
            detail=str(exc),
        ) from exc
    except DocumentApplicationError as exc:
        _handle_indexing_failure(
            context=context,
            phase=phase,
            confirmed_point_ids=confirmed_point_ids,
            uncertain_point_ids=uncertain_point_ids,
            error=exc,
            index_logger=index_logger,
        )
        raise
    except Exception as exc:
        _handle_indexing_failure(
            context=context,
            phase=phase,
            confirmed_point_ids=confirmed_point_ids,
            uncertain_point_ids=uncertain_point_ids,
            error=exc,
            index_logger=index_logger,
        )
        raise DocumentApplicationError(
            status_code=500,
            detail="向量索引失败，请稍后重试或联系管理员",
        ) from exc


def _claim_indexing(
    document_id: int,
    *,
    operation_id: str,
    ports: DocumentApplicationPorts,
) -> IndexingContext:
    """以行锁领取索引权，并提交 Document/Chunk 的 indexing 状态。

    业务规则：
    - 文档状态必须为 chunked 或 failed（409）
    - 无未释放的 active_operation_id（409）
    - 不存在处于 indexing 状态的悬挂子块（409）
    - 仅领取 active 且 vector_status in ('pending', 'failed') 的子块
    - 确保子块与文档知识库、领域编码一致

    Args:
        document_id: 文档 ID。
        operation_id: 操作 ID。
        ports: 端口容器。

    Returns:
        领取成功后的上下文快照。
    """
    with ports.uow_factory() as uow:
        document = uow.documents.get_by_id_for_update(document_id)

        if document is None:
            raise DocumentApplicationError(status_code=404, detail="文档不存在")
        if document.active_operation_id is not None:
            raise DocumentApplicationError(
                status_code=409,
                detail="文档已有未释放的索引 Operation",
            )
        if document.status not in {
            DocumentStatus.CHUNKED.value,
            DocumentStatus.FAILED.value,
        }:
            raise DocumentApplicationError(
                status_code=409,
                detail=f"当前文档状态不允许索引: {document.status}",
            )
        if document.lifecycle_status not in INDEXABLE_LIFECYCLE_STATUSES:
            raise DocumentApplicationError(
                status_code=409,
                detail="失效文档不能索引",
            )
        if document.storage_status != DocumentStorageStatus.ACTIVE.value:
            raise DocumentApplicationError(
                status_code=409,
                detail="文档不在活跃存储区",
            )

        # 检查是否存在上一次未完成的 indexing 悬挂状态
        if uow.child_chunks.exists_by_doc_id_and_vector_status(
            document.id,
            "indexing",
        ):
            raise DocumentApplicationError(
                status_code=409,
                detail="文档存在未完成的索引任务，请先执行恢复操作",
            )

        # 仅查询 active 且待索引或失败的子块
        chunks = uow.child_chunks.list_indexable_by_doc_id(
            document.id,
            set(INDEXABLE_VECTOR_STATUSES),
        )
        if not chunks:
            detail = (
                "文档没有可重试的向量块"
                if document.status == DocumentStatus.FAILED.value
                else "文档没有可索引的子块"
            )
            raise DocumentApplicationError(status_code=409, detail=detail)

        # 严密复核子块与文档的归属权威性
        _validate_indexing_chunk_ownership(document, chunks)

        status_before = document.status
        pending_count = sum(
            chunk.vector_status == "pending" for chunk in chunks
        )
        retry_count = sum(
            chunk.vector_status == "failed" for chunk in chunks
        )
        context = IndexingContext(
            document_id=document.id,
            source_type=document.source_type,
            title=document.title,
            original_filename=document.original_filename,
            chunks=tuple(_to_chunk_input(chunk) for chunk in chunks),
            doc_code=document.doc_code,
            kb_id=document.kb_id,
            domain_code=document.domain_code,
            business_scene=document.business_scene,
            status_before=status_before,
            pending_count=pending_count,
            retry_count=retry_count,
            operation_id=operation_id,
        )
        # 将本次处理的子块批量推进为 indexing 状态
        uow.child_chunks.mark_indexing(chunks)
        document.status = DocumentStatus.INDEXING.value
        document.active_operation_id = operation_id
        uow.flush()
        uow.commit()

    return context


def _execute_indexing(
    context: IndexingContext,
    *,
    embedding_client: EmbeddingClient,
    vector_store: VectorStoreClient,
    index_logger: DocumentIndexLogger | None = None,
    ports: DocumentApplicationPorts,
    settings: DocumentIndexingSettings,
) -> IndexingExecutionResult:
    """在数据库事务外分批生成向量并以稳定 ID upsert Qdrant。

    Args:
        context: 索引上下文。
        embedding_client: Embedding 客户端。
        vector_store: 向量库客户端。
        index_logger: 可选日志记录器。
        ports: 端口容器。
        settings: 索引参数设置。

    Returns:
        写入成功的执行结果。

    Raises:
        IndexingExecutionError: 外部服务调用或校验失败时抛出。
    """
    if settings.embedding_batch_size <= 0:
        raise IndexingExecutionError(
            "EMBEDDING_BATCH_SIZE 必须大于 0",
            operation="vector_validation",
            batch_index=None,
            batch_size=None,
            confirmed_point_ids=(),
            uncertain_point_ids=(),
        )

    confirmed_point_ids: list[int] = []
    try:
        # 确保 Qdrant collection 就绪
        vector_store.ensure_collection()
    except Exception as exc:
        raise IndexingExecutionError(
            "Qdrant Collection 检查失败",
            operation="collection_check",
            batch_index=None,
            batch_size=None,
            confirmed_point_ids=(),
            uncertain_point_ids=(),
        ) from exc

    if index_logger is not None:
        index_logger.collection_ready(
            collection_name=getattr(
                vector_store,
                "collection_name",
                None,
            ),
            vector_size=settings.embedding_vector_size,
        )

    # 按照 EMBEDDING_BATCH_SIZE 分批处理
    for start in range(
        0,
        len(context.chunks),
        settings.embedding_batch_size,
    ):
        batch = context.chunks[
            start:start + settings.embedding_batch_size
        ]
        batch_index = start // settings.embedding_batch_size + 1
        batch_point_ids = [chunk.chunk_id for chunk in batch]
        embedding_started_at_ms = now_ms()
        if index_logger is not None:
            embedding_started_at_ms = index_logger.embedding_batch_started(
                batch_index=batch_index,
                batch_size=len(batch),
                embedding_model=settings.embedding_model_name,
            )
        try:
            # 外部 Embedding API 调用
            vectors = embedding_client.embed_texts(
                [chunk.embedding_text for chunk in batch]
            )
        except Exception as exc:
            raise IndexingExecutionError(
                "Embedding 调用失败",
                operation="embedding",
                batch_index=batch_index,
                batch_size=len(batch),
                confirmed_point_ids=tuple(confirmed_point_ids),
                uncertain_point_ids=(),
            ) from exc

        try:
            # 严格校验向量数量与维度
            _validate_vectors(batch, vectors, settings=settings)
            # 构造与 ChildChunk.id 一一对应的 Qdrant Point
            points = [
                _build_point(context, chunk, vector, ports=ports)
                for chunk, vector in zip(batch, vectors)
            ]
        except Exception as exc:
            raise IndexingExecutionError(
                "Embedding 结果校验失败",
                operation="vector_validation",
                batch_index=batch_index,
                batch_size=len(batch),
                confirmed_point_ids=tuple(confirmed_point_ids),
                uncertain_point_ids=(),
            ) from exc

        if index_logger is not None:
            index_logger.embedding_batch_completed(
                batch_index=batch_index,
                input_count=len(batch),
                vectors=vectors,
                started_at_ms=embedding_started_at_ms,
            )

        qdrant_started_at_ms = now_ms()
        try:
            # 在 MySQL 命名锁 document:index:{id} 围栏内写入 Qdrant
            with ports.external_effect_fence.hold(
                _index_effect_fence_key(context.document_id)
            ):
                _assert_indexing_owned(context, ports=ports)
                vector_store.upsert_points(points)
        except IndexingAbortedError:
            raise
        except Exception as exc:
            raise IndexingExecutionError(
                "Qdrant Upsert 失败",
                operation="qdrant_upsert",
                batch_index=batch_index,
                batch_size=len(batch),
                confirmed_point_ids=tuple(confirmed_point_ids),
                uncertain_point_ids=tuple(batch_point_ids),
            ) from exc

        confirmed_point_ids.extend(batch_point_ids)
        if index_logger is not None:
            index_logger.qdrant_batch_completed(
                batch_index=batch_index,
                point_count=len(batch_point_ids),
                started_at_ms=qdrant_started_at_ms,
            )

    return IndexingExecutionResult(
        context=context,
        point_ids=tuple(confirmed_point_ids),
    )


def _complete_indexing(
    result: IndexingExecutionResult,
    *,
    ports: DocumentApplicationPorts,
) -> IndexVectorsResult:
    """在短事务中复核文档和子块状态，并原子登记 indexed。

    Args:
        result: 索引执行结果。
        ports: 端口容器。

    Returns:
        索引结果统计。
    """
    context = result.context
    chunk_ids = _context_chunk_ids(context)
    with ports.uow_factory() as uow:
        document = uow.documents.get_by_id_for_update(context.document_id)

        if document is None:
            raise DocumentApplicationError(status_code=404, detail="文档不存在")
        if document.status != DocumentStatus.INDEXING.value:
            raise IndexingAbortedError(
                f"文档索引状态已经变化: {document.status}"
            )
        if document.active_operation_id != context.operation_id:
            raise IndexingAbortedError("当前索引 Operation 已被其他执行接管")
        if document.lifecycle_status not in INDEXABLE_LIFECYCLE_STATUSES:
            raise IndexingAbortedError("文档索引期间已经失效")
        if document.storage_status != DocumentStorageStatus.ACTIVE.value:
            raise IndexingAbortedError("文档已进入归档流程")

        # 锁定并复核本次处理的子块
        chunks = uow.child_chunks.list_by_ids_for_update(
            document.id,
            chunk_ids,
        )
        if len(chunks) != len(chunk_ids) or any(
            chunk.vector_status != "indexing" for chunk in chunks
        ):
            raise IndexingAbortedError("索引子块状态已经变化")

        # 将这批子块标记为 indexed
        uow.child_chunks.mark_indexed_many(chunks)

        # 检查是否还有其他未完成索引的子块
        remaining_count = (
            uow.child_chunks.count_active_not_indexed_by_doc_id(document.id)
        )
        if remaining_count > 0:
            raise IndexingAbortedError("文档仍存在未完成索引的子块")

        # 全部子块已完成索引：推进 Document 为 indexed 并清空 active_operation_id
        document.status = DocumentStatus.INDEXED.value
        document.active_operation_id = None
        document.indexed_at = datetime.now()
        uow.flush()
        response = IndexVectorsResult(
            document_id=document.id,
            total_chunks=len(chunks),
            indexed_chunks=len(chunks),
            failed_chunks=0,
            status="success",
        )
        uow.commit()

    return response


class IndexVectorsCompensator:
    """在命名锁围栏内复核 Document 后按稳定 Chunk ID 删除 Qdrant Point 并标记失败的补偿器。

    由 Task Runtime 在 attempt 失败或超时后驱动调用。
    补偿流程：
    1. Prepare 短事务：查询当前仍由该 operation_id 持有的 indexing 子块并构造快照，将 Document 置为 failed。
    2. 在 document:index:{document_id} 命名锁围栏内调用 Qdrant 删除这些 Point。
    3. Complete 短事务：Qdrant 删除成功后，将子块置为 failed 并清空 Document.active_operation_id。
    若 Qdrant 删除抛错，保留 ownership 禁止新 attempt 接管。
    """

    def __init__(self, *, ports: DocumentApplicationPorts) -> None:
        """初始化向量索引补偿器。

        Args:
            ports: 端口容器。
        """
        self._ports = ports

    def compensate(
        self,
        *,
        document_id: int,
        operation_id: str,
        vector_store: VectorStoreClient | None = None,
        index_logger: DocumentIndexLogger | None = None,
    ) -> IndexFailureStateResult:
        """执行向量索引副作用补偿。

        Args:
            document_id: 文档 ID。
            operation_id: 需补偿的操作 ID。
            vector_store: 可选向量库客户端。
            index_logger: 可选日志记录器。

        Returns:
            状态变更快照。
        """
        snapshot = self._prepare(
            document_id=document_id,
            operation_id=operation_id,
        )
        if snapshot is None:
            return NO_INDEX_FAILURE_STATE_CHANGE

        with self._ports.external_effect_fence.hold(
            _index_effect_fence_key(document_id)
        ):
            compensation_point_ids = snapshot.chunk_ids
            if compensation_point_ids:
                resolved_vector_store = (
                    vector_store or self._ports.vector_store_factory()
                )
                started_at_ms = now_ms()
                if index_logger is not None:
                    started_at_ms = index_logger.compensation_started(
                        confirmed_point_count=len(compensation_point_ids),
                        uncertain_point_count=0,
                    )
                try:
                    # 从 Qdrant 中物理删除对应的向量点
                    resolved_vector_store.delete_points(
                        list(compensation_point_ids)
                    )
                except Exception as exc:
                    if index_logger is not None:
                        index_logger.compensation_failed(
                            error=exc,
                            confirmed_point_count=len(compensation_point_ids),
                            uncertain_point_count=0,
                            point_count=len(compensation_point_ids),
                            started_at_ms=started_at_ms,
                        )
                    raise
                if index_logger is not None:
                    index_logger.compensation_completed(
                        requested_point_count=len(compensation_point_ids),
                        started_at_ms=started_at_ms,
                    )

            # Qdrant 删除成功后完成数据库子块状态更新与 ownership 释放
            return self._complete(
                snapshot=snapshot,
                operation_id=operation_id,
                chunk_ids=snapshot.chunk_ids,
            )

    def _prepare(
        self,
        *,
        document_id: int,
        operation_id: str,
    ) -> IndexingCompensationSnapshot | None:
        """短事务捕获当前 operation_id 持有的 indexing 子块，将 Document 置为 failed 并提交。"""
        with self._ports.uow_factory() as uow:
            document = uow.documents.get_by_id_for_update(document_id)
            if (
                document is None
                or document.active_operation_id != operation_id
                or document.status
                not in {
                    DocumentStatus.INDEXING.value,
                    DocumentStatus.FAILED.value,
                }
            ):
                return None
            status_before = document.status
            chunks = uow.child_chunks.list_indexable_by_doc_id(
                document.id,
                {"indexing"},
            )
            snapshot = IndexingCompensationSnapshot(
                document_id=document.id,
                chunk_ids=tuple(chunk.id for chunk in chunks),
                status_before=status_before,
            )
            if document.status == DocumentStatus.INDEXING.value:
                document.status = DocumentStatus.FAILED.value
                uow.flush()
                uow.commit()
            return snapshot

    def _complete(
        self,
        *,
        snapshot: IndexingCompensationSnapshot,
        operation_id: str,
        chunk_ids: tuple[int, ...],
    ) -> IndexFailureStateResult:
        """短事务将子块置为 failed 并释放 active_operation_id。"""
        with self._ports.uow_factory() as uow:
            document = uow.documents.get_by_id_for_update(
                snapshot.document_id
            )
            if (
                document is None
                or document.active_operation_id != operation_id
                or document.status != DocumentStatus.FAILED.value
            ):
                return NO_INDEX_FAILURE_STATE_CHANGE
            chunks = uow.child_chunks.list_by_ids_for_update(
                document.id,
                chunk_ids,
            )
            chunk_statuses_before = [
                chunk.vector_status for chunk in chunks
            ]
            uow.child_chunks.mark_failed(chunks)
            chunk_state_updated_count = sum(
                before != chunk.vector_status
                for before, chunk in zip(chunk_statuses_before, chunks)
            )
            document.active_operation_id = None
            uow.flush()
            uow.commit()
            return IndexFailureStateResult(
                document_state_updated=(
                    snapshot.status_before != document.status
                ),
                chunk_state_updated_count=chunk_state_updated_count,
                status_before=snapshot.status_before,
                status_after=document.status,
            )


def _handle_indexing_failure(
    *,
    context: IndexingContext | None,
    phase: str,
    confirmed_point_ids: tuple[int, ...],
    uncertain_point_ids: tuple[int, ...],
    error: Exception,
    index_logger: DocumentIndexLogger,
    operation: str | None = None,
    batch_index: int | None = None,
    batch_size: int | None = None,
) -> None:
    """记录索引失败诊断日志；claim 提交后的正式补偿由 Task Runtime 编排驱动。"""
    index_logger.failed(
        error=error,
        phase=phase,
        context=context,
        document_state_updated=False,
        chunk_state_updated_count=0,
        operation=operation,
        batch_index=batch_index,
        batch_size=batch_size,
        confirmed_point_count=len(confirmed_point_ids),
        uncertain_point_count=len(uncertain_point_ids),
    )


def _validate_indexing_chunk_ownership(document, chunks) -> None:
    """确保待索引子块与 Document 的权威归属字段（doc_id, kb_id, domain_code）一致。"""
    if any(chunk.doc_id != document.id for chunk in chunks):
        raise RuntimeError("索引子块与文档关联不一致")
    if any(chunk.kb_id != document.kb_id for chunk in chunks):
        raise RuntimeError("索引子块与文档知识库不一致")
    if any(chunk.domain_code != document.domain_code for chunk in chunks):
        raise RuntimeError("索引子块与文档领域编码不一致")


def _to_chunk_input(chunk) -> IndexingChunkInput:
    """把 ORM 子块转换为事务外使用的不可变快照对象。"""
    return IndexingChunkInput(
        chunk_id=chunk.id,
        chunk_code=chunk.chunk_code,
        embedding_text=chunk.embedding_text,
        parent_id=chunk.parent_id,
        doc_id=chunk.doc_id,
        kb_id=chunk.kb_id,
        domain_code=chunk.domain_code,
        business_scene=chunk.business_scene,
        chunk_index=chunk.chunk_index,
        section_path=(
            list(chunk.section_path)
            if chunk.section_path is not None
            else None
        ),
        source_row_index=chunk.source_row_index,
    )


def _validate_vectors(
    chunks: tuple[IndexingChunkInput, ...],
    vectors: list[list[float]],
    *,
    settings: DocumentIndexingSettings,
) -> None:
    """严格校验一个批次的 Embedding 返回向量数量与维度大小。"""
    if len(vectors) != len(chunks):
        raise RuntimeError("Embedding 返回数量不一致")
    if any(
        len(vector) != settings.embedding_vector_size
        for vector in vectors
    ):
        raise RuntimeError("Embedding 维度不一致")


def _build_point(
    context: IndexingContext,
    chunk: IndexingChunkInput,
    vector: list[float],
    *,
    ports: DocumentApplicationPorts,
) -> Any:
    """使用 ChildChunk 主键构造可幂等 upsert 的 Qdrant PointStruct 向量点。"""
    return ports.point_factory(
        id=chunk.chunk_id,
        vector=vector,
        payload={
            "document_id": chunk.doc_id,
            "kb_id": chunk.kb_id,
            "parent_block_id": chunk.parent_id,
            "child_chunk_id": chunk.chunk_id,
            "chunk_index": chunk.chunk_index,
            "chunk_code": chunk.chunk_code,
            "section_path": chunk.section_path,
            "source_row_index": chunk.source_row_index,
            "domain_code": chunk.domain_code,
            "business_scene": chunk.business_scene,
            "source_type": context.source_type,
            "title": context.title,
            "original_filename": context.original_filename,
            "operation_id": context.operation_id,
        },
    )


def _assert_indexing_owned(
    context: IndexingContext,
    *,
    ports: DocumentApplicationPorts,
) -> None:
    """在外部写入围栏内复核 Operation 仍持有 Document 处理权（防 stale attempt 冲突）。"""
    with ports.uow_factory() as uow:
        document = uow.documents.get_by_id_for_update(context.document_id)
        if (
            document is None
            or document.status != DocumentStatus.INDEXING.value
            or document.active_operation_id != context.operation_id
        ):
            raise IndexingAbortedError("当前索引 Operation ownership 已失效")


def _index_effect_fence_key(document_id: int) -> str:
    """获取文档向量索引外部副作用的 MySQL 命名锁唯一键名。"""
    return f"document:index:{document_id}"


def _context_chunk_ids(context: IndexingContext) -> tuple[int, ...]:
    """提取上下文中的全部子块主键 ID 元组。"""
    return tuple(chunk.chunk_id for chunk in context.chunks)
