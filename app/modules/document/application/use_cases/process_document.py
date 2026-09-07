"""处理文档应用用例：以短事务编排领取、执行与结果登记。

流水线阶段 2：Process
负责将原始文档转换为标准化清洗文本（cleaned_text），流程遵循严格的三段式边界：
1. Claim 短事务：以行锁锁定 Document，校验前置技术状态（uploaded/failed）与三状态轴，
   将状态推进为 processing 并写入当前 operation_id 作为排他 ownership token。
2. 事务外执行（在 document:process:{document_id} 命名锁围栏内）：
   - 复核 operation ownership
   - 如需外部转换（PDF/Word/PPT）则调用 Docling 生成 Markdown 二级文本
   - 调用对应格式的文本清洗处理器（Processor）清洗为标准文本
   - 将临时 staging 产物提升（promote）为 operation-scoped 正式目录
3. Finalize 短事务：再次以行锁复核状态与 operation_id，将旧 active 产物标记为 superseded，
   持久化新产物记录，更新 Document.cleaned_uri 并推进状态至 processed，释放 active_operation_id。

若失败，由 ProcessDocumentCompensator 校验 ownership 并清理当前 operation_id 的目录，将 Document 置为 failed。
"""

import shutil
from dataclasses import dataclass, replace
from pathlib import Path
from uuid import uuid4

from app.modules.document.application.dto import (
    DocumentArtifactCreate,
    ProcessDocumentResult,
)
from app.modules.document.application.errors import DocumentApplicationError
from app.modules.document.application.failure_state import (
    FailureStateResult,
    NO_FAILURE_STATE_CHANGE,
)
from app.modules.document.application.ports import DocumentApplicationPorts
from app.modules.document.application.settings import (
    DocumentProcessingSettings,
)
from app.modules.document.application.use_cases.prepare_document_source import (
    PendingArtifact,
    PreparedProcessSource,
    prepare_process_source,
)
from app.modules.document.domain.enums import (
    DocumentLifecycleStatus,
    DocumentStatus,
    DocumentStorageStatus,
)
from app.shared.observability.document_process_logger import (
    DocumentProcessLogger,
)
from app.shared.observability.correlation import DocumentOperationContext


# 允许执行处理的业务生命周期状态集合
PROCESSABLE_LIFECYCLE_STATUSES = frozenset(
    {
        DocumentLifecycleStatus.ACTIVE.value,
        DocumentLifecycleStatus.SCHEDULED.value,
    }
)


class ProcessingAbortedError(RuntimeError):
    """表示任务执行或完成期间检测到文档状态/ownership 发生非预期变化，处理结果必须丢弃。"""


@dataclass(frozen=True)
class ProcessingContext:
    """领取事务提交后传递给事务外执行阶段的不可变文档上下文快照。

    Attributes:
        document_id: 文档自增主键 ID。
        doc_code: 文档业务编码。
        kb_id: 所属知识库 ID。
        domain_code: 业务领域编码。
        business_scene: 业务场景标识。
        source_type: 原始文件类型。
        source_path: 原始文件物理路径。
        created_by_actor_code: 创建人编码。
        status_before: 领取前的原始状态。
        operation_id: 本次处理操作的唯一 operation 标识（兼作 ownership token）。
    """

    document_id: int
    doc_code: str
    kb_id: int
    domain_code: str | None
    business_scene: str | None
    source_type: str
    source_path: Path
    created_by_actor_code: str | None
    status_before: str
    operation_id: str


@dataclass(frozen=True)
class ProcessingExecutionResult:
    """事务外处理执行完成、等待在完成短事务中登记的内存结果结构。

    Attributes:
        document_id: 文档 ID。
        operation_id: 本次操作 ID。
        cleaned_path: 清洗后生成的标准化文本文件绝对路径。
        prepared_source: 预备阶段生成的源文件及二级产物信息。
        cleaned_artifact: 清洗后文本产物的待登记元数据。
    """

    document_id: int
    operation_id: str
    cleaned_path: Path
    prepared_source: PreparedProcessSource
    cleaned_artifact: PendingArtifact

    @property
    def secondary_artifact(self) -> PendingArtifact | None:
        """获取可选的二级文本（Markdown）待登记产物。"""
        return self.prepared_source.secondary_artifact


class ProcessDocumentUseCase:
    """在短事务与外部副作用围栏之间编排文档格式转换与文本清洗的用例入口。"""

    def __init__(
        self,
        *,
        ports: DocumentApplicationPorts,
        settings: DocumentProcessingSettings,
    ) -> None:
        """初始化文档处理用例。

        Args:
            ports: 外部依赖端口容器。
            settings: 文档处理存储路径配置。
        """
        self._ports = ports
        self._settings = settings

    def execute(
        self,
        document_id: int,
        *,
        operation_context: DocumentOperationContext | None = None,
    ) -> ProcessDocumentResult:
        """同步执行文档处理流水线（Claim -> Execute/Promote -> Finalize）。

        Args:
            document_id: 待处理的文档 ID。
            operation_context: 可选的操作上下文追踪信息。

        Returns:
            处理结果 DTO（包含 cleaned_uri 等）。

        Raises:
            DocumentApplicationError: 状态不合法（409）、未找到（404）或执行失败（500）。
        """
        return _process_document(
            document_id,
            ports=self._ports,
            settings=self._settings,
            operation_context=operation_context,
        )


def _process_document(
    document_id: int,
    *,
    ports: DocumentApplicationPorts,
    settings: DocumentProcessingSettings,
    operation_context: DocumentOperationContext | None = None,
) -> ProcessDocumentResult:
    """领取文档后在事务外处理，并以独立短事务登记结果或失败。"""
    logger_kwargs = {"document_id": document_id}
    if operation_context is not None:
        logger_kwargs["operation_context"] = operation_context
    process_logger = DocumentProcessLogger(**logger_kwargs)
    operation_id = process_logger.operation_context.operation_id
    context: ProcessingContext | None = None
    phase = "claim"
    try:
        # 阶段 1：短事务领取处理权（行锁 + 校验三状态轴 + 写入 processing 与 operation_id）
        context = _claim_processing(
            document_id,
            operation_id=operation_id,
            ports=ports,
        )
        process_logger.claimed(context)

        # 阶段 2：在 MySQL document:process:{document_id} 命名锁围栏内执行文件副作用
        with ports.external_effect_fence.hold(
            _process_effect_fence_key(document_id)
        ):
            # 锁内复核 operation ownership，防止并发 stale attempt 产生冲突
            _assert_processing_owned(context, ports=ports)
            phase = "execute"
            execution_result = _execute_processing(
                context,
                ports=ports,
                settings=settings,
            )

            # 将 staging 临时产物原子提升至正式 operation-scoped 目录
            phase = "promote"
            execution_result = _promote_processing_artifacts(
                execution_result,
                settings=settings,
            )

        # 阶段 3：短事务完成登记（supersede 旧产物、创建新产物、推进至 processed、清空 ownership）
        phase = "finalize"
        response = _complete_processing(execution_result, ports=ports)
        process_logger.completed(
            processed_source_type=(
                execution_result.cleaned_artifact.artifact_format
            ),
            cleaned_uri=response.cleaned_uri,
        )
        return response
    except ProcessingAbortedError as exc:
        process_logger.failed(
            error=exc,
            phase=phase,
            context=context,
            state_updated=False,
            status_before=None,
            status_after=None,
        )
        raise DocumentApplicationError(
            status_code=409,
            detail=str(exc),
        ) from exc
    except DocumentApplicationError as exc:
        process_logger.failed(
            error=exc,
            phase=phase,
            context=context,
            state_updated=False,
            status_before=None,
            status_after=None,
        )
        raise
    except Exception as exc:
        process_logger.failed(
            error=exc,
            phase=phase,
            context=context,
            state_updated=False,
            status_before=None,
            status_after=None,
        )
        raise DocumentApplicationError(
            status_code=500,
            detail="文档处理失败，请稍后重试或联系管理员",
        ) from exc


def _claim_processing(
    document_id: int,
    *,
    operation_id: str,
    ports: DocumentApplicationPorts,
) -> ProcessingContext:
    """以行锁领取处理权，并立即提交 processing 状态。

    严格复核：
    - 文档必须存在（404）
    - 无未释放的 active_operation_id（409）
    - 文档状态必须为 uploaded 或 failed（409）
    - 业务生命周期必须为 active 或 scheduled（409）
    - 存储状态必须为 active（409）

    Args:
        document_id: 文档 ID。
        operation_id: 本次操作唯一 ID。
        ports: 端口容器。

    Returns:
        领取成功后的不可变快照。
    """
    with ports.uow_factory() as uow:
        document = uow.documents.get_by_id_for_update(document_id)

        if document is None:
            raise DocumentApplicationError(status_code=404, detail="文档不存在")
        if document.active_operation_id is not None:
            raise DocumentApplicationError(
                status_code=409,
                detail="文档已有未释放的处理 Operation",
            )
        if document.status not in {
            DocumentStatus.UPLOADED.value,
            DocumentStatus.FAILED.value,
        }:
            raise DocumentApplicationError(
                status_code=409,
                detail=f"当前文档状态不允许处理: {document.status}",
            )
        if document.lifecycle_status not in PROCESSABLE_LIFECYCLE_STATUSES:
            raise DocumentApplicationError(
                status_code=409,
                detail="失效文档不能处理",
            )
        if document.storage_status != DocumentStorageStatus.ACTIVE.value:
            raise DocumentApplicationError(
                status_code=409,
                detail="文档不在活跃存储区",
            )

        status_before = document.status
        context = ProcessingContext(
            document_id=document.id,
            doc_code=document.doc_code,
            kb_id=document.kb_id,
            domain_code=document.domain_code,
            business_scene=document.business_scene,
            source_type=document.source_type,
            source_path=Path(document.source_uri),
            created_by_actor_code=document.created_by_actor_code,
            status_before=status_before,
            operation_id=operation_id,
        )
        document.status = DocumentStatus.PROCESSING.value
        document.active_operation_id = operation_id
        uow.flush()
        uow.commit()

    return context


def _execute_processing(
    context: ProcessingContext,
    *,
    ports: DocumentApplicationPorts,
    settings: DocumentProcessingSettings,
) -> ProcessingExecutionResult:
    """在数据库事务外执行源文件检查、格式转换、文本清洗和文件元数据计算。

    Args:
        context: 领取上下文。
        ports: 端口容器。
        settings: 处理配置。

    Returns:
        事务外执行结果。
    """
    if not context.source_path.exists():
        raise DocumentApplicationError(
            status_code=404,
            detail=f"原始文件不存在: {context.source_path}",
        )
    if not context.source_path.is_file():
        raise DocumentApplicationError(
            status_code=400,
            detail=f"原始路径不是有效文件: {context.source_path}",
        )

    # 在 staging/{operation_id}/ 临时目录中执行转换与清洗
    operation_dir = _processing_operation_dir(
        settings,
        context.operation_id,
    )
    prepared_source = prepare_process_source(
        context,
        ports=ports,
        settings=settings,
        output_dir=operation_dir,
    )
    operation_dir.mkdir(parents=True, exist_ok=True)
    cleaned_filename = (
        f"{context.doc_code}.cleaned.{prepared_source.source_type}"
    )
    cleaned_path = operation_dir / cleaned_filename
    processor = ports.processor_factory(prepared_source.source_type)
    process_result = processor.process(
        source_path=prepared_source.source_path,
        cleaned_path=cleaned_path,
    )

    cleaned_artifact = PendingArtifact(
        artifact_type="cleaned_text",
        artifact_role="process_output",
        artifact_format=process_result.source_type,
        artifact_uri=str(cleaned_path),
        artifact_hash=ports.calculate_file_hash(cleaned_path),
        provider=None,
        processor=processor.__class__.__name__,
        file_size=cleaned_path.stat().st_size,
        char_count=process_result.char_count,
        line_count=process_result.line_count,
        metadata=process_result.metadata,
    )
    return ProcessingExecutionResult(
        document_id=context.document_id,
        operation_id=context.operation_id,
        cleaned_path=cleaned_path,
        prepared_source=prepared_source,
        cleaned_artifact=cleaned_artifact,
    )


def _assert_processing_owned(
    context: ProcessingContext,
    *,
    ports: DocumentApplicationPorts,
) -> None:
    """在文件副作用围栏内复核当前 Operation 仍持有文档处理权（防 stale attempt 接管）。

    Args:
        context: 处理上下文。
        ports: 端口容器。

    Raises:
        ProcessingAbortedError: 当 ownership 失效、状态改变或文档已进入归档/失效时抛出。
    """
    with ports.uow_factory() as uow:
        document = uow.documents.get_by_id_for_update(context.document_id)
        if (
            document is None
            or document.status != DocumentStatus.PROCESSING.value
            or document.active_operation_id != context.operation_id
        ):
            raise ProcessingAbortedError("当前处理 Operation ownership 已失效")
        if document.lifecycle_status not in PROCESSABLE_LIFECYCLE_STATUSES:
            raise ProcessingAbortedError("文档处理期间已经失效")
        if document.storage_status != DocumentStorageStatus.ACTIVE.value:
            raise ProcessingAbortedError("文档已进入归档流程")


def _complete_processing(
    result: ProcessingExecutionResult,
    *,
    ports: DocumentApplicationPorts,
) -> ProcessDocumentResult:
    """在短事务中复核状态、登记新 Artifact、标记旧产物 superseded 并推进状态至 processed。

    Args:
        result: 处理执行结果。
        ports: 端口容器。

    Returns:
        完成响应。
    """
    with ports.uow_factory() as uow:
        document = uow.documents.get_by_id_for_update(result.document_id)

        if document is None:
            raise DocumentApplicationError(status_code=404, detail="文档不存在")
        if document.status != DocumentStatus.PROCESSING.value:
            raise ProcessingAbortedError(
                f"文档处理状态已经变化: {document.status}"
            )
        if document.active_operation_id != result.operation_id:
            raise ProcessingAbortedError("当前处理 Operation 已被其他执行接管")
        if document.lifecycle_status not in PROCESSABLE_LIFECYCLE_STATUSES:
            raise ProcessingAbortedError("文档处理期间已经失效")
        if document.storage_status != DocumentStorageStatus.ACTIVE.value:
            raise ProcessingAbortedError("文档已进入归档流程")

        # 登记可选的 Docling 二级文本产物
        if result.secondary_artifact is not None:
            _persist_secondary_artifact(
                uow=uow,
                document=document,
                artifact=result.secondary_artifact,
            )
        # 登记本次生成的 cleaned 文本产物
        _persist_cleaned_artifact(
            uow=uow,
            document=document,
            artifact=result.cleaned_artifact,
        )

        # 更新文档主表 cleaned_uri 并推进状态为 processed，释放 active_operation_id
        document.cleaned_uri = str(result.cleaned_path)
        document.status = DocumentStatus.PROCESSED.value
        document.active_operation_id = None
        uow.flush()
        response = ProcessDocumentResult(
            document_id=document.id,
            doc_code=document.doc_code,
            source_type=document.source_type,
            source_uri=document.source_uri,
            cleaned_uri=document.cleaned_uri,
            status=document.status,
        )
        uow.commit()

    return response


def _promote_processing_artifacts(
    result: ProcessingExecutionResult,
    *,
    settings: DocumentProcessingSettings,
) -> ProcessingExecutionResult:
    """把 operation staging 临时产物原子提升（移动）到正式的 operation-scoped 存储目录。

    Args:
        result: 原始执行结果。
        settings: 处理路径配置。

    Returns:
        更新了物理路径后的执行结果。
    """
    cleaned_dir = _operation_scoped_dir(
        settings.cleaned_storage_dir,
        result.operation_id,
    )
    cleaned_path = _promote_file(result.cleaned_path, cleaned_dir)
    cleaned_artifact = replace(
        result.cleaned_artifact,
        artifact_uri=str(cleaned_path),
    )

    prepared_source = result.prepared_source
    if result.secondary_artifact is not None:
        secondary_dir = _operation_scoped_dir(
            settings.secondary_text_storage_dir,
            result.operation_id,
        )
        secondary_path = _promote_file(
            prepared_source.source_path,
            secondary_dir,
        )
        prepared_source = replace(
            prepared_source,
            source_path=secondary_path,
            secondary_artifact=replace(
                result.secondary_artifact,
                artifact_uri=str(secondary_path),
            ),
        )

    # 清理 staging 下的临时目录
    operation_dir = _processing_operation_dir(settings, result.operation_id)
    if operation_dir.exists():
        shutil.rmtree(operation_dir)

    return replace(
        result,
        cleaned_path=cleaned_path,
        prepared_source=prepared_source,
        cleaned_artifact=cleaned_artifact,
    )


def _promote_file(source_path: Path, destination_dir: Path) -> Path:
    """将源文件原子移动到目标目录中。

    Args:
        source_path: 源文件路径。
        destination_dir: 目标目录。

    Returns:
        移动后的目标文件 Path。
    """
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination_path = destination_dir / source_path.name
    if destination_path.exists():
        raise RuntimeError(f"正式产物路径已存在: {destination_path}")
    shutil.move(str(source_path), str(destination_path))
    return destination_path


def _fail_processing(
    document_id: int,
    error: Exception,
    *,
    operation_id: str,
    ports: DocumentApplicationPorts,
) -> FailureStateResult:
    """条件更新（CAS）：仅当仍由指定 operation_id 持有且处于 processing 状态时，标记为 failed。

    Args:
        document_id: 文档 ID。
        error: 导致失败的异常。
        operation_id: 操作 ID。
        ports: 端口容器。

    Returns:
        状态变更快照。
    """
    del error
    with ports.uow_factory() as uow:
        document = uow.documents.get_by_id_for_update(document_id)
        if document is None:
            return NO_FAILURE_STATE_CHANGE
        status_before = document.status
        if document.active_operation_id != operation_id:
            return FailureStateResult(
                state_updated=False,
                status_before=status_before,
                status_after=document.status,
            )
        if document.status == DocumentStatus.PROCESSING.value:
            document.status = DocumentStatus.FAILED.value
            uow.flush()
            uow.commit()
            return FailureStateResult(
                state_updated=True,
                status_before=status_before,
                status_after=document.status,
            )
        return FailureStateResult(
            state_updated=False,
            status_before=status_before,
            status_after=document.status,
        )


class ProcessDocumentCompensator:
    """按 operation-scoped 目录边界补偿文档处理副作用的确定性补偿器。

    由 Task Runtime 在 attempt 失败或超时后驱动调用。
    补偿流程：
    1. 校验文档是否仍由当前 operation_id 持有。
    2. 获取 document:process:{document_id} 命名锁围栏。
    3. 物理删除 staging 与正式 storage 下属于该 operation_id 的产物目录。
    4. 释放 Document.active_operation_id，完成补偿。
    """

    def __init__(
        self,
        *,
        ports: DocumentApplicationPorts,
        settings: DocumentProcessingSettings,
    ) -> None:
        """初始化补偿器。

        Args:
            ports: 端口容器。
            settings: 存储路径配置。
        """
        self._ports = ports
        self._settings = settings

    def compensate(
        self,
        *,
        document_id: int,
        operation_id: str,
    ) -> FailureStateResult:
        """执行处理阶段副作用的恢复与补偿。

        Args:
            document_id: 文档 ID。
            operation_id: 需补偿的操作 ID。

        Returns:
            状态变更结果。
        """
        failure_result = _fail_processing(
            document_id,
            RuntimeError("文档处理 Operation 需要补偿"),
            operation_id=operation_id,
            ports=self._ports,
        )
        with self._ports.external_effect_fence.hold(
            _process_effect_fence_key(document_id)
        ):
            with self._ports.uow_factory() as uow:
                document = uow.documents.get_by_id_for_update(document_id)
                owns_operation = (
                    document is not None
                    and document.active_operation_id == operation_id
                    and document.status == DocumentStatus.FAILED.value
                )
            if not owns_operation:
                return failure_result

            # 收集该 operation_id 下的所有可能产物目录并清理
            operation_dirs = tuple(
                dict.fromkeys(
                    (
                        _processing_operation_dir(
                            self._settings,
                            operation_id,
                        ),
                        _operation_scoped_dir(
                            self._settings.cleaned_storage_dir,
                            operation_id,
                        ),
                        _operation_scoped_dir(
                            self._settings.secondary_text_storage_dir,
                            operation_id,
                        ),
                    )
                )
            )
            for operation_dir in operation_dirs:
                if operation_dir.exists():
                    shutil.rmtree(operation_dir)

            # 释放 active_operation_id token
            with self._ports.uow_factory() as uow:
                document = uow.documents.get_by_id_for_update(document_id)
                if (
                    document is None
                    or document.active_operation_id != operation_id
                ):
                    return failure_result
                document.active_operation_id = None
                uow.flush()
                uow.commit()
        return failure_result


def _processing_operation_dir(
    settings: DocumentProcessingSettings,
    operation_id: str,
) -> Path:
    """返回不能逃逸 staging 根目录的 Operation 临时产物目录。"""
    return _operation_scoped_dir(settings.staging_storage_dir, operation_id)


def _process_effect_fence_key(document_id: int) -> str:
    """获取文档处理外部副作用的 MySQL 命名锁唯一键名。"""
    return f"document:process:{document_id}"


def _operation_scoped_dir(root: Path, operation_id: str) -> Path:
    """返回安全限定在指定根目录下的 Operation 目录（防止路径穿越攻击）。"""
    if (
        not operation_id
        or Path(operation_id).name != operation_id
        or operation_id in {".", ".."}
    ):
        raise ValueError("operation_id 不能用于文件路径")
    return root / operation_id


def _persist_secondary_artifact(*, uow, document, artifact: PendingArtifact) -> None:
    """登记本次 Docling 二级文本产物，并将同角色的旧 active 产物标记为 superseded。"""
    artifact_code = Path(artifact.artifact_uri).stem
    _persist_artifact(
        uow=uow,
        document=document,
        artifact=artifact,
        artifact_code=artifact_code,
    )


def _persist_cleaned_artifact(*, uow, document, artifact: PendingArtifact) -> None:
    """登记本次 cleaned 文本产物，并将旧 active 产物标记为 superseded。"""
    _persist_artifact(
        uow=uow,
        document=document,
        artifact=artifact,
        artifact_code=_generate_cleaned_artifact_code(
            document.doc_code,
            artifact.artifact_format,
        ),
    )


def _persist_artifact(
    *,
    uow,
    document,
    artifact: PendingArtifact,
    artifact_code: str,
) -> None:
    """在当前 UoW 事务中将待登记产物转换为持久化实体并写入。"""
    uow.document_artifacts.mark_active_as_superseded(
        document_id=document.id,
        artifact_type=artifact.artifact_type,
        artifact_role=artifact.artifact_role,
        artifact_format=artifact.artifact_format,
    )
    uow.document_artifacts.create(
        DocumentArtifactCreate(
            document_id=document.id,
            artifact_code=artifact_code,
            artifact_type=artifact.artifact_type,
            artifact_role=artifact.artifact_role,
            artifact_format=artifact.artifact_format,
            artifact_uri=artifact.artifact_uri,
            artifact_hash=artifact.artifact_hash,
            provider=artifact.provider,
            processor=artifact.processor,
            file_size=artifact.file_size,
            char_count=artifact.char_count,
            line_count=artifact.line_count,
            status="active",
            metadata=artifact.metadata or {},
            created_by_actor_code=document.created_by_actor_code,
        )
    )


def _generate_cleaned_artifact_code(
    doc_code: str,
    artifact_format: str,
) -> str:
    """生成不超过 Artifact 字段长度限制的唯一 cleaned 产物编码。"""
    suffix = (
        f"_ART_CLEANED_{artifact_format.upper()}_"
        f"{uuid4().hex[:12].upper()}"
    )
    return f"{doc_code[:100 - len(suffix)]}{suffix}"
