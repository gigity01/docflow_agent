"""上传文档应用用例：编排落盘、去重、建档和审计日志。

流水线阶段 1：Upload
负责接收原始文档流并进行 MIME 与扩展名白名单校验，流式写入本地文件存储，
计算 SHA-256 哈希并在所属知识库内执行查重，最后创建初始 status='uploaded' 的文档记录。
若上传过程中断或失败，尽力物理清理已落盘的文件。
"""

import logging
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from app.modules.document.application.dto import DocumentResult
from app.modules.document.application.errors import DocumentApplicationError
from app.modules.document.application.ports import (
    DocumentApplicationPorts,
    UploadFilePort,
    UploadMetadataPort,
)
from app.modules.document.application.settings import (
    DocumentUploadSettings,
)
from app.modules.document.domain.enums import (
    DocumentLifecycleStatus,
    DocumentStorageStatus,
)
from app.modules.document.domain.policies import (
    normalize_source_type,
    requires_external_processing,
)
from app.shared.observability.document_upload_logger import (
    DocumentUploadLogger,
)
from app.shared.observability.correlation import DocumentOperationContext


# 分块流式读取与落盘的缓冲区大小：1 MiB
READ_CHUNK_SIZE = 1024 * 1024
# 数据库知识库内活跃内容哈希唯一约束名称
DOCUMENT_CONTENT_UNIQUE_CONSTRAINT = "uq_documents_kb_active_content_hash"
logger = logging.getLogger(__name__)


def is_duplicate_content_error(exc: BaseException) -> bool:
    """判断底层数据库完整性异常是否由知识库内内容哈希唯一约束冲突引起。

    Args:
        exc: 捕获的底层异常。

    Returns:
        若属于同一知识库中 active_content_hash 唯一键冲突则返回 True，否则返回 False。
    """
    error_message = str(getattr(exc, "orig", exc)).lower()
    return (
        DOCUMENT_CONTENT_UNIQUE_CONSTRAINT.lower() in error_message
        or (
            "documents.kb_id" in error_message
            and "documents.active_content_hash" in error_message
        )
    )


def safe_log_completed(
    upload_logger: DocumentUploadLogger,
    document: object,
) -> None:
    """尽力记录上传完成可观测性事件。

    防止因日志或监控组件故障导致已成功提交的数据库与落盘业务回退或抛错。

    Args:
        upload_logger: 文档上传专用观测日志记录器。
        document: 刚刚创建成功的持久化 Document 对象。
    """
    try:
        upload_logger.completed(document=document)
    except Exception:
        logger.exception(
            "文档上传已提交，但完成事件写入失败",
            extra={
                "document_id": getattr(document, "id", None),
                "doc_code": getattr(document, "doc_code", None),
            },
        )


def get_initial_lifecycle_status(effective_at: datetime | None) -> str:
    """根据生效时间确定新上传文档的初始业务生命周期状态。

    若指定了未来的生效时间，则初始生命周期状态为 scheduled；否则默认为 active。

    Args:
        effective_at: 可选的业务计划生效时间。

    Returns:
        初始业务生命周期状态枚举值字符串（'scheduled' 或 'active'）。
    """
    if effective_at is None:
        return DocumentLifecycleStatus.ACTIVE.value

    now = datetime.now(tz=effective_at.tzinfo)
    if effective_at > now:
        return DocumentLifecycleStatus.SCHEDULED.value

    return DocumentLifecycleStatus.ACTIVE.value


def generate_doc_code(settings: DocumentUploadSettings) -> str:
    """生成带时间戳和随机字母后缀的全局唯一文档业务编号（doc_code）。

    格式形如：DOC_20260825190000_ABCD12

    Args:
        settings: 上传配置参数对象。

    Returns:
        生成的业务唯一编码字符串。
    """
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    random_part = uuid4().hex[
        :settings.document_code_random_length
    ].upper()
    return f"{settings.document_code_prefix}_{now}_{random_part}"


def get_raw_storage_dir(
    source_type: str,
    settings: DocumentUploadSettings,
) -> Path:
    """按处理路径返回原始文件的本地持久化目标存储目录。

    需要外部 Docling 转换的复杂格式存入 raw_external_storage_dir，
    本地可直接清洗的格式存入 raw_local_storage_dir。

    Args:
        source_type: 归一化后的文档源类型（如 'pdf', 'md', 'txt' 等）。
        settings: 上传配置参数对象。

    Returns:
        目标原始文件存储目录的 Path 对象。
    """
    if requires_external_processing(source_type):
        return settings.raw_external_storage_dir

    return settings.raw_local_storage_dir


class UploadDocumentUseCase:
    """保存上传文件、校验内容去重，并创建文档初始记录。"""

    def __init__(
        self,
        *,
        ports: DocumentApplicationPorts,
        settings: DocumentUploadSettings,
    ) -> None:
        """初始化文档上传用例。

        Args:
            ports: 外部依赖端口容器。
            settings: 上传配置参数。
        """
        self._ports = ports
        self._settings = settings

    async def execute(
        self,
        file: UploadFilePort,
        meta: UploadMetadataPort,
        created_by_actor_code: str | None = None,
        *,
        operation_context: DocumentOperationContext | None = None,
    ) -> DocumentResult:
        """执行文档上传核心逻辑。

        包括：
        1. 文件名与 Content-Type 白名单校验，归一化扩展名。
        2. 以 1 MiB 分块读取并流式写入本地存储目录，严格限制最大文件大小（<=20MB）。
        3. 计算文件的 SHA-256 哈希值。
        4. 在同一知识库（kb_id）下查重；若已存在相同有效文件则拒绝并报错 409。
        5. 在数据库事务中创建初始 status='uploaded'、lifecycle_status 对应的 Document 记录并提交。
        6. 写入生命周期观测事件。
        任一步骤在数据库提交前失败时，尽力物理清理已落盘的文件。

        Args:
            file: 实现了 UploadFilePort 的文件输入流。
            meta: 包含知识库 ID、领域编码、标题等表单元数据对象。
            created_by_actor_code: 可选的创建人编码。
            operation_context: 可选的操作上下文追踪对象。

        Returns:
            创建成功的文档领域数据 DTO。

        Raises:
            DocumentApplicationError: 参数不合法（400）或知识库内文件重复（409）。
        """
        upload_logger = (
            DocumentUploadLogger(operation_context=operation_context)
            if operation_context is not None
            else DocumentUploadLogger()
        )
        actor_code = (
            created_by_actor_code
            or self._settings.default_created_by_actor_code
        )
        phase = "validate"
        doc_code: str | None = None
        source_type: str | None = None
        save_path: Path | None = None
        total_size = 0
        db_committed = False

        try:
            # 阶段 1：校验文件名与 MIME 白名单
            doc_code = generate_doc_code(self._settings)
            if not file.filename:
                raise DocumentApplicationError(
                    status_code=400,
                    detail="必须上传文件",
                )

            self._ports.validate_content_type(file)
            source_extension = self._ports.get_safe_extension(file.filename)
            source_type = normalize_source_type(source_extension)

            # 阶段 2：准备存储目录与落盘路径
            phase = "prepare_storage"
            # 两个目录都会预创建：选择由 source_type 决定，避免在校验通过后因目录不存在而中断
            self._settings.raw_external_storage_dir.mkdir(
                parents=True,
                exist_ok=True,
            )
            self._settings.raw_local_storage_dir.mkdir(
                parents=True,
                exist_ok=True,
            )
            saved_filename = f"{doc_code}.{source_extension}"
            save_path = (
                get_raw_storage_dir(source_type, self._settings)
                / saved_filename
            )

            # 阶段 3：分块流式写入并统计大小
            phase = "execute"
            upload_logger.started(
                doc_code=doc_code,
                kb_id=meta.kb_id,
                domain_code=meta.domain_code,
                business_scene=meta.business_scene,
                title=meta.title,
                filename=file.filename,
                source_type=source_type,
                saved_filename=saved_filename,
                created_by_actor_code=actor_code,
            )

            with save_path.open("wb") as buffer:
                while True:
                    chunk = await file.read(READ_CHUNK_SIZE)

                    if not chunk:
                        break

                    total_size += len(chunk)

                    # 严格校验文件大小上限（默认 20 MiB）
                    if total_size > self._settings.max_upload_file_size:
                        limit_mb = (
                            self._settings.max_upload_file_size // 1024 // 1024
                        )
                        raise DocumentApplicationError(
                            status_code=400,
                            detail=f"文件超过 {limit_mb}MB 限制",
                        )

                    buffer.write(chunk)

            if total_size == 0:
                raise DocumentApplicationError(
                    status_code=400,
                    detail="不能上传空文件",
                )

            upload_logger.raw_file_saved(
                doc_code=doc_code,
                kb_id=meta.kb_id,
                source_uri=str(save_path),
                file_size=total_size,
            )

            # 阶段 4：完整落盘后计算 SHA-256 哈希
            # 先完整落盘再计算哈希，保证去重比较的是实际保存的字节，而非分块读取的中间状态
            content_hash = self._ports.calculate_file_hash(save_path)

            upload_logger.hash_calculated(
                doc_code=doc_code,
                kb_id=meta.kb_id,
                content_hash=content_hash,
            )

            # 阶段 5：知识库内查重与数据库持久化
            phase = "finalize"
            with self._ports.uow_factory() as uow:
                # 去重范围限定在知识库内：不同知识库可各自维护相同原件
                duplicated = uow.documents.get_active_by_hash_in_kb(
                    kb_id=meta.kb_id,
                    content_hash=content_hash,
                )

                if duplicated:
                    upload_logger.duplicate_detected(
                        doc_code=doc_code,
                        kb_id=meta.kb_id,
                        content_hash=content_hash,
                        duplicated_document=duplicated,
                    )

                    raise DocumentApplicationError(
                        status_code=409,
                        detail=f"已存在相同有效文件: {duplicated.doc_code}",
                    )

                # 构建初始状态为 uploaded 的 Document 实体
                document = self._ports.document_factory(
                    doc_code=doc_code,
                    kb_id=meta.kb_id,
                    domain_code=meta.domain_code,
                    business_scene=meta.business_scene,
                    title=meta.title,
                    original_filename=file.filename,
                    file_size=total_size,
                    source_type=source_type,
                    source_uri=str(save_path),
                    cleaned_uri=None,
                    content_hash=content_hash,
                    active_content_hash=content_hash,
                    lifecycle_status=get_initial_lifecycle_status(
                        meta.effective_at
                    ),
                    storage_status=DocumentStorageStatus.ACTIVE.value,
                    version=self._settings.default_document_version,
                    status=self._settings.default_document_status,
                    replaced_by=None,
                    risk_level=meta.risk_level,
                    effective_at=meta.effective_at,
                    expired_at=meta.expired_at,
                    created_by_actor_code=actor_code,
                    indexed_at=None,
                )

                try:
                    created_document = uow.documents.create(document)
                    created_response = DocumentResult.model_validate(
                        created_document
                    )

                    # commit 必须是 UoW 内最后一个数据库动作
                    uow.commit()
                except Exception as exc:
                    if (
                        self._ports.is_integrity_error(exc)
                        and is_duplicate_content_error(exc)
                    ):
                        raise DocumentApplicationError(
                            status_code=409,
                            detail="该知识库中已存在相同有效文件",
                        ) from exc
                    raise

                db_committed = True

            # 尽力记录完成审计事件
            safe_log_completed(upload_logger, created_document)

            return created_response

        except DocumentApplicationError as exc:
            if db_committed:
                raise

            # 业务拒绝（格式、大小、重复等）同样可能已创建临时原件，必须尽力清理
            cleanup_success = (
                self._ports.cleanup_file(save_path)
                if save_path is not None
                else True
            )
            upload_logger.failed_by_http_exception(
                exc=exc,
                phase=phase,
                doc_code=doc_code,
                kb_id=meta.kb_id,
                domain_code=meta.domain_code,
                business_scene=meta.business_scene,
                title=meta.title,
                filename=file.filename,
                source_type=source_type,
                source_uri=(
                    str(save_path) if save_path is not None else None
                ),
                file_size=total_size,
                cleanup_success=cleanup_success,
            )
            raise

        except Exception as exc:
            if db_committed:
                logger.exception(
                    "文档上传已提交，后续操作失败但保留原始文件",
                    extra={"doc_code": doc_code},
                )
                raise

            # 未预期异常清理落盘文件
            cleanup_success = (
                self._ports.cleanup_file(save_path)
                if save_path is not None
                else True
            )

            upload_logger.failed_by_unexpected_exception(
                exc=exc,
                phase=phase,
                doc_code=doc_code,
                kb_id=meta.kb_id,
                domain_code=meta.domain_code,
                business_scene=meta.business_scene,
                title=meta.title,
                filename=file.filename,
                source_type=source_type,
                source_uri=(
                    str(save_path) if save_path is not None else None
                ),
                file_size=total_size,
                cleanup_success=cleanup_success,
            )

            raise
