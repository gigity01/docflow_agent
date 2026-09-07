"""文档业务生命周期变更应用用例。

负责在数据库单一事务中处理文档业务有效性状态变更（失效、过期、替代、软删除），
并在失效时清空 active_content_hash 以释放内容哈希占用，使同哈希新文档能够重新上传。
若原因为 'replaced'，严格校验替代文档（replacement）必须属于同一知识库、已完成索引、有效且未过期。
"""

from __future__ import annotations

from datetime import datetime
from typing import Callable, Any

from app.modules.document.application.dto import DocumentResult
from app.modules.document.application.errors import DocumentApplicationError
from app.modules.document.domain.enums import (
    DocumentLifecycleStatus,
    DocumentStatus,
    DocumentStorageStatus,
)


# 支持作为失效原因的生命周期状态枚举集合
DEACTIVATION_REASONS = frozenset(
    {
        DocumentLifecycleStatus.EXPIRED,
        DocumentLifecycleStatus.REPLACED,
        DocumentLifecycleStatus.DELETED,
    }
)
# 已失效的生命周期状态值集合
INACTIVE_LIFECYCLE_STATUSES = frozenset(
    status.value for status in DEACTIVATION_REASONS
)
# 活跃/有效的生命周期状态值集合
ACTIVE_LIFECYCLE_STATUSES = frozenset(
    {
        DocumentLifecycleStatus.ACTIVE.value,
        DocumentLifecycleStatus.SCHEDULED.value,
    }
)


def _validate_replacement(
    document,
    replacement,
) -> None:
    """校验替代文档的合法性。

    要求：
    1. 不能替代自身。
    2. 必须属于同一知识库（kb_id）。
    3. 状态必须为 indexed（已完成切块与向量索引）。
    4. 业务生命周期必须为 active。
    5. 存储状态必须为 active。
    6. 替代文档当前必须已生效且未过期。

    Args:
        document: 被替代的当前文档实体。
        replacement: 指定的替代新文档实体。

    Raises:
        DocumentApplicationError: 任意校验规则不满足时抛出 400 或 409。
    """
    if replacement.id == document.id:
        raise DocumentApplicationError(status_code=400, detail="文档不能替代自身")
    if replacement.kb_id != document.kb_id:
        raise DocumentApplicationError(
            status_code=409,
            detail="替代文档必须属于同一知识库",
        )
    if replacement.status != DocumentStatus.INDEXED.value:
        raise DocumentApplicationError(
            status_code=409,
            detail="替代文档尚未完成索引",
        )
    if replacement.lifecycle_status != DocumentLifecycleStatus.ACTIVE.value:
        raise DocumentApplicationError(
            status_code=409,
            detail="替代文档不是业务有效文档",
        )
    if replacement.storage_status != DocumentStorageStatus.ACTIVE.value:
        raise DocumentApplicationError(
            status_code=409,
            detail="替代文档不在活跃存储区",
        )
    if (
        replacement.effective_at is not None
        and replacement.effective_at
        > datetime.now(tz=replacement.effective_at.tzinfo)
    ):
        raise DocumentApplicationError(
            status_code=409,
            detail="替代文档尚未生效",
        )
    if (
        replacement.expired_at is not None
        and replacement.expired_at
        <= datetime.now(tz=replacement.expired_at.tzinfo)
    ):
        raise DocumentApplicationError(
            status_code=409,
            detail="替代文档已经到期",
        )


class ChangeDocumentLifecycleUseCase:
    """在单一事务中变更文档业务生命周期的用例入口。"""

    def __init__(self, *, uow_factory: Callable[[], Any]) -> None:
        """初始化生命周期变更用例。

        Args:
            uow_factory: 数据库工作单元工厂。
        """
        self._uow_factory = uow_factory

    def execute(
        self,
        document_id: int,
        reason: DocumentLifecycleStatus,
        *,
        replaced_by: int | None = None,
    ) -> DocumentResult:
        """执行文档生命周期状态变更。

        Args:
            document_id: 待操作的文档 ID。
            reason: 变更目标状态/失效原因（EXPIRED / REPLACED / DELETED）。
            replaced_by: 若 reason 为 REPLACED，必须传入替代文档的 ID。

        Returns:
            更新后的文档详情 DTO。

        Raises:
            DocumentApplicationError: 参数非法或状态冲突时抛出。
        """
        return _deactivate_document(
            document_id,
            reason,
            replaced_by=replaced_by,
            uow_factory=self._uow_factory,
        )


def _deactivate_document(
    document_id: int,
    reason: DocumentLifecycleStatus,
    *,
    replaced_by: int | None,
    uow_factory: Callable[[], Any],
) -> DocumentResult:
    """在单一事务中将文档标记为失效、释放 active_content_hash 并提交。"""
    if not isinstance(reason, DocumentLifecycleStatus) or reason not in DEACTIVATION_REASONS:
        raise ValueError("不支持的失效原因")

    with uow_factory() as uow:
        replacement = None
        # 若为替换操作，同时以行锁锁定原文档与新文档
        if (
            reason == DocumentLifecycleStatus.REPLACED
            and replaced_by is not None
            and replaced_by != document_id
        ):
            locked_documents = uow.documents.get_by_ids_for_update(
                (document_id, replaced_by)
            )
            documents_by_id = {
                locked_document.id: locked_document
                for locked_document in locked_documents
            }
            document = documents_by_id.get(document_id)
            replacement = documents_by_id.get(replaced_by)
        else:
            document = uow.documents.get_by_id_for_update(document_id)

        if document is None:
            raise DocumentApplicationError(status_code=404, detail="文档不存在")

        # 幂等处理：若已处于相同失效状态
        if document.lifecycle_status == reason.value:
            if (
                reason == DocumentLifecycleStatus.REPLACED
                and document.replaced_by != replaced_by
            ):
                raise DocumentApplicationError(
                    status_code=409,
                    detail="文档已经被其他文档替代",
                )
            return DocumentResult.model_validate(document)

        if document.lifecycle_status in INACTIVE_LIFECYCLE_STATUSES:
            raise DocumentApplicationError(
                status_code=409,
                detail=f"文档已经失效: {document.lifecycle_status}",
            )
        if document.lifecycle_status not in ACTIVE_LIFECYCLE_STATUSES:
            raise DocumentApplicationError(
                status_code=409,
                detail=f"文档生命周期状态无效: {document.lifecycle_status}",
            )

        if reason == DocumentLifecycleStatus.REPLACED:
            if replaced_by is None:
                raise DocumentApplicationError(
                    status_code=400,
                    detail="替代操作必须指定新文档",
                )
            if replaced_by == document.id:
                raise DocumentApplicationError(
                    status_code=400,
                    detail="文档不能替代自身",
                )

            if replacement is None:
                raise DocumentApplicationError(
                    status_code=404,
                    detail="替代文档不存在",
                )
            _validate_replacement(document, replacement)

        # 执行停用：修改 lifecycle_status，记录 replaced_by，并将 active_content_hash 置为 None
        updated_document = uow.documents.deactivate(
            document,
            lifecycle_status=reason.value,
            replaced_by=replaced_by,
        )
        response = DocumentResult.model_validate(updated_document)
        uow.commit()

    return response
