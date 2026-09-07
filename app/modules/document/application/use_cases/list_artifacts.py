"""列出指定文档关联全部派生产物（Artifact）的查询用例。

包含原始文件路径、源格式以及由清洗/转换流水线生成的二级文本、清洗文本等全部产物元数据。
"""

from collections.abc import Callable
from typing import Any

from app.modules.document.application.dto import (
    DocumentArtifactResult,
    ListDocumentArtifactsResult,
)
from app.modules.document.application.errors import DocumentApplicationError


class ListDocumentArtifactsUseCase:
    """通过应用层查询并返回指定文档的所有派生产物元数据列表。"""

    def __init__(self, *, uow_factory: Callable[[], Any]) -> None:
        """初始化产物列表查询用例。

        Args:
            uow_factory: 数据库工作单元工厂。
        """
        self._uow_factory = uow_factory

    def execute(self, document_id: int) -> ListDocumentArtifactsResult:
        """查询指定文档 ID 关联的全部派生产物。

        Args:
            document_id: 目标文档 ID。

        Returns:
            包含源信息及产物实体列表的结果 DTO。

        Raises:
            DocumentApplicationError: 文档不存在时抛出 404。
        """
        with self._uow_factory() as uow:
            document = uow.documents.get_by_id(document_id)
            if document is None:
                raise DocumentApplicationError(404, "文档不存在")

            artifacts = uow.document_artifacts.list_by_document_id(
                document_id
            )
            return ListDocumentArtifactsResult(
                document_id=document_id,
                source_uri=document.source_uri,
                source_type=document.source_type,
                original_filename=document.original_filename,
                items=[
                    DocumentArtifactResult.model_validate(artifact)
                    for artifact in artifacts
                ],
            )
