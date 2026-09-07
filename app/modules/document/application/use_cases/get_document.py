"""获取单份文档完整详情的查询用例。

通过应用层直接根据主键 ID 读取文档实体并转换为 DocumentResult DTO。
"""

from collections.abc import Callable
from typing import Any

from app.modules.document.application.dto import DocumentResult
from app.modules.document.application.errors import DocumentApplicationError


class GetDocumentUseCase:
    """按主键 ID 获取文档详情视图的只读用例。"""

    def __init__(self, *, uow_factory: Callable[[], Any]) -> None:
        """初始化文档详情查询用例。

        Args:
            uow_factory: 数据库工作单元工厂。
        """
        self._uow_factory = uow_factory

    def execute(self, document_id: int) -> DocumentResult:
        """根据文档 ID 查询文档记录并转换为 DocumentResult DTO。

        Args:
            document_id: 待查询的文档主键 ID。

        Returns:
            文档完整详情视图。

        Raises:
            DocumentApplicationError: 当文档不存在时抛出 404 异常。
        """
        with self._uow_factory() as uow:
            document = uow.documents.get_by_id(document_id)
            if document is None:
                raise DocumentApplicationError(404, "文档不存在")
            return DocumentResult.model_validate(document)
