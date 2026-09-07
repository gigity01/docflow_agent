"""文档派生产物（Artifact）多条件高级检索用例。

支持按多文档 ID、产物类型、产物角色、文件格式、状态、转换提供方、处理类名及创建时间范围进行复合查询与分页。
"""

from collections.abc import Callable
from typing import Any

from app.modules.document.application.dto import (
    DocumentArtifactResult,
    DocumentArtifactSearchQuery,
    SearchDocumentArtifactsResult,
)


class SearchDocumentArtifactsUseCase:
    """提供派生产物多条件组合查询与分页能力的应用层用例。"""

    def __init__(self, *, uow_factory: Callable[[], Any]) -> None:
        """初始化产物高级检索用例。

        Args:
            uow_factory: 数据库工作单元工厂。
        """
        self._uow_factory = uow_factory

    def execute(
        self,
        query: DocumentArtifactSearchQuery,
    ) -> SearchDocumentArtifactsResult:
        """执行派生产物多条件高级检索。

        Args:
            query: 产物检索查询参数 DTO。

        Returns:
            包含产物结果列表与分页总数的 DTO。
        """
        with self._uow_factory() as uow:
            artifacts = uow.document_artifacts.search(query)
            return SearchDocumentArtifactsResult(
                items=[
                    DocumentArtifactResult.model_validate(artifact)
                    for artifact in artifacts
                ],
                total=uow.document_artifacts.count_search(query),
                limit=query.limit,
                offset=query.offset,
            )
