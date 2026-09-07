"""文档业务日志的显式只读应用用例（Use Cases）。

封装针对文档业务流水日志的多维查询、单次操作事件时间线读取及跨阶段工作流时间线读取用例。
"""

from app.modules.operations.application.dto import (
    DocumentBusinessLogPage,
    DocumentBusinessLogQuery,
    DocumentOperationTimelineQuery,
    DocumentOperationTimelineResult,
    DocumentWorkflowTimelineQuery,
    DocumentWorkflowTimelineResult,
)
from app.modules.operations.application.query_service import (
    OperationsQueryService,
)


class QueryDocumentLogEventsUseCase:
    """按关联 ID、文档、阶段和时间筛选文档业务事件用例。"""

    def __init__(self, query_service: OperationsQueryService) -> None:
        """初始化文档日志事件查询用例。

        Args:
            query_service: Operations 查询服务实例。
        """
        self._query_service = query_service

    def execute(
        self,
        query: DocumentBusinessLogQuery,
    ) -> DocumentBusinessLogPage:
        """执行文档业务事件多维查询。

        Args:
            query: 查询条件。

        Returns:
            匹配的事件分页结果集。
        """
        return self._query_service.query_document_business_logs(query)


class GetDocumentOperationTimelineUseCase:
    """读取一个 operation_id 下的完整阶段事件时间线用例。"""

    def __init__(self, query_service: OperationsQueryService) -> None:
        """初始化单次操作时间线查询用例。

        Args:
            query_service: Operations 查询服务实例。
        """
        self._query_service = query_service

    def execute(
        self,
        query: DocumentOperationTimelineQuery,
    ) -> DocumentOperationTimelineResult:
        """执行单次阶段操作事件时间线查询。

        Args:
            query: 包含 operation_id 的查询条件。

        Returns:
            单次操作的完整聚合事件流。
        """
        return self._query_service.get_document_operation_timeline(query)


class GetDocumentWorkflowTimelineUseCase:
    """读取一个 workflow_id 下跨阶段、跨重试的完整事件时间线用例。"""

    def __init__(self, query_service: OperationsQueryService) -> None:
        """初始化工作流时间线查询用例。

        Args:
            query_service: Operations 查询服务实例。
        """
        self._query_service = query_service

    def execute(
        self,
        query: DocumentWorkflowTimelineQuery,
    ) -> DocumentWorkflowTimelineResult:
        """执行跨阶段工作流事件时间线查询。

        Args:
            query: 包含 workflow_id 的查询条件。

        Returns:
            工作流的完整聚合事件流。
        """
        return self._query_service.get_document_workflow_timeline(query)
