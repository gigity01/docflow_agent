"""文档业务日志和 Agent Tool 审计的查询服务。

组合文档日志与 Tool 审计日志的底层 JsonlLogQueryPort，
提供多维筛选、时序重构、时间线聚合（Execution / Operation / Workflow / Failure / Tool Timeline）及 DTO 转换。
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from app.modules.operations.application.dto import (
    AgentToolAuditEvent,
    AgentToolAuditPage,
    AgentToolAuditQuery,
    DocumentBusinessLogEvent,
    DocumentBusinessLogPage,
    DocumentBusinessLogQuery,
    DocumentExecutionTimelineResult,
    DocumentFailureEvent,
    DocumentFailureTimelineResult,
    DocumentTimelineQuery,
    DocumentOperationTimelineQuery,
    DocumentOperationTimelineResult,
    DocumentWorkflowTimelineQuery,
    DocumentWorkflowTimelineResult,
    ToolInvocationTimelineItem,
    ToolTimelineQuery,
    ToolTimelineResult,
)
from app.modules.operations.application.ports import JsonlLogQueryPort


# 文档事件通用顶层字段集合，其余额外字段将被归入 details 字典
DOCUMENT_EVENT_COMMON_FIELDS = {
    "schema_version",
    "event_id",
    "run_id",
    "workflow_id",
    "operation_id",
    "parent_operation_id",
    "attempt",
    "document_id",
    "doc_code",
    "kb_id",
    "stage",
    "event",
    "phase",
    "level",
    "message",
    "error_type",
    "error_message",
    "status_before",
    "status_after",
    "state_updated",
    "document_state_updated",
    "duration_ms",
    "created_at",
}


class OperationsQueryService:
    """运维查询核心服务。

    组合受控 JSONL Repository，提供确定性筛选和时间线聚合。
    """

    def __init__(
        self,
        *,
        document_logs: JsonlLogQueryPort,
        agent_tool_logs: JsonlLogQueryPort,
    ) -> None:
        """初始化 Operations 查询服务。

        Args:
            document_logs: 文档业务流水日志查询端口。
            agent_tool_logs: Agent Tool 调用审计日志查询端口。
        """
        self._document_logs = document_logs
        self._agent_tool_logs = agent_tool_logs

    def query_document_business_logs(
        self,
        query: DocumentBusinessLogQuery,
    ) -> DocumentBusinessLogPage:
        """根据多维查询条件检索文档业务日志。

        Args:
            query: 文档业务日志多维查询参数。

        Returns:
            分页文档业务日志结果集。
        """
        predicate = self._document_predicate(query)
        page = self._document_logs.scan(
            predicate=predicate,
            created_from=query.created_from,
            created_to=query.created_to,
            limit=query.limit,
            cursor=query.cursor,
        )
        return DocumentBusinessLogPage(
            items=[self._document_event(event) for event in page.events],
            next_cursor=page.next_cursor,
        )

    def get_document_execution_timeline(
        self,
        query: DocumentTimelineQuery,
    ) -> DocumentExecutionTimelineResult:
        """按时间升序重构单篇文档从上传到索引的完整生命周期执行时间线。

        Args:
            query: 文档生命周期时间线查询条件。

        Returns:
            包含按时间升序排序的事件序列与截断标志。
        """
        page = self._document_logs.scan(
            predicate=lambda event: event.get("document_id")
            == query.document_id,
            created_from=query.created_from,
            created_to=query.created_to,
            limit=query.limit,
            ascending=True,
        )
        return DocumentExecutionTimelineResult(
            document_id=query.document_id,
            events=[self._document_event(event) for event in page.events],
            truncated=page.next_cursor is not None,
        )

    def get_document_failure_timeline(
        self,
        query: DocumentTimelineQuery,
    ) -> DocumentFailureTimelineResult:
        """按时间升序提取单篇文档发生过的所有失败事件与错误摘要。

        Args:
            query: 文档失败时间线查询条件。

        Returns:
            包含失败事件序列与截断标志。
        """
        page = self._document_logs.scan(
            predicate=lambda event: (
                event.get("document_id") == query.document_id
                and self._is_failure(event)
            ),
            created_from=query.created_from,
            created_to=query.created_to,
            limit=query.limit,
            ascending=True,
        )
        return DocumentFailureTimelineResult(
            document_id=query.document_id,
            failures=[self._failure_event(event) for event in page.events],
            truncated=page.next_cursor is not None,
        )

    def get_document_operation_timeline(
        self,
        query: DocumentOperationTimelineQuery,
    ) -> DocumentOperationTimelineResult:
        """按 operation_id 提取单次阶段操作的完整事件时间线。

        Args:
            query: 单次操作时间线查询条件。

        Returns:
            包含该操作内部事件流及关联 workflow_id 和 attempt。
        """
        page = self._document_logs.scan(
            predicate=lambda event: self._operation_id(event)
            == query.operation_id,
            created_from=query.created_from,
            created_to=query.created_to,
            limit=query.limit,
            ascending=True,
        )
        events = [self._document_event(event) for event in page.events]
        first = events[0] if events else None
        return DocumentOperationTimelineResult(
            operation_id=query.operation_id,
            workflow_id=first.workflow_id if first is not None else None,
            attempt=first.attempt if first is not None else None,
            events=events,
            truncated=page.next_cursor is not None,
        )

    def get_document_workflow_timeline(
        self,
        query: DocumentWorkflowTimelineQuery,
    ) -> DocumentWorkflowTimelineResult:
        """按 workflow_id 提取跨阶段、跨重试的完整工作流事件时间线。

        Args:
            query: 工作流时间线查询条件。

        Returns:
            包含完整工作流事件流与截断标志。
        """
        page = self._document_logs.scan(
            predicate=lambda event: event.get("workflow_id")
            == query.workflow_id,
            created_from=query.created_from,
            created_to=query.created_to,
            limit=query.limit,
            ascending=True,
        )
        return DocumentWorkflowTimelineResult(
            workflow_id=query.workflow_id,
            events=[self._document_event(event) for event in page.events],
            truncated=page.next_cursor is not None,
        )

    def query_agent_tool_audits(
        self,
        query: AgentToolAuditQuery,
    ) -> AgentToolAuditPage:
        """按多维条件检索 Agent Tool 审计日志。

        Args:
            query: Tool 审计多维查询参数。

        Returns:
            分页 Tool 审计事件结果集。
        """
        page = self._agent_tool_logs.scan(
            predicate=self._audit_predicate(query),
            created_from=query.created_from,
            created_to=query.created_to,
            limit=query.limit,
            cursor=query.cursor,
        )
        return AgentToolAuditPage(
            items=[self._audit_event(event) for event in page.events],
            next_cursor=page.next_cursor,
        )

    def get_task_tool_timeline(
        self,
        query: ToolTimelineQuery,
    ) -> ToolTimelineResult:
        """按 task_id 获取该任务执行过程中的 Tool 调用聚合时间线。

        Args:
            query: Tool 时间线查询条件（identifier 为 task_id）。

        Returns:
            包含聚合配对后的 Tool 调用条目列表。
        """
        return self._tool_timeline(
            query,
            identifier_field="task_id",
        )

    def get_agent_run_tool_timeline(
        self,
        query: ToolTimelineQuery,
    ) -> ToolTimelineResult:
        """按 agent_run_id 获取该 Agent 运行会话中的 Tool 调用聚合时间线。

        Args:
            query: Tool 时间线查询条件（identifier 为 agent_run_id）。

        Returns:
            包含聚合配对后的 Tool 调用条目列表。
        """
        return self._tool_timeline(
            query,
            identifier_field="agent_run_id",
        )

    def _tool_timeline(
        self,
        query: ToolTimelineQuery,
        *,
        identifier_field: str,
    ) -> ToolTimelineResult:
        """通用 Tool 时间线聚合扫描实现。

        根据指定标识字段（如 task_id 或 agent_run_id）扫描并聚合工具调用。

        Args:
            query: 查询条件。
            identifier_field: 标识符在日志中的字段名称。

        Returns:
            聚合配对后的结果。
        """
        def predicate(event: dict) -> bool:
            if event.get(identifier_field) != query.identifier:
                return False
            if query.agent_names and event.get("agent_name") not in set(
                query.agent_names
            ):
                return False
            if query.tool_names and event.get("tool_name") not in set(
                query.tool_names
            ):
                return False
            return True

        page = self._agent_tool_logs.scan(
            predicate=predicate,
            created_from=query.created_from,
            created_to=query.created_to,
            limit=query.limit,
            ascending=True,
        )
        return ToolTimelineResult(
            identifier=query.identifier,
            invocations=self._aggregate_invocations(page.events),
            truncated=page.next_cursor is not None,
        )

    @staticmethod
    def _document_predicate(
        query: DocumentBusinessLogQuery,
    ) -> Callable[[dict], bool]:
        """构建文档业务流水日志的内存过滤断言。

        Args:
            query: 文档业务日志多维查询对象。

        Returns:
            Callable[[dict], bool]: 断言函数。
        """
        document_ids = set(query.document_ids)
        doc_codes = set(query.doc_codes)
        kb_ids = set(query.kb_ids)
        stages = set(query.stages)
        events = set(query.events)
        phases = set(query.phases)
        levels = set(query.levels)
        workflow_ids = set(query.workflow_ids)
        operation_ids = set(query.operation_ids)
        attempts = set(query.attempts)

        def predicate(event: dict) -> bool:
            # 1. 集合多值精确匹配检查
            checks = (
                (workflow_ids, event.get("workflow_id")),
                (operation_ids, OperationsQueryService._operation_id(event)),
                (attempts, event.get("attempt")),
                (document_ids, event.get("document_id")),
                (doc_codes, event.get("doc_code")),
                (kb_ids, event.get("kb_id")),
                (stages, event.get("stage")),
                (events, event.get("event")),
                (phases, event.get("phase")),
                (levels, event.get("level")),
            )
            if any(values and actual not in values for values, actual in checks):
                return False

            # 2. 单值标量匹配检查
            if query.trace_id is not None and event.get("trace_id") != query.trace_id:
                return False
            if query.task_id is not None and event.get("task_id") != query.task_id:
                return False

            # 3. 失败日志专属过滤
            return not query.failed_only or OperationsQueryService._is_failure(
                event
            )

        return predicate

    @staticmethod
    def _audit_predicate(
        query: AgentToolAuditQuery,
    ) -> Callable[[dict], bool]:
        """构建 Agent Tool 审计日志的内存过滤断言。

        Args:
            query: Tool 审计查询对象。

        Returns:
            Callable[[dict], bool]: 断言函数。
        """
        agent_names = set(query.agent_names)
        tool_names = set(query.tool_names)
        result_codes = set(query.result_codes)
        events = set(query.events)

        def predicate(event: dict) -> bool:
            # 1. 单值标量匹配检查
            scalar_filters = (
                ("trace_id", query.trace_id),
                ("agent_run_id", query.agent_run_id),
                ("conversation_id", query.conversation_id),
                ("turn_id", query.turn_id),
                ("task_id", query.task_id),
                ("actor_code", query.actor_code),
            )
            if any(
                expected is not None and event.get(field) != expected
                for field, expected in scalar_filters
            ):
                return False

            # 2. 集合多值过滤检查
            if agent_names and event.get("agent_name") not in agent_names:
                return False
            if tool_names and event.get("tool_name") not in tool_names:
                return False
            if result_codes and event.get("result_code") not in result_codes:
                return False
            if events and event.get("event") not in events:
                return False
            if (
                query.retryable is not None
                and event.get("retryable") is not query.retryable
            ):
                return False
            return True

        return predicate

    @staticmethod
    def _is_failure(event: dict) -> bool:
        """判定一条日志事件是否代表失败状态。

        Args:
            event: 原始事件字典。

        Returns:
            bool: True 表示代表失败，False 表示正常。
        """
        name = str(event.get("event") or "")
        level = str(event.get("level") or "").lower()
        return (
            name.endswith("_failed")
            or level in {"error", "critical"}
            or event.get("outcome") == "error"
        )

    @staticmethod
    def _document_event(event: dict[str, Any]) -> DocumentBusinessLogEvent:
        """将原始文档日志字典转换为结构化 DocumentBusinessLogEvent DTO。

        提取未在顶层显式声明的字段放置于 details 字典中。

        Args:
            event: 原始事件字典。

        Returns:
            DocumentBusinessLogEvent: 转换后的数据对象。
        """
        details = {
            key: value
            for key, value in event.items()
            if key not in DOCUMENT_EVENT_COMMON_FIELDS
        }
        return DocumentBusinessLogEvent(
            event_id=str(event.get("event_id") or ""),
            workflow_id=event.get("workflow_id"),
            operation_id=OperationsQueryService._operation_id(event),
            parent_operation_id=event.get("parent_operation_id"),
            attempt=event.get("attempt"),
            document_id=event.get("document_id"),
            doc_code=event.get("doc_code"),
            kb_id=event.get("kb_id"),
            stage=str(event.get("stage") or "unknown"),
            event=str(event.get("event") or "unknown"),
            phase=event.get("phase"),
            level=event.get("level"),
            message=event.get("message"),
            error_type=event.get("error_type"),
            error_summary=event.get("error_message"),
            status_before=event.get("status_before"),
            status_after=event.get("status_after"),
            state_updated=event.get(
                "state_updated",
                event.get("document_state_updated"),
            ),
            duration_ms=int(event.get("duration_ms") or 0),
            created_at=event["created_at"],
            details=details,
        )

    @staticmethod
    def _operation_id(event: dict[str, Any]) -> str | None:
        """兼容读取 schema v1 的 run_id，新日志只写 operation_id。

        Args:
            event: 原始事件字典。

        Returns:
            str | None: 提取的操作 ID。
        """
        return event.get("operation_id") or event.get("run_id")

    @staticmethod
    def _failure_event(event: dict[str, Any]) -> DocumentFailureEvent:
        """将原始事件字典转换为精简的 DocumentFailureEvent DTO。

        Args:
            event: 原始事件字典。

        Returns:
            DocumentFailureEvent: 转换后的失败事件对象。
        """
        mapped = OperationsQueryService._document_event(event)
        return DocumentFailureEvent(
            event_id=mapped.event_id,
            stage=mapped.stage,
            event=mapped.event,
            phase=mapped.phase,
            error_type=mapped.error_type,
            error_summary=mapped.error_summary,
            status_before=mapped.status_before,
            status_after=mapped.status_after,
            state_updated=mapped.state_updated,
            created_at=mapped.created_at,
        )

    @staticmethod
    def _audit_event(event: dict[str, Any]) -> AgentToolAuditEvent:
        """将原始 Tool 审计字典转换为 AgentToolAuditEvent DTO。

        Args:
            event: 原始审计事件字典。

        Returns:
            AgentToolAuditEvent: 转换后的审计实体对象。
        """
        return AgentToolAuditEvent(
            event_id=str(event.get("event_id") or ""),
            invocation_id=str(event.get("invocation_id") or ""),
            event=str(event.get("event") or "unknown"),
            trace_id=str(event.get("trace_id") or ""),
            agent_run_id=str(event.get("agent_run_id") or ""),
            conversation_id=event.get("conversation_id"),
            turn_id=event.get("turn_id"),
            task_id=event.get("task_id"),
            agent_name=str(event.get("agent_name") or ""),
            tool_name=str(event.get("tool_name") or ""),
            actor_code=str(event.get("actor_code") or ""),
            resource_refs=list(event.get("resource_refs") or []),
            duration_ms=int(event.get("duration_ms") or 0),
            result_code=event.get("result_code"),
            retryable=event.get("retryable"),
            created_at=event["created_at"],
        )

    @staticmethod
    def _aggregate_invocations(
        events: list[dict[str, Any]],
    ) -> list[ToolInvocationTimelineItem]:
        """将分散的 Tool started / terminal 事件按 invocation_id 配对聚合为完整的生命周期条目。

        Args:
            events: 原始审计事件列表。

        Returns:
            list[ToolInvocationTimelineItem]: 按 started_at 时间升序排序的完整调用条目列表。
        """
        grouped: dict[str, dict[str, Any]] = {}
        for event in events:
            invocation_id = str(event.get("invocation_id") or "")
            if not invocation_id:
                continue
            current = grouped.setdefault(
                invocation_id,
                {
                    "first": event,
                    "started_at": event["created_at"],
                    "terminal": None,
                },
            )
            if event.get("event") == "agent_tool_invocation_started":
                current["first"] = event
                current["started_at"] = event["created_at"]
            elif event.get("event") in {
                "agent_tool_invocation_succeeded",
                "agent_tool_invocation_rejected",
                "agent_tool_invocation_failed",
            }:
                current["terminal"] = event

        items: list[ToolInvocationTimelineItem] = []
        for invocation_id, current in grouped.items():
            first = current["first"]
            terminal = current["terminal"]
            outcome = None
            if terminal is not None:
                outcome = str(terminal["event"]).removeprefix(
                    "agent_tool_invocation_"
                )
            items.append(
                ToolInvocationTimelineItem(
                    invocation_id=invocation_id,
                    trace_id=str(first.get("trace_id") or ""),
                    agent_run_id=str(first.get("agent_run_id") or ""),
                    conversation_id=first.get("conversation_id"),
                    turn_id=first.get("turn_id"),
                    task_id=first.get("task_id"),
                    agent_name=str(first.get("agent_name") or ""),
                    tool_name=str(first.get("tool_name") or ""),
                    actor_code=str(first.get("actor_code") or ""),
                    resource_refs=list(first.get("resource_refs") or []),
                    started_at=current["started_at"],
                    completed_at=(
                        terminal.get("created_at")
                        if terminal is not None
                        else None
                    ),
                    outcome=outcome,
                    result_code=(
                        terminal.get("result_code")
                        if terminal is not None
                        else None
                    ),
                    retryable=(
                        terminal.get("retryable")
                        if terminal is not None
                        else None
                    ),
                    duration_ms=(
                        terminal.get("duration_ms")
                        if terminal is not None
                        else None
                    ),
                )
            )
        return sorted(items, key=lambda item: item.started_at)
