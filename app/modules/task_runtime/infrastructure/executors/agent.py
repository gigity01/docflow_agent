"""把 capability-scoped Agent Run 适配为 Task Executor Port。

本模块实现了通过 OpenAI Agents SDK 驱动 Document Executor Agent 的适配器：
1. 每个 Capability 独享专属 Agent，仅提供受限只读查询 Tool 和当前 Capability 的唯一 Command Tool。
2. 内部严格禁用并行工具调用（parallel_tool_calls=False, max_function_tool_concurrency=1）。
3. 使用 StopAtTools 在 Command Tool 执行后立即中断 Agent 循环，直接将结构化 Tool Output 传递给适配器。
4. 任务成败严格取决于 Command Tool 的结构化返回值，LLM 自由文本不影响判定。
5. 任务取消时通过 await_side_effect_quiescence 排空内部工具调用，避免与补偿产生竞态。
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from uuid import uuid4

from agents import Agent, RunConfig, Runner
from pydantic import BaseModel, ValidationError

from app.agent_runtime.audit import AgentToolAuditLogger
from app.agent_runtime.context import (
    AgentToolContext,
    ContextToolServices,
    DocumentToolServices,
)
from app.modules.document.agent_tools.schemas import (
    BuildDocumentChunksToolOutput,
    IndexDocumentVectorsToolOutput,
    ProcessDocumentToolOutput,
    ToolResult,
)
from app.modules.task_runtime.application.dto import (
    TaskExecutorResult,
    TaskRuntimeContext,
)
from app.modules.task_runtime.application.errors import TaskExecutionError
from app.modules.task_runtime.application.schemas import (
    BuildDocumentChunksTaskOutput,
    IndexDocumentVectorsTaskOutput,
    ProcessDocumentTaskOutput,
)

from ._quiescence import await_side_effect_quiescence


DOCUMENT_EXECUTOR_ACTOR_CODE = "document_executor_agent"
DEFAULT_DOCUMENT_EXECUTOR_MAX_TURNS = 8


def adapt_process_document_output(
    output: ToolResult,
) -> TaskExecutorResult:
    """将 ProcessDocumentToolOutput 转换为通用的 TaskExecutorResult。

    Args:
        output: 工具执行返回的 ToolResult 结构。

    Returns:
        适配后的任务执行结果与资源引用。
    """
    result = ProcessDocumentToolOutput.model_validate(output)
    task_output = ProcessDocumentTaskOutput(
        document_id=result.document_id,
        status=result.document_status,
        cleaned_uri=result.cleaned_uri,
    )
    return TaskExecutorResult(
        output_json=task_output.model_dump(mode="json"),
        resource_refs=result.resource_refs,
    )


def adapt_build_document_chunks_output(
    output: ToolResult,
) -> TaskExecutorResult:
    """将 BuildDocumentChunksToolOutput 转换为通用的 TaskExecutorResult。

    Args:
        output: 工具执行返回的 ToolResult 结构。

    Returns:
        适配后的任务执行结果与资源引用。
    """
    result = BuildDocumentChunksToolOutput.model_validate(output)
    task_output = BuildDocumentChunksTaskOutput(
        document_id=result.document_id,
        status=result.document_status,
        parent_count=result.parent_count,
        child_count=result.child_count,
    )
    return TaskExecutorResult(
        output_json=task_output.model_dump(mode="json"),
        resource_refs=result.resource_refs,
    )


def adapt_index_document_vectors_output(
    output: ToolResult,
) -> TaskExecutorResult:
    """将 IndexDocumentVectorsToolOutput 转换为通用的 TaskExecutorResult。

    Args:
        output: 工具执行返回的 ToolResult 结构。

    Returns:
        适配后的任务执行结果与资源引用。
    """
    result = IndexDocumentVectorsToolOutput.model_validate(output)
    task_output = IndexDocumentVectorsTaskOutput(
        document_id=result.document_id,
        status=result.document_status,
        indexed_chunks=result.indexed_chunks,
        failed_chunks=result.failed_chunks,
    )
    return TaskExecutorResult(
        output_json=task_output.model_dump(mode="json"),
        resource_refs=result.resource_refs,
    )


class AgentTaskExecutor:
    """基于 Agents SDK 的能力受限 Task Executor 适配器。

    执行特点：
    1. 每个 Executor Agent 仅暴露受限的查询 Tool 和针对当前 Capability 的唯一 Command Tool。
    2. 禁止并行工具调用（串行执行）。
    3. 结果严格由 Command Tool 的结构化 Tool Output 驱动，LLM 生成的自由文本不决定任务成败。
    4. 任务取消时等待同步 Command Tool 完全排空（Quiescence），避免并发补偿破坏持久化副作用。
    """

    def __init__(
        self,
        *,
        agent: Agent[AgentToolContext],
        executor_code: str,
        primary_tool_name: str,
        tool_output_model: type[ToolResult],
        output_adapter: Callable[[ToolResult], TaskExecutorResult],
        document_services: DocumentToolServices,
        permissions: frozenset[str],
        run_config: RunConfig,
        max_turns: int = DEFAULT_DOCUMENT_EXECUTOR_MAX_TURNS,
        runner: Callable[..., Any] = Runner.run,
        audit_logger_factory: Callable[[], AgentToolAuditLogger] = (
            AgentToolAuditLogger
        ),
    ) -> None:
        """初始化 AgentTaskExecutor。

        Args:
            agent: OpenAI Agents SDK Agent 实例。
            executor_code: Executor 标识。
            primary_tool_name: 该 Capability 唯一的 Command Tool 名称。
            tool_output_model: 工具输出 ToolResult 模型类。
            output_adapter: 工具结果到 TaskExecutorResult 的适配函数。
            document_services: 注入的文档领域服务。
            permissions: 授权权限集合。
            run_config: Agent 运行配置（含 LLM Provider 等）。
            max_turns: Agent 允许交互的最大 Turn 数（默认 8）。
            runner: Agents SDK Runner 入口。
            audit_logger_factory: 审计日志记录器工厂。

        Raises:
            ValueError: 当 Agent 未包含指定的 primary_tool_name 时。
        """
        if primary_tool_name not in {tool.name for tool in agent.tools}:
            raise ValueError("Executor Agent 未暴露指定 Command Tool")
        self._agent = agent
        self._executor_code = executor_code
        self._primary_tool_name = primary_tool_name
        self._tool_output_model = tool_output_model
        self._output_adapter = output_adapter
        self._document_services = document_services
        self._permissions = permissions
        self._run_config = run_config
        self._max_turns = max_turns
        self._runner = runner
        self._audit_logger_factory = audit_logger_factory

    async def execute(
        self,
        payload: BaseModel,
        context: TaskRuntimeContext,
    ) -> TaskExecutorResult:
        """执行 Agent Task，并通过 Command Tool 输出解析确定性结果。

        Args:
            payload: 任务输入 payload 模型。
            context: 运行时上下文。

        Returns:
            执行结果。

        Raises:
            TaskExecutionError: 当输入无效、Agent 失败、工具拒绝、执行失败或输出契约不合法时。
        """
        document_id = getattr(payload, "document_id", None)
        if not isinstance(document_id, int):
            raise TaskExecutionError(
                "executor_invalid_task_payload",
                "Document Executor Task Payload 缺少 document_id",
                retryable=False,
                blocked=True,
            )
        agent_context = AgentToolContext(
            trace_id=f"trace_{uuid4().hex}",
            agent_run_id=context.agent_run_id,
            agent_name=self._agent.name,
            conversation_id=context.conversation_id,
            turn_id=context.turn_id,
            task_id=context.task_id,
            actor_code=DOCUMENT_EXECUTOR_ACTOR_CODE,
            permissions=self._permissions,
            document_services=self._document_services,
            context_services=ContextToolServices(),
            plan_id=context.plan_id,
            workflow_id=context.workflow_id,
            execution_id=context.execution_id,
            operation_id=context.operation_id,
            task_document_id=document_id,
            attempt=context.attempt,
            audit_logger=self._audit_logger_factory(),
        )
        agent_input = json.dumps(
            {
                "executor_code": self._executor_code,
                "primary_tool_name": self._primary_tool_name,
                "task_payload": payload.model_dump(mode="json"),
            },
            ensure_ascii=False,
        )
        try:
            # 事务外执行 Agent 并通过 await_side_effect_quiescence 保护排空
            run_result = await await_side_effect_quiescence(
                self._runner(
                    self._agent,
                    agent_input,
                    context=agent_context,
                    max_turns=self._max_turns,
                    run_config=self._run_config,
                )
            )
        except TaskExecutionError:
            raise
        except Exception as exc:
            raise TaskExecutionError(
                "executor_agent_run_failed",
                "Executor Agent 运行失败",
                retryable=True,
            ) from exc

        # 校验 Command Tool Output
        try:
            output = self._validate_tool_output(run_result.final_output)
        except (TypeError, ValidationError, ValueError) as exc:
            raise TaskExecutionError(
                "executor_command_not_completed",
                "Executor Agent 未返回有效的 Command Tool Output",
                retryable=True,
            ) from exc

        # 校验授权范围一致性
        if output.document_id != document_id:
            raise TaskExecutionError(
                "task_scope_violation",
                "Command Tool Output 与 Task 资源范围不一致",
                retryable=False,
                blocked=True,
            )
        # 显式映射 outcome 分类
        if output.outcome == "rejected":
            raise TaskExecutionError(
                output.result_code,
                output.message,
                retryable=False,
                blocked=True,
            )
        if output.outcome == "failed":
            raise TaskExecutionError(
                output.result_code,
                output.message,
                retryable=output.retryable,
            )

        try:
            return self._output_adapter(output)
        except (TypeError, ValidationError, ValueError) as exc:
            raise TaskExecutionError(
                "executor_invalid_command_output",
                "Command Tool 成功输出不符合 Task Result 契约",
                retryable=False,
            ) from exc

    def _validate_tool_output(self, output: Any) -> ToolResult:
        """解析并校验 final_output 符合 ToolResult 契约。"""
        if isinstance(output, str):
            return self._tool_output_model.model_validate_json(output)
        return self._tool_output_model.model_validate(output)
