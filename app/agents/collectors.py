"""三个只读 Collector Agent 及其 Agent-as-Tool 包装模块。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

from agents import Agent, ModelSettings
from agents.items import ToolCallItem, ToolCallOutputItem
from agents.tool import FunctionTool
from pydantic import BaseModel, Field

from app.agent_runtime.context import AgentToolContext
from app.modules.context.agent_tools.catalog import CONTEXT_COLLECTOR_TOOLS
from app.modules.document.agent_tools.catalog import DOCUMENT_COLLECTOR_TOOLS
from app.modules.operations.agent_tools.catalog import OPERATIONS_COLLECTOR_TOOLS


class CollectorRequest(BaseModel):
    """Planner 提交给 Collector Agent 的结构化信息收集请求模型。"""

    question: str = Field(min_length=1, max_length=4000)
    conversation_id: str | None = Field(default=None, max_length=100)
    turn_id: str | None = Field(default=None, max_length=100)
    document_ids: list[int] = Field(default_factory=list)
    chain_ids: list[str] = Field(default_factory=list)
    workflow_ids: list[str] = Field(default_factory=list)
    operation_ids: list[str] = Field(default_factory=list)


# 支持的 Collector 代码标识
CollectorCode = Literal[
    "document_collector",
    "context_collector",
    "operations_collector",
]

# Planner 暴露的 Tool 名称到 Collector 代码的映射字典
COLLECTOR_TOOL_CODES: dict[str, CollectorCode] = {
    "collect_document_information": "document_collector",
    "collect_context_information": "context_collector",
    "collect_operation_information": "operations_collector",
}


class CollectorLLMResult(BaseModel):
    """Collector Agent 中 LLM 生成的结构化解释与缺口声明模型。"""

    summary: str = Field(min_length=1)
    gaps: list[str] = Field(default_factory=list)


class EvidenceItem(BaseModel):
    """Collector 内部单次只读 Function Tool 调用的底层真实执行记录。"""

    tool_name: str = Field(min_length=1)
    tool_call_id: str = Field(min_length=1)
    original_tool_call_id: str = Field(min_length=1)
    attempt_count: int = Field(ge=1)

    # ToolCallItem：实际查询输入。
    arguments: dict[str, Any] = Field(default_factory=dict)

    # ToolCallOutputItem：实际查询结果。
    outcome: Literal["succeeded", "rejected", "failed"]
    result_code: str = Field(min_length=1)
    message: str
    retryable: bool
    resource_refs: list[str] = Field(default_factory=list)

    # 公共结果 envelope 之外的 Tool-specific business output。
    payload: dict[str, Any] = Field(default_factory=dict)


class CollectorResult(BaseModel):
    """Runtime 真实执行证据与 LLM 解释确定性组合后的 Collector 对外输出契约。"""

    collector_code: CollectorCode
    summary: str = Field(min_length=1)
    gaps: list[str] = Field(default_factory=list)
    resource_refs: list[str] = Field(default_factory=list)
    evidence_items: list[EvidenceItem] = Field(default_factory=list)


@dataclass(frozen=True)
class CollectorAgentSet:
    """三个只读 Collector Agent 实例及其向 Planner 暴露的 Agent-as-Tool 工具元组。"""

    document: Agent[AgentToolContext]
    context: Agent[AgentToolContext]
    operations: Agent[AgentToolContext]
    planner_tools: tuple[FunctionTool, ...]


# Collector Agent 通用系统提示词指令
_COMMON_INSTRUCTIONS = """
你是只读信息收集 Agent。只能调用当前 Catalog 中提供的查询 Tool，
不能执行写操作，不能请求或执行 Handoff，不能规划业务 Task。

请围绕输入中的 question 和资源范围调用最少且足够的查询 Tool。

最终结构化输出只负责：
- summary：简洁总结查询 Tool 已验证的信息；
- gaps：列出调查后仍无法确认且与当前问题相关的信息；未调用 Tool 或 Tool failed
  本身不是 gap，应描述因此仍无法确认的业务事实。

不得编造 Tool 未返回的业务事实，不得把 rejected 或 failed Tool
结果描述成成功业务状态。

真实 Tool Call、Tool Output、资源引用和业务 Payload 由 Runtime
独立提取，不需要在 summary 中重复复制完整原始数据。
""".strip()


# 公共工具结果封套字段集合
_COMMON_EVIDENCE_FIELDS = frozenset(
    {
        "outcome",
        "result_code",
        "message",
        "retryable",
        "resource_refs",
    }
)
_REQUIRED_EVIDENCE_FIELDS = _COMMON_EVIDENCE_FIELDS


def _extract_tool_arguments(item: ToolCallItem) -> dict[str, Any]:
    """从 SDK 原始 ToolCallItem 中安全提取 JSON 对象形式的查询参数。"""
    raw_item = item.raw_item

    if isinstance(raw_item, dict):
        raw_arguments = raw_item.get("arguments")
    else:
        raw_arguments = getattr(raw_item, "arguments", None)

    if not isinstance(raw_arguments, str):
        raise ValueError("Collector ToolCall 缺少 arguments")

    try:
        parsed = json.loads(raw_arguments)
    except json.JSONDecodeError as exc:
        raise ValueError("Collector ToolCall arguments 不是有效 JSON") from exc

    if not isinstance(parsed, dict):
        raise ValueError("Collector ToolCall arguments 必须是 JSON object")

    return parsed


def _normalize_tool_output(output: Any) -> dict[str, Any]:
    """将 SDK ToolCallOutputItem 中的 output 归一化解析为 JSON 字典。"""
    if isinstance(output, BaseModel):
        return output.model_dump(mode="json")

    if isinstance(output, dict):
        return output

    if isinstance(output, str):
        parsed = json.loads(output)
        if not isinstance(parsed, dict):
            raise ValueError("Collector Tool output 必须是 JSON object")
        return parsed

    raise TypeError(f"Unsupported Collector Tool output: {type(output)!r}")


def _validate_tool_output_envelope(data: dict[str, Any]) -> None:
    """验证所有 Collector Query Tool 的输出字典是否完整包含公共封套字段。"""
    missing = _REQUIRED_EVIDENCE_FIELDS - data.keys()
    if missing:
        raise ValueError(
            f"Collector Tool output 缺少公共字段: {sorted(missing)}"
        )

    if data["outcome"] not in {"succeeded", "rejected", "failed"}:
        raise ValueError("非法 Tool outcome")


def _extract_evidence_items(new_items: list[Any]) -> list[EvidenceItem]:
    """按 call_id 严格配对 ToolCall 与 ToolOutput，构建不可变的 EvidenceItem 列表。"""
    calls: dict[str, tuple[str, dict[str, Any]]] = {}
    for item in new_items:
        if not isinstance(item, ToolCallItem):
            continue

        call_id = item.call_id
        tool_name = item.tool_name
        if not call_id or not tool_name:
            raise ValueError("Collector ToolCall 缺少 call_id 或 tool_name")
        if call_id in calls:
            raise ValueError(f"重复 Tool call_id: {call_id}")
        calls[call_id] = (
            tool_name,
            _extract_tool_arguments(item),
        )

    evidence_items: list[EvidenceItem] = []
    consumed_call_ids: set[str] = set()
    for item in new_items:
        if not isinstance(item, ToolCallOutputItem):
            continue

        call_id = item.call_id
        if not call_id:
            raise ValueError("Collector ToolCallOutput 缺少 call_id")

        call = calls.get(call_id)
        if call is None:
            raise ValueError(
                f"找不到 ToolCallOutput 对应的 ToolCall: {call_id}"
            )
        if call_id in consumed_call_ids:
            raise ValueError(f"同一 ToolCall 出现多个 Output: {call_id}")

        tool_name, arguments = call
        data = _normalize_tool_output(item.output)
        _validate_tool_output_envelope(data)
        payload = {
            key: value
            for key, value in data.items()
            if key not in _COMMON_EVIDENCE_FIELDS
        }
        evidence_items.append(
            EvidenceItem(
                tool_name=tool_name,
                tool_call_id=call_id,
                arguments=arguments,
                outcome=data["outcome"],
                result_code=data["result_code"],
                message=data["message"],
                retryable=data["retryable"],
                resource_refs=data["resource_refs"],
                original_tool_call_id=call_id,
                attempt_count=1,
                payload=payload,
            )
        )
        consumed_call_ids.add(call_id)

    missing_outputs = set(calls) - consumed_call_ids
    if missing_outputs:
        raise ValueError(
            f"Collector ToolCall 缺少 Output: {sorted(missing_outputs)}"
        )

    return evidence_items


def extract_collector_results(new_items: list[Any]) -> list[CollectorResult]:
    """从顶层 Evidence Run 的输出条目中提取并配对各 Collector 的结构化结果。"""
    calls: dict[str, CollectorCode] = {}
    for item in new_items:
        if not isinstance(item, ToolCallItem):
            continue

        call_id = item.call_id
        tool_name = item.tool_name
        if not call_id or not tool_name:
            raise ValueError("Evidence Collector ToolCall 缺少 call_id 或 tool_name")
        if call_id in calls:
            raise ValueError(f"重复 Evidence Tool call_id: {call_id}")

        collector_code = COLLECTOR_TOOL_CODES.get(tool_name)
        if collector_code is None:
            raise ValueError(f"Evidence Run 包含非 Collector Tool: {tool_name}")
        calls[call_id] = collector_code

    results: list[CollectorResult] = []
    consumed_call_ids: set[str] = set()
    for item in new_items:
        if not isinstance(item, ToolCallOutputItem):
            continue

        call_id = item.call_id
        if not call_id:
            raise ValueError("Evidence Collector ToolCallOutput 缺少 call_id")
        expected_code = calls.get(call_id)
        if expected_code is None:
            raise ValueError(
                "找不到 Evidence Collector ToolCallOutput 对应的 ToolCall: "
                f"{call_id}"
            )
        if call_id in consumed_call_ids:
            raise ValueError(
                f"同一 Evidence Collector ToolCall 出现多个 Output: {call_id}"
            )

        result = CollectorResult.model_validate(
            _normalize_tool_output(item.output)
        )
        if result.collector_code != expected_code:
            raise ValueError(
                "CollectorResult 来源与调用 Tool 不一致: "
                f"expected={expected_code}, actual={result.collector_code}"
            )
        results.append(result)
        consumed_call_ids.add(call_id)

    missing_outputs = set(calls) - consumed_call_ids
    if missing_outputs:
        raise ValueError(
            "Evidence Collector ToolCall 缺少 Output: "
            f"{sorted(missing_outputs)}"
        )
    return results


def _build_collector_output_extractor(collector_code: CollectorCode):
    """为指定 Collector 创建固定来源代码的确定性输出提取器函数。"""

    async def extract(run_result) -> str:
        llm_result = CollectorLLMResult.model_validate(
            run_result.final_output
        )
        evidence_items = _extract_evidence_items(run_result.new_items)
        resource_refs = list(
            dict.fromkeys(
                resource_ref
                for item in evidence_items
                for resource_ref in item.resource_refs
            )
        )
        result = CollectorResult(
            collector_code=collector_code,
            summary=llm_result.summary,
            gaps=llm_result.gaps,
            resource_refs=resource_refs,
            evidence_items=evidence_items,
        )
        return result.model_dump_json()

    return extract


def _instructions(responsibility: str) -> str:
    """组合通用只读指令与特定 Collector 职责说明。"""
    return f"{_COMMON_INSTRUCTIONS}\n\n{responsibility}"


def build_collector_agents(
    *,
    model: Any,
    model_settings: ModelSettings,
) -> CollectorAgentSet:
    """初始化构建三个只读 Collector Agent，并将其包装为 Planner 可调用的 FunctionTool。"""
    collector_settings = model_settings.resolve(
        ModelSettings(parallel_tool_calls=False)
    )
    document_agent = Agent[AgentToolContext](
        name="Document Collector Agent",
        instructions=_instructions(
            "只收集 Document、Artifact、ParentBlock、ChildChunk 和流水线状态。",
        ),
        tools=list(DOCUMENT_COLLECTOR_TOOLS),
        handoffs=[],
        model=model,
        model_settings=collector_settings,
        output_type=CollectorLLMResult,
    )
    context_agent = Agent[AgentToolContext](
        name="Context Collector Agent",
        instructions=_instructions(
            "只收集 Turn、Chain、Node、Resource 和 SelectionRecord 的持久化事实。",
        ),
        tools=list(CONTEXT_COLLECTOR_TOOLS),
        handoffs=[],
        model=model,
        model_settings=collector_settings,
        output_type=CollectorLLMResult,
    )
    operations_agent = Agent[AgentToolContext](
        name="Operations Collector Agent",
        instructions=_instructions(
            "只收集文档业务日志和 Agent Tool 审计事实。",
        ),
        tools=list(OPERATIONS_COLLECTOR_TOOLS),
        handoffs=[],
        model=model,
        model_settings=collector_settings,
        output_type=CollectorLLMResult,
    )
    planner_tools = (
        document_agent.as_tool(
            tool_name="collect_document_information",
            tool_description="收集 Document 及其处理流水线的只读事实。",
            parameters=CollectorRequest,
            include_input_schema=True,
            max_turns=8,
            custom_output_extractor=_build_collector_output_extractor(
                "document_collector"
            ),
        ),
        context_agent.as_tool(
            tool_name="collect_context_information",
            tool_description="收集 Conversation、Turn 和 Context Chain 的只读事实。",
            parameters=CollectorRequest,
            include_input_schema=True,
            max_turns=8,
            custom_output_extractor=_build_collector_output_extractor(
                "context_collector"
            ),
        ),
        operations_agent.as_tool(
            tool_name="collect_operation_information",
            tool_description="收集业务日志与 Agent Tool 审计的只读事实。",
            parameters=CollectorRequest,
            include_input_schema=True,
            max_turns=8,
            custom_output_extractor=_build_collector_output_extractor(
                "operations_collector"
            ),
        ),
    )
    return CollectorAgentSet(
        document=document_agent,
        context=context_agent,
        operations=operations_agent,
        planner_tools=planner_tools,
    )
