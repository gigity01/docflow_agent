"""Planner Evidence、Gap 判断、Commit 及其 LangGraph 编排模块。"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Literal, NotRequired, TypedDict

from agents import (
    Agent,
    ModelSettings,
    RunContextWrapper,
    RunConfig,
    Runner,
    StopAtTools,
    ToolExecutionConfig,
    handoff,
)
from agents.tool import FunctionTool
from agents.tool_context import ToolContext
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime
from pydantic import BaseModel, Field

from app.agent_runtime.business_docs import load_service_map
from app.agent_runtime.context import AgentToolContext
from app.agents.collectors import (
    CollectorAgentSet,
    CollectorResult,
    EvidenceItem,
    extract_collector_results,
)
from app.agents.gap_handler import (
    EvidenceRound,
    GapAction,
    GapDecision,
    GapDecisionError,
    GapHandlerInput,
    build_gap_handler_agent,
    parse_gap_decision,
    validate_gap_decision,
)
from app.modules.context.agent_tools.catalog import CONTEXT_COLLECTOR_TOOLS
from app.modules.document.agent_tools.catalog import DOCUMENT_COLLECTOR_TOOLS
from app.modules.operations.agent_tools.catalog import OPERATIONS_COLLECTOR_TOOLS
from app.modules.planning.agent_tools.catalog import PLANNER_TOOLS
from app.modules.planning.application.dto import (
    MarkPlanNeedsClarificationInput,
    MarkPlanUnsupportedInput,
    PlannerContextInput,
)
from app.modules.planning.application.errors import PlanningRetryRequested

DEFAULT_PLANNER_MAX_TURNS = 12
PLANNER_EVIDENCE_MAX_FUNCTION_TOOL_CONCURRENCY = 3
PLANNER_COMMIT_MAX_FUNCTION_TOOL_CONCURRENCY = 1
# 单轮 Evidence 首次失败后允许的额外局部调用次数
MAX_EVIDENCE_RETRY_COUNT = 2
_COLLECTOR_QUERY_TOOLS = (
    *DOCUMENT_COLLECTOR_TOOLS,
    *CONTEXT_COLLECTOR_TOOLS,
    *OPERATIONS_COLLECTOR_TOOLS,
)


class _PlannerWorkflowInput(TypedDict):
    """LangGraph Planner 工作流的输入状态字典。"""

    planner_input: PlannerContextInput


class _PlannerWorkflowOutput(TypedDict):
    """LangGraph Planner 工作流的输出状态字典。"""

    final_result: Any


class _PlannerWorkflowState(TypedDict):
    """LangGraph Planner 工作流节点间流转的内部完整状态字典。"""

    planner_input: PlannerContextInput
    evidence_rounds: NotRequired[list[EvidenceRound]]
    evidence_history: NotRequired[list[Any]]
    gap_result: NotRequired[Any]
    gap_decision: NotRequired[GapDecision]
    final_result: NotRequired[Any]
    local_retry_counts: NotRequired[dict[int, int]]
    active_evidence_round: NotRequired[Literal[1, 2]]


class ClarificationHandoffInput(BaseModel):
    """转交 Clarification Agent 生成用户澄清问题时的结构化载荷。"""

    clarification_kind: Literal[
        "resource",
        "intent",
        "missing_parameter",
    ]
    reason: str = Field(min_length=1, max_length=4000)
    required_information: list[str] = Field(min_length=1, max_length=10)
    known_resource_refs: list[str] = Field(default_factory=list, max_length=50)


class ClarificationAgentOutput(BaseModel):
    """Clarification Agent 输出的单一自然语言澄清问题模型。"""

    question: str = Field(min_length=1, max_length=4000)


@dataclass(frozen=True)
class PlannerAgentRunner:
    """使用进程内 StateGraph 编排 Evidence 取证、Gap 缺口判断与 Commit 提交的编排运行器。"""

    evidence_agent: Agent[AgentToolContext]
    gap_handler_agent: Agent[AgentToolContext]
    commit_agent: Agent[AgentToolContext]
    clarification_agent: Agent[AgentToolContext]
    evidence_run_config: RunConfig
    gap_handler_run_config: RunConfig
    commit_run_config: RunConfig
    clarification_run_config: RunConfig
    max_turns: int = DEFAULT_PLANNER_MAX_TURNS
    _workflow: Any = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        """初始化编译 LangGraph 工作流状态图。"""
        object.__setattr__(self, "_workflow", self._build_workflow())

    async def run(
        self,
        *,
        planner_input: PlannerContextInput,
        context: AgentToolContext,
    ) -> Any:
        """异步执行完整的 Planning 工作流状态图。"""
        result = await self._workflow.ainvoke(
            {"planner_input": planner_input},
            context=context,
        )
        return result["final_result"]

    def _build_workflow(self) -> Any:
        """构建并编译 LangGraph StateGraph 状态机图。"""
        workflow = StateGraph(
            _PlannerWorkflowState,
            context_schema=AgentToolContext,
            input_schema=_PlannerWorkflowInput,
            output_schema=_PlannerWorkflowOutput,
        )
        workflow.add_node(
            "collect_initial_evidence",
            self._collect_initial_evidence,
        )
        workflow.add_node("assess_initial_gap", self._assess_initial_gap)
        workflow.add_node(
            "collect_follow_up_evidence",
            self._collect_follow_up_evidence,
        )
        workflow.add_node("assess_final_gap", self._assess_final_gap)
        workflow.add_node("commit_plan", self._commit_plan)
        workflow.add_node("resolve_gap", self._resolve_gap)
        workflow.add_node(
            "retry_failed_evidence",
            self._retry_failed_evidence,
        )

        workflow.add_edge(START, "collect_initial_evidence")
        workflow.add_conditional_edges(
            "collect_initial_evidence",
            _route_initial_evidence,
            {
                "commit": "commit_plan",
                "assess_gap": "assess_initial_gap",
            },
        )
        workflow.add_conditional_edges(
            "assess_initial_gap",
            _route_initial_gap,
            {
                "collect_more": "collect_follow_up_evidence",
                "retry": "retry_failed_evidence",
                "resolve": "resolve_gap",
            },
        )
        workflow.add_edge(
            "collect_follow_up_evidence",
            "assess_final_gap",
        )
        workflow.add_conditional_edges(
            "assess_final_gap",
            _route_final_gap,
            {
                "retry": "retry_failed_evidence",
                "resolve": "resolve_gap",
            },
        )
        workflow.add_conditional_edges(
            "retry_failed_evidence",
            _route_retry_to_gap_handler,
            {
                "initial": "assess_initial_gap",
                "final": "assess_final_gap",
            },
        )
        workflow.add_edge("commit_plan", END)
        workflow.add_edge("resolve_gap", END)
        return workflow.compile(name="planner_workflow")

    async def _collect_initial_evidence(
        self,
        state: _PlannerWorkflowState,
        runtime: Runtime[AgentToolContext],
    ) -> dict[str, Any]:
        """状态图节点：执行第一轮 Evidence Agent 取证。"""
        first_evidence_result = await Runner.run(
            self.evidence_agent,
            state["planner_input"].model_dump_json(indent=2),
            context=runtime.context,
            max_turns=self.max_turns,
            run_config=self.evidence_run_config,
        )
        first_collector_results = extract_collector_results(
            first_evidence_result.new_items
        )
        evidence_rounds = [
            EvidenceRound(
                round_number=1,
                collector_results=first_collector_results,
            )
        ]
        return {
            "evidence_rounds": evidence_rounds,
            "evidence_history": first_evidence_result.to_input_list(),
            "local_retry_counts": {1: 0, 2: 0},
            "active_evidence_round": 1,
        }

    async def _assess_initial_gap(
        self,
        state: _PlannerWorkflowState,
        runtime: Runtime[AgentToolContext],
    ) -> dict[str, Any]:
        """状态图节点：执行首轮缺口评估（允许 COLLECT_MORE）。"""
        first_gap_result, decision = await self._run_gap_handler(
            planner_input=state["planner_input"],
            evidence_rounds=state["evidence_rounds"],
            collect_more_allowed=True,
            context=runtime.context,
        )
        return {
            "gap_result": first_gap_result,
            "gap_decision": decision,
        }

    async def _collect_follow_up_evidence(
        self,
        state: _PlannerWorkflowState,
        runtime: Runtime[AgentToolContext],
    ) -> dict[str, Any]:
        """状态图节点：执行第二轮定向补充取证。"""
        decision = state["gap_decision"]
        second_evidence_result = await Runner.run(
            self.evidence_agent,
            _append_internal_message(
                state["evidence_history"],
                "Evidence Follow-up",
                {
                    "instruction": decision.follow_up,
                    "constraint": (
                        "只补充该未知事实；不得重复已经成功完成且与该未知无关的查询。"
                    ),
                },
            ),
            context=runtime.context,
            max_turns=self.max_turns,
            run_config=self.evidence_run_config,
        )
        return {
            "evidence_rounds": [
                *state["evidence_rounds"],
                EvidenceRound(
                    round_number=2,
                    collector_results=extract_collector_results(
                        second_evidence_result.new_items
                    ),
                ),
            ],
            "evidence_history": second_evidence_result.to_input_list(),
            "active_evidence_round": 2,
        }

    async def _assess_final_gap(
        self,
        state: _PlannerWorkflowState,
        runtime: Runtime[AgentToolContext],
    ) -> dict[str, Any]:
        """状态图节点：执行终轮缺口评估（禁止再次 COLLECT_MORE）。"""
        second_gap_result, second_decision = await self._run_gap_handler(
            planner_input=state["planner_input"],
            evidence_rounds=state["evidence_rounds"],
            collect_more_allowed=False,
            context=runtime.context,
        )
        return {
            "gap_result": second_gap_result,
            "gap_decision": second_decision,
        }

    async def _commit_plan(
        self,
        state: _PlannerWorkflowState,
        runtime: Runtime[AgentToolContext],
    ) -> dict[str, Any]:
        """状态图节点：直接进入 Commit 阶段创建并发布 Plan。"""
        return {
            "final_result": await self._run_commit(
                evidence_history=state["evidence_history"],
                context=runtime.context,
            )
        }

    async def _retry_failed_evidence(
        self,
        state: _PlannerWorkflowState,
        runtime: Runtime[AgentToolContext],
    ) -> dict[str, Any]:
        """只重新执行当前 Evidence 轮次中仍可重试的失败 Query Tool。"""
        active_round = state["active_evidence_round"]
        current_round = _get_evidence_round(
            state["evidence_rounds"],
            active_round,
        )
        failed_items = _retryable_failed_items(current_round)
        if not failed_items:
            raise PlanningRetryRequested(state["gap_decision"].reason)

        retry_counts = dict(state["local_retry_counts"])
        retry_count = retry_counts[active_round] + 1
        retry_counts[active_round] = retry_count
        retry_attempts: list[dict[str, Any]] = []
        replacements: dict[str, EvidenceItem] = {}

        for failed_item in failed_items:
            retried_item = await _invoke_evidence_retry(
                failed_item=failed_item,
                retry_count=retry_count,
                context=runtime.context,
            )
            replacements[_evidence_query_key(failed_item)] = retried_item
            retry_attempts.append(
                {
                    "tool_name": failed_item.tool_name,
                    "arguments": failed_item.arguments,
                    "failure_reason": failed_item.message,
                    "current_retry_count": retry_count,
                    "result": retried_item.model_dump(mode="json"),
                }
            )

        merged_round = _replace_evidence_items(current_round, replacements)
        return {
            "evidence_rounds": [
                merged_round if item.round_number == active_round else item
                for item in state["evidence_rounds"]
            ],
            "evidence_history": _append_internal_message(
                state["evidence_history"],
                "Evidence Local Retry",
                {
                    "round_number": active_round,
                    "retry_count": retry_count,
                    "attempts": retry_attempts,
                },
            ),
            "local_retry_counts": retry_counts,
        }

    async def _resolve_gap(
        self,
        state: _PlannerWorkflowState,
        runtime: Runtime[AgentToolContext],
    ) -> dict[str, Any]:
        """状态图节点：根据 Gap 决策终态执行对应动作。"""
        return {
            "final_result": await self._resolve_gap_decision(
                decision=state["gap_decision"],
                gap_result=state["gap_result"],
                evidence_history=state["evidence_history"],
                context=runtime.context,
            )
        }

    async def _run_gap_handler(
        self,
        *,
        planner_input: PlannerContextInput,
        evidence_rounds: list[EvidenceRound],
        collect_more_allowed: bool,
        context: AgentToolContext,
    ) -> tuple[Any, GapDecision]:
        """封装调用 GapHandler Agent 并解析校验其决策。"""
        handler_input = GapHandlerInput(
            current_user_input=planner_input.current_user_input,
            selected_context=planner_input.context_chains,
            evidence_rounds=evidence_rounds,
            collect_more_allowed=collect_more_allowed,
        )
        result = await Runner.run(
            self.gap_handler_agent,
            handler_input.model_dump_json(indent=2),
            context=context,
            max_turns=self.max_turns,
            run_config=self.gap_handler_run_config,
        )
        decision = parse_gap_decision(result.final_output)
        validate_gap_decision(decision, handler_input)
        return result, decision

    async def _resolve_gap_decision(
        self,
        *,
        decision: GapDecision,
        gap_result: Any,
        evidence_history: list[Any],
        context: AgentToolContext,
    ) -> Any:
        """根据 GapDecision 动作类型进行最终分支分发。"""
        if decision.action == GapAction.COMMIT:
            return await self._run_commit(
                evidence_history=_append_internal_message(
                    evidence_history,
                    "Gap Decision",
                    decision.model_dump(mode="json"),
                ),
                context=context,
            )
        if decision.action == GapAction.RETRY:
            raise PlanningRetryRequested(decision.reason)
        if decision.action == GapAction.SYSTEM_FAILURE:
            raise RuntimeError(
                f"Planning 前置取证发生系统故障: {decision.reason}"
            )
        if decision.action == GapAction.UNSUPPORTED:
            await _mark_gap_unsupported(context, decision.reason)
            return gap_result
        if decision.action == GapAction.CLARIFICATION:
            clarification = ClarificationHandoffInput(
                clarification_kind=decision.clarification_kind,
                reason=decision.reason,
                required_information=decision.required_information,
                known_resource_refs=decision.known_resource_refs,
            )
            await _mark_gap_clarification(context, clarification)
            return await Runner.run(
                self.clarification_agent,
                clarification.model_dump_json(indent=2),
                context=context,
                max_turns=self.max_turns,
                run_config=self.clarification_run_config,
            )
        raise GapDecisionError("COLLECT_MORE 未在 Evidence 补查边界内处理")

    async def _run_commit(
        self,
        *,
        evidence_history: list[Any],
        context: AgentToolContext,
    ) -> Any:
        """运行 Commit Agent 创建 Task 并提交 Plan。"""
        return await Runner.run(
            self.commit_agent,
            evidence_history,
            context=context,
            max_turns=self.max_turns,
            run_config=self.commit_run_config,
        )


def _route_initial_evidence(
    state: _PlannerWorkflowState,
) -> Literal["commit", "assess_gap"]:
    """条件路由函数：判断第一轮取证后是否直接可 Commit。"""
    first_round = state["evidence_rounds"][0]
    if _evidence_is_ready_for_commit(first_round.collector_results):
        return "commit"
    return "assess_gap"


def _route_initial_gap(
    state: _PlannerWorkflowState,
) -> Literal["collect_more", "retry", "resolve"]:
    """条件路由函数：判断首轮 Gap 评估后是否需要发起补查。"""
    if state["gap_decision"].action == GapAction.COLLECT_MORE:
        return "collect_more"
    if _can_retry_active_evidence(state):
        return "retry"
    return "resolve"


def _route_final_gap(
    state: _PlannerWorkflowState,
) -> Literal["retry", "resolve"]:
    """条件路由函数：判断终轮 Gap 评估后是否执行局部重试。"""
    if _can_retry_active_evidence(state):
        return "retry"
    return "resolve"


def _route_retry_to_gap_handler(
    state: _PlannerWorkflowState,
) -> Literal["initial", "final"]:
    """局部重试后回到触发该重试的原 GapHandler 节点。"""
    if state["active_evidence_round"] == 1:
        return "initial"
    return "final"


def _can_retry_active_evidence(state: _PlannerWorkflowState) -> bool:
    """判断 RETRY 决策是否仍有当前轮次的局部调用额度与失败项。"""
    if state["gap_decision"].action != GapAction.RETRY:
        return False
    active_round = state["active_evidence_round"]
    if state["local_retry_counts"][active_round] >= MAX_EVIDENCE_RETRY_COUNT:
        return False
    evidence_round = _get_evidence_round(
        state["evidence_rounds"],
        active_round,
    )
    return bool(_retryable_failed_items(evidence_round))


def _get_evidence_round(
    evidence_rounds: list[EvidenceRound],
    round_number: Literal[1, 2],
) -> EvidenceRound:
    """按轮次取得唯一 EvidenceRound。"""
    for evidence_round in evidence_rounds:
        if evidence_round.round_number == round_number:
            return evidence_round
    raise RuntimeError(f"缺少 Evidence round {round_number}")


def _evidence_query_key(item: EvidenceItem) -> str:
    """生成 `tool_name + arguments` 的稳定逻辑查询标识。"""
    canonical_arguments = json.dumps(
        item.arguments,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return f"{item.tool_name}:{canonical_arguments}"


def _retryable_failed_items(
    evidence_round: EvidenceRound,
) -> list[EvidenceItem]:
    """稳定去重取得当前轮次中可局部重试的失败查询。"""
    failed_by_query: dict[str, EvidenceItem] = {}
    for result in evidence_round.collector_results:
        for item in result.evidence_items:
            if item.outcome == "failed" and item.retryable:
                failed_by_query.setdefault(_evidence_query_key(item), item)
    return list(failed_by_query.values())


def _resolve_evidence_tool(tool_name: str) -> FunctionTool:
    """只从三个 Collector 的只读 Query Catalog 解析 Tool。"""
    matches = [
        tool for tool in _COLLECTOR_QUERY_TOOLS if tool.name == tool_name
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"Evidence retry Tool 必须在只读 Catalog 中唯一注册: {tool_name}"
        )
    return matches[0]


async def _invoke_evidence_retry(
    *,
    failed_item: EvidenceItem,
    retry_count: int,
    context: AgentToolContext,
) -> EvidenceItem:
    """使用原始 arguments 直接重放一个失败的只读 Query Tool。"""
    tool = _resolve_evidence_tool(failed_item.tool_name)
    arguments_json = json.dumps(failed_item.arguments, ensure_ascii=False)
    tool_call_id = (
        f"{failed_item.original_tool_call_id}-local-retry-{retry_count}"
    )
    output = await tool.on_invoke_tool(
        ToolContext(
            context=context,
            tool_name=tool.name,
            tool_call_id=tool_call_id,
            tool_arguments=arguments_json,
        ),
        arguments_json,
    )
    output_data = _normalize_retry_tool_output(output)
    required_fields = {
        "outcome",
        "result_code",
        "message",
        "retryable",
        "resource_refs",
    }
    missing_fields = required_fields - output_data.keys()
    if missing_fields:
        raise ValueError(
            "Evidence retry Tool output 缺少公共字段: "
            f"{sorted(missing_fields)}"
        )
    payload = {
        key: value
        for key, value in output_data.items()
        if key not in required_fields
    }
    return EvidenceItem(
        tool_name=failed_item.tool_name,
        tool_call_id=tool_call_id,
        original_tool_call_id=failed_item.original_tool_call_id,
        attempt_count=failed_item.attempt_count + 1,
        arguments=failed_item.arguments,
        outcome=output_data["outcome"],
        result_code=output_data["result_code"],
        message=output_data["message"],
        retryable=output_data["retryable"],
        resource_refs=output_data["resource_refs"],
        payload=payload,
    )


def _normalize_retry_tool_output(output: Any) -> dict[str, Any]:
    """把直接调用 FunctionTool 的输出归一化为 JSON object。"""
    if isinstance(output, BaseModel):
        return output.model_dump(mode="json")
    if isinstance(output, dict):
        return output
    if isinstance(output, str):
        parsed = json.loads(output)
        if isinstance(parsed, dict):
            return parsed
    raise TypeError("Evidence retry Tool output 必须是 JSON object")


def _replace_evidence_items(
    evidence_round: EvidenceRound,
    replacements: dict[str, EvidenceItem],
) -> EvidenceRound:
    """按逻辑查询标识替换当前有效结果，并保留其他 Evidence。"""
    collector_results: list[CollectorResult] = []
    for result in evidence_round.collector_results:
        evidence_items = [
            replacements.get(_evidence_query_key(item), item)
            for item in result.evidence_items
        ]
        collector_results.append(
            result.model_copy(
                update={
                    "evidence_items": evidence_items,
                    "resource_refs": list(
                        dict.fromkeys(
                            resource_ref
                            for item in evidence_items
                            for resource_ref in item.resource_refs
                        )
                    ),
                }
            )
        )
    return evidence_round.model_copy(
        update={"collector_results": collector_results}
    )


def _evidence_is_ready_for_commit(results: list[CollectorResult]) -> bool:
    """检查取证结果是否达到无缺口直通 Commit 的严格标准。"""
    evidence_items = [
        item for result in results for item in result.evidence_items
    ]
    return (
        bool(evidence_items)
        and all(item.outcome == "succeeded" for item in evidence_items)
        and not any(
            gap.strip() for result in results for gap in result.gaps
        )
    )


def _append_internal_message(
    history: list[Any],
    label: str,
    payload: Any,
) -> list[Any]:
    """向消息历史中追加内部系统/状态机提示消息。"""
    return [
        *history,
        {
            "role": "user",
            "content": (
                f"{label}:\n"
                f"{json.dumps(payload, ensure_ascii=False, indent=2)}"
            ),
        },
    ]


def _planning_scope(
    context: AgentToolContext,
) -> tuple[Any, str, str]:
    """提取 Planning 上下文必要服务与 ID，确保不为空。"""
    if (
        context.planning_services is None
        or context.plan_id is None
        or context.conversation_id is None
    ):
        raise RuntimeError("Gap 决策缺少 Planning 上下文")
    return context.planning_services, context.plan_id, context.conversation_id


async def _mark_gap_unsupported(
    context: AgentToolContext,
    reason: str,
) -> None:
    """在线程池中调用 Use Case 将 Plan 标记为 unsupported。"""
    services, plan_id, _ = _planning_scope(context)
    await asyncio.to_thread(
        services.mark_plan_unsupported.execute,
        MarkPlanUnsupportedInput(plan_id=plan_id, reason=reason),
    )


async def _mark_gap_clarification(
    context: AgentToolContext,
    data: ClarificationHandoffInput,
) -> None:
    """在线程池中调用 Use Case 将 Plan 标记为 needs_clarification 并持久化澄清请求。"""
    services, plan_id, conversation_id = _planning_scope(context)
    await asyncio.to_thread(
        services.mark_plan_needs_clarification.execute,
        MarkPlanNeedsClarificationInput(
            plan_id=plan_id,
            conversation_id=conversation_id,
            kind=data.clarification_kind,
            reason=data.reason,
            required_information=data.required_information,
            known_resource_refs=data.known_resource_refs,
        ),
    )


_SERVICE_MAP = load_service_map()

_PLANNER_BASE_INSTRUCTIONS = f"""
你是业务 Planner。输入中的 current_user_input 是当前请求，context_chains 只包含
Context Selection 授权读取的完整历史 Chain；当前 Turn 不属于这些历史 Chain。
你的职责是只基于可验证
事实创建一个可执行 Plan，不执行 Task，也不把最终文本当作业务事实。Planner 决定
WHAT，Task Runtime 和受限 Executor 决定 HOW。

以下 Service Map 只帮助理解请求所属业务，不授予 Tool 权限，也不扩大 Context Read Set：

{_SERVICE_MAP}
""".strip()

_PLANNER_EVIDENCE_INSTRUCTIONS = f"""
{_PLANNER_BASE_INSTRUCTIONS}

当前是 Evidence Phase。根据请求判断需要哪些可验证事实，自主选择 Document、Context
或 Operations Collector Tool；不得调用与当前规划无关的 Collector，也不得重复相同
取证。多个彼此独立的 Collector 可以在同一轮发出，每轮最多 3 个 Tool Call。

Collector 已经返回足够证据的事实，不得重复调查。对于当前规划所需但仍无法确认的事实，
必须明确写入对应 Collector 的 gap；不得因为 Tool 未调用而默认事实成立。
gap 只描述未知事实，不决定 clarification、retry、unsupported 或后续规划动作。

你在本阶段不能创建、发布或终止 Plan，也不能向用户澄清。收集完所需事实后，只生成
简短的取证就绪说明并结束本阶段；Collector Tool Call 与结构化 Tool Output 会由运行时
完整传递给 Commit Phase，不得编造或改写其中的事实。
""".strip()

_PLANNER_COMMIT_INSTRUCTIONS = f"""
{_PLANNER_BASE_INSTRUCTIONS}

当前是 Commit Phase。之前的输入历史已经包含 Evidence Phase 的 Collector Tool Call、
结构化 Tool Output 和取证就绪说明。前置 Gap 层已经确认现有 Evidence 足够进入规划；
本阶段不得重新分类 gap，也不能再次调用 Collector，只能基于现有证据创建 Task、发布
Plan，或处理执行 Capability 本身不支持、用户意图/参数仍不唯一的情况。

Collector Tool Output 中：
1. evidence_items 是 Runtime 从实际 Query Tool 调用中确定性提取的证据，
   是规划业务状态时的主要依据。
2. summary 和 gaps 是 Collector 对证据的解释，仅用于辅助理解；
   不得使用 summary 覆盖或修改 evidence_items。
3. outcome=succeeded 只表示 Query Tool 成功执行，
   不表示 payload 中业务资源的状态为 succeeded。
4. 业务状态必须读取 succeeded EvidenceItem 的 payload。
5. rejected / failed EvidenceItem 的 payload 不得作为成功业务事实；
   应结合 outcome、result_code、message、retryable 判断本次取证是否足够。
6. resource_refs 只表示本次调查涉及的资源，
   不证明资源存在或状态正常。
7. summary 与 evidence_items 冲突时，以 evidence_items 为准。
8. EvidenceItem.arguments 表示 Query Tool 实际使用的查询条件；payload 表示该查询
   实际返回的业务数据。判断 Evidence 是否支持某项业务结论时必须同时考虑
   arguments 与 payload。
9. evidence_items 为空只表示该 Collector 未提供可验证业务证据，不表示任何业务状态
   已经验证，不得仅根据 summary 创建依赖业务状态的 Task。
   Tool succeeded 但业务对象不存在仍是有效查询事实，必须按 payload 的空结果理解。

必须遵守以下约束：
1. 只使用 Planning Function Tools 创建 Task。每项 Task 的 sequence 从 1 开始、
   唯一且连续，总数必须为 1～10。不要重试已经返回 succeeded 的创建调用。
2. 支持当前请求时，确认所有 Task 创建成功后调用 finalize_plan。无法由当前三个
   Capability 完成时调用 mark_plan_unsupported，并提供简短明确的原因。
3. 不得在 finalize_plan 或 mark_plan_unsupported 成功前结束运行。Tool 返回 rejected
   或 failed 时不得伪装成成功；无法安全恢复时结束运行，由 Application 标记重试。
4. 只有创建 Task 时仍发现用户意图有多种合理解释或缺少必要业务参数，才使用
   clarification_handoff；不得把系统状态或可由 Collector 验证的信息转成澄清问题。
5. 只通过 task_ref 和 depends_on_task_refs 表达 DAG；不得直接创建依赖边、Outbox 或
   Task Runtime 行为。DAG 最大深度为 3，同一 Plan Task 总数不超过 10。

最终 Plan 状态、Task 状态和 task_ids 全部以数据库为准。不要生成业务总结；
finalize_plan 或 mark_plan_unsupported 成功后当前 Run 会立即结束。
""".strip()


def build_planner_agent(
    *,
    model: Any,
    model_settings: ModelSettings,
    collectors: CollectorAgentSet,
) -> PlannerAgentRunner:
    """初始化装配物理隔离的 Evidence、GapHandler、Commit 与 Clarification 各阶段 Agent。"""
    evidence_settings = model_settings.resolve(
        ModelSettings(parallel_tool_calls=True)
    )
    commit_settings = model_settings.resolve(
        ModelSettings(parallel_tool_calls=False)
    )
    gap_handler_agent = build_gap_handler_agent(
        model=model,
        model_settings=model_settings,
    )
    clarification_agent = Agent[AgentToolContext](
        name="Clarification Agent",
        handoff_description="把已确认的信息缺口转成一个简洁、可回答的问题。",
        instructions=(
            "根据输入或 Handoff 历史中的结构化缺口，只生成一个直接问题。"
            "不得规划 Task、调用 Tool 或假设用户答案。"
        ),
        tools=[],
        handoffs=[],
        model=model,
        model_settings=commit_settings,
        output_type=ClarificationAgentOutput,
    )

    async def on_clarification_handoff(
        ctx: RunContextWrapper[AgentToolContext],
        data: ClarificationHandoffInput,
    ) -> None:
        """Commit Agent 决定转交 Clarification 时的 Handoff 回调函数。"""
        await _mark_gap_clarification(ctx.context, data)

    clarification_handoff = handoff(
        clarification_agent,
        tool_name_override="clarification_handoff",
        tool_description_override="转交 Clarification Agent 生成用户澄清问题。",
        on_handoff=on_clarification_handoff,
        input_type=ClarificationHandoffInput,
    )

    base_planner = Agent[AgentToolContext](
        name="Planner Agent",
        instructions=_PLANNER_BASE_INSTRUCTIONS,
        tools=[],
        handoffs=[],
        model=model,
        model_settings=model_settings,
        output_type=None,
    )
    evidence_agent = base_planner.clone(
        instructions=_PLANNER_EVIDENCE_INSTRUCTIONS,
        tools=list(collectors.planner_tools),
        handoffs=[],
        model_settings=evidence_settings,
    )
    commit_agent = base_planner.clone(
        instructions=_PLANNER_COMMIT_INSTRUCTIONS,
        tools=list(PLANNER_TOOLS),
        handoffs=[clarification_handoff],
        model_settings=commit_settings,
        tool_use_behavior=StopAtTools(
            stop_at_tool_names=[
                "finalize_plan",
                "mark_plan_unsupported",
            ]
        ),
    )
    evidence_run_config = RunConfig(
        tracing_disabled=True,
        workflow_name="Planner Evidence Run",
        tool_execution=ToolExecutionConfig(
            max_function_tool_concurrency=(
                PLANNER_EVIDENCE_MAX_FUNCTION_TOOL_CONCURRENCY
            )
        ),
    )
    gap_handler_run_config = RunConfig(
        tracing_disabled=True,
        workflow_name="Planner Gap Handler Run",
        tool_execution=ToolExecutionConfig(
            max_function_tool_concurrency=1
        ),
    )
    commit_run_config = RunConfig(
        tracing_disabled=True,
        workflow_name="Planner Commit Run",
        tool_execution=ToolExecutionConfig(
            max_function_tool_concurrency=(
                PLANNER_COMMIT_MAX_FUNCTION_TOOL_CONCURRENCY
            )
        ),
    )
    clarification_run_config = RunConfig(
        tracing_disabled=True,
        workflow_name="Planner Clarification Run",
        tool_execution=ToolExecutionConfig(
            max_function_tool_concurrency=1
        ),
    )
    return PlannerAgentRunner(
        evidence_agent=evidence_agent,
        gap_handler_agent=gap_handler_agent,
        commit_agent=commit_agent,
        clarification_agent=clarification_agent,
        evidence_run_config=evidence_run_config,
        gap_handler_run_config=gap_handler_run_config,
        commit_run_config=commit_run_config,
        clarification_run_config=clarification_run_config,
    )
