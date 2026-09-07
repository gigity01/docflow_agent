"""Planner 证据图编排中的 GapHandler 决策、定向补查与 LangGraph 图流转测试。

核心业务不变量（遵循 AGENTS.md 规范）：
1. 进程内 LangGraph 状态图与补查上限：
   - 编排流程：第一轮 Evidence -> 首次 Gap 判断 -> 最多 1 次定向补查 -> 最终 Gap 判断 -> Commit。
   - 若首次取证后仍有证据缺口（gaps）且未达到补查上限，触发 targeted_collect；
   - 补查后若依然存在缺口，则根据意图明确程度分派至澄清（clarify）、不支持（unsupported）或继续由 Commit Agent 尝试。
2. 架构防腐隔离：
   - GapHandler 和 Commit Agent 不直接执行任何副作用命令；
   - Collector 结果与 ToolRegistry/BusinessDocs 路由严格分离。
"""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from agents import ModelSettings, RunContextWrapper
from agents.items import ToolCallItem, ToolCallOutputItem
from pydantic import ValidationError

from app.agent_runtime import business_docs as business_docs_module
from app.agent_runtime.business_docs import (
    BusinessDocMatch,
    SearchBusinessDocsInput,
    load_service_map,
    search_business_docs_handler,
)
from app.agent_runtime.context import AgentToolContext, ContextToolServices
from app.agent_runtime.tool_registry import (
    FindEvidenceToolsInput,
    ListEvidenceToolsInput,
    find_evidence_tools_handler,
    list_evidence_tools_handler,
)
from app.agents.collectors import (
    CollectorResult,
    EvidenceItem,
    build_collector_agents,
)
from app.agents.gap_handler import (
    GAP_HANDLER_INSTRUCTIONS,
    GapAction,
    GapDecision,
    GapDecisionError,
)
from app.agents.planner import (
    ClarificationAgentOutput,
    _invoke_evidence_retry,
    build_planner_agent,
)
from app.modules.context.agent_tools.query_tools import (
    get_context_chain_handler,
)
from app.modules.context.agent_tools.schemas import GetContextChainToolInput
from app.modules.planning.application.dto import (
    PlannerContextInput,
    RunPlanningInput,
)
from app.modules.planning.application.errors import PlanningRetryRequested
from app.modules.planning.application.run_planning import RunPlanningUseCase


class _RunResult:
    def __init__(
        self,
        *,
        new_items=None,
        final_output=None,
        history=None,
    ) -> None:
        self.new_items = list(new_items or [])
        self.final_output = final_output
        self._history = list(history or [])

    def to_input_list(self):
        return self._history


class GapHandlerRunnerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        collectors = build_collector_agents(
            model="test-model",
            model_settings=ModelSettings(),
        )
        self.runner = build_planner_agent(
            model="test-model",
            model_settings=ModelSettings(),
            collectors=collectors,
        )
        self.planner_input = PlannerContextInput(
            current_user_input="索引文档 7",
            context_chains=[],
        )

    @staticmethod
    def _evidence_item(
        *,
        tool_name="get_document_chunk_statistics",
        tool_call_id="inner-call",
        arguments=None,
        outcome="succeeded",
        retryable=False,
        message="查询完成",
        payload=None,
    ) -> EvidenceItem:
        return EvidenceItem(
            tool_name=tool_name,
            tool_call_id=tool_call_id,
            original_tool_call_id=tool_call_id,
            attempt_count=1,
            arguments=arguments
            or {"tool_input": {"document_id": 7}},
            outcome=outcome,
            result_code="chunk_statistics_result",
            message=message,
            retryable=retryable,
            resource_refs=["document:7"],
            payload=payload or {},
        )

    @staticmethod
    def _retry_tool_output(
        *,
        outcome="succeeded",
        retryable=False,
        message="重试查询完成",
        payload=None,
    ) -> dict:
        return {
            "outcome": outcome,
            "result_code": "chunk_statistics_result",
            "message": message,
            "retryable": retryable,
            "resource_refs": ["document:7"],
            **(payload or {}),
        }

    def _collector_result(
        self,
        *,
        gaps=None,
        evidence_items=None,
        summary="文档证据已检查",
    ) -> CollectorResult:
        return CollectorResult(
            collector_code="document_collector",
            summary=summary,
            gaps=list(gaps or []),
            resource_refs=["document:7"],
            evidence_items=list(evidence_items or []),
        )

    def _evidence_run(
        self,
        collector_result: CollectorResult,
        *,
        history_label: str,
    ) -> _RunResult:
        call_id = f"outer-{history_label}"
        call = ToolCallItem(
            agent=self.runner.evidence_agent,
            raw_item={
                "type": "function_call",
                "call_id": call_id,
                "name": "collect_document_information",
                "arguments": "{}",
            },
        )
        output = ToolCallOutputItem(
            agent=self.runner.evidence_agent,
            raw_item={
                "type": "function_call_output",
                "call_id": call_id,
                "output": "serialized-output",
            },
            output=collector_result.model_dump_json(),
        )
        return _RunResult(
            new_items=[call, output],
            history=[
                {"role": "user", "content": history_label},
                {
                    "type": "function_call",
                    "call_id": call_id,
                    "name": "collect_document_information",
                    "arguments": "{}",
                },
                {
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": collector_result.model_dump_json(),
                },
            ],
        )

    @staticmethod
    def _gap_run(decision: GapDecision) -> _RunResult:
        return _RunResult(final_output=decision)

    @staticmethod
    def _planning_context():
        return SimpleNamespace(
            planning_services=SimpleNamespace(
                mark_plan_unsupported=SimpleNamespace(execute=mock.Mock()),
                mark_plan_needs_clarification=SimpleNamespace(
                    execute=mock.Mock()
                ),
            ),
            plan_id="plan-1",
            conversation_id="conversation-1",
        )

    async def test_no_gap_skips_gap_handler_and_enters_commit(self) -> None:
        evidence = self._evidence_run(
            self._collector_result(
                evidence_items=[
                    self._evidence_item(payload={"chunk_count": 3})
                ]
            ),
            history_label="evidence-1",
        )
        commit = _RunResult(final_output="committed")

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(side_effect=[evidence, commit]),
        ) as run:
            result = await self.runner.run(
                planner_input=self.planner_input,
                context=mock.Mock(),
            )

        self.assertIs(result, commit)
        self.assertEqual(run.await_count, 2)
        self.assertIs(run.await_args_list[1].args[0], self.runner.commit_agent)

    async def test_empty_evidence_is_checked_before_capability_only_commit(
        self,
    ) -> None:
        evidence = _RunResult(
            new_items=[],
            history=[{"role": "user", "content": "evidence-1"}],
        )
        capability_only = self._gap_run(
            GapDecision(
                action=GapAction.COMMIT,
                reason="该请求只需由 Commit 判断执行 Capability 是否存在",
            )
        )
        commit = _RunResult(final_output="unsupported committed")

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(
                side_effect=[evidence, capability_only, commit]
            ),
        ) as run:
            result = await self.runner.run(
                planner_input=PlannerContextInput(
                    current_user_input="给 Slack 发消息",
                    context_chains=[],
                ),
                context=mock.Mock(),
            )

        self.assertIs(result, commit)
        self.assertEqual(run.await_count, 3)
        self.assertIs(
            run.await_args_list[1].args[0],
            self.runner.gap_handler_agent,
        )

    async def test_collect_more_runs_one_targeted_second_round_then_commit(
        self,
    ) -> None:
        first = self._evidence_run(
            self._collector_result(gaps=["无法确认文档 7 的切块状态"]),
            history_label="evidence-1",
        )
        collect_more = self._gap_run(
            GapDecision(
                action=GapAction.COLLECT_MORE,
                reason="Registry 中存在切块统计查询，但第一轮没有调用",
                follow_up="只确认文档 7 当前切块构建状态",
            )
        )
        second = self._evidence_run(
            self._collector_result(
                evidence_items=[
                    self._evidence_item(payload={"chunk_count": 3})
                ]
            ),
            history_label="evidence-2",
        )
        resolved = self._gap_run(
            GapDecision(
                action=GapAction.COMMIT,
                reason="第二轮 Evidence 已确认切块状态",
            )
        )
        commit = _RunResult(final_output="committed")

        pending_results = [first, collect_more, second, resolved, commit]

        async def run_agent(_agent, agent_input, **_kwargs):
            result = pending_results.pop(0)
            if result is second:
                result._history = [*agent_input, *result._history]
            return result

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(side_effect=run_agent),
        ) as run:
            result = await self.runner.run(
                planner_input=self.planner_input,
                context=mock.Mock(),
            )

        self.assertIs(result, commit)
        self.assertEqual(run.await_count, 5)
        second_evidence_call = run.await_args_list[2]
        self.assertIs(second_evidence_call.args[0], self.runner.evidence_agent)
        self.assertIn(
            "只确认文档 7 当前切块构建状态",
            second_evidence_call.args[1][-1]["content"],
        )
        second_gap_input = json.loads(run.await_args_list[3].args[1])
        self.assertFalse(second_gap_input["collect_more_allowed"])
        self.assertEqual(len(second_gap_input["evidence_rounds"]), 2)

        second_evidence_input = second_evidence_call.args[1]
        commit_history = run.await_args_list[4].args[1]
        positions = {
            "round_1_call": next(
                index
                for index, item in enumerate(commit_history)
                if item.get("call_id") == "outer-evidence-1"
                and item.get("type") == "function_call"
            ),
            "round_1_output": next(
                index
                for index, item in enumerate(commit_history)
                if item.get("call_id") == "outer-evidence-1"
                and item.get("type") == "function_call_output"
            ),
            "follow_up": next(
                index
                for index, item in enumerate(commit_history)
                if item.get("role") == "user"
                and "Evidence Follow-up" in item.get("content", "")
            ),
            "round_2_marker": next(
                index
                for index, item in enumerate(commit_history)
                if item == {"role": "user", "content": "evidence-2"}
            ),
            "round_2_call": next(
                index
                for index, item in enumerate(commit_history)
                if item.get("call_id") == "outer-evidence-2"
                and item.get("type") == "function_call"
            ),
            "round_2_output": next(
                index
                for index, item in enumerate(commit_history)
                if item.get("call_id") == "outer-evidence-2"
                and item.get("type") == "function_call_output"
            ),
        }
        self.assertEqual(
            list(positions.values()),
            sorted(positions.values()),
        )
        self.assertEqual(
            commit_history[: positions["round_2_marker"]],
            second_evidence_input,
        )
        self.assertEqual(
            json.loads(commit_history[positions["round_1_output"]]["output"])[
                "collector_code"
            ],
            "document_collector",
        )
        self.assertEqual(
            json.loads(commit_history[positions["round_2_output"]]["output"])[
                "collector_code"
            ],
            "document_collector",
        )
        self.assertIn("Gap Decision", commit_history[-1]["content"])
        self.assertIn('"action": "COMMIT"', commit_history[-1]["content"])

    async def test_initial_evidence_local_retry_succeeds_then_commits(
        self,
    ) -> None:
        evidence = self._evidence_run(
            self._collector_result(
                gaps=["切块状态仍未知"],
                evidence_items=[
                    self._evidence_item(
                        outcome="failed",
                        retryable=True,
                    )
                ],
            ),
            history_label="evidence-1",
        )
        retry = self._gap_run(
            GapDecision(
                action=GapAction.RETRY,
                reason="正确查询发生可重试故障",
            )
        )
        resolved = self._gap_run(
            GapDecision(
                action=GapAction.COMMIT,
                reason="局部重试已取得切块状态",
            )
        )
        commit = _RunResult(final_output="committed")
        retry_tool = SimpleNamespace(
            name="get_document_chunk_statistics",
            on_invoke_tool=mock.AsyncMock(
                return_value=self._retry_tool_output(
                    payload={"statistics": {"chunk_count": 3}}
                )
            ),
        )

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(
                side_effect=[evidence, retry, resolved, commit]
            ),
        ) as run, mock.patch(
            "app.agents.planner._resolve_evidence_tool",
            return_value=retry_tool,
        ):
            result = await self.runner.run(
                planner_input=self.planner_input,
                context=mock.Mock(),
            )

        self.assertIs(result, commit)
        retry_tool.on_invoke_tool.assert_awaited_once()
        retried_gap_input = json.loads(run.await_args_list[2].args[1])
        retried_item = retried_gap_input["evidence_rounds"][0][
            "collector_results"
        ][0]["evidence_items"][0]
        self.assertEqual(retried_item["outcome"], "succeeded")
        self.assertEqual(retried_item["attempt_count"], 2)
        self.assertEqual(retried_item["original_tool_call_id"], "inner-call")
        commit_history = run.await_args_list[3].args[1]
        retry_history = next(
            item
            for item in commit_history
            if item.get("role") == "user"
            and "Evidence Local Retry" in item.get("content", "")
        )
        self.assertIn('"current_retry_count": 1', retry_history["content"])

    async def test_local_retry_replays_original_arguments_through_catalog_tool(
        self,
    ) -> None:
        get_statistics = mock.Mock()
        get_statistics.execute.return_value = {
            "document_id": 7,
            "doc_code": "DOC-7",
            "parent_count": 1,
            "child_count": 3,
            "parent_status_counts": {"active": 1},
            "child_status_counts": {"active": 3},
            "vector_status_counts": {"pending": 3},
            "chunk_type_counts": {"text": 3},
            "chunks_with_vector_id": 0,
            "chunks_without_vector_id": 3,
        }
        audit_logger = mock.Mock()
        context = SimpleNamespace(
            permissions=frozenset({"document:read"}),
            document_services=SimpleNamespace(
                get_document_chunk_statistics=get_statistics
            ),
            audit_logger=audit_logger,
        )
        failed_item = self._evidence_item(
            outcome="failed",
            retryable=True,
            message="统计查询超时",
        )

        retried_item = await _invoke_evidence_retry(
            failed_item=failed_item,
            retry_count=1,
            context=context,
        )

        get_statistics.execute.assert_called_once_with(7)
        self.assertEqual(retried_item.outcome, "succeeded")
        self.assertEqual(retried_item.arguments, failed_item.arguments)
        self.assertEqual(retried_item.original_tool_call_id, "inner-call")
        self.assertEqual(retried_item.attempt_count, 2)
        self.assertEqual(retried_item.payload["statistics"]["child_count"], 3)
        audit_logger.start.assert_called_once()

    async def test_follow_up_evidence_local_retry_returns_to_final_gap(
        self,
    ) -> None:
        first = self._evidence_run(
            self._collector_result(gaps=["第一轮没有查询切块状态"]),
            history_label="evidence-1",
        )
        collect_more = self._gap_run(
            GapDecision(
                action=GapAction.COLLECT_MORE,
                reason="需要定向补查切块状态",
                follow_up="只确认文档 7 当前切块状态",
            )
        )
        second = self._evidence_run(
            self._collector_result(
                gaps=["切块状态仍未知"],
                evidence_items=[
                    self._evidence_item(
                        tool_call_id="second-round-call",
                        outcome="failed",
                        retryable=True,
                    )
                ],
            ),
            history_label="evidence-2",
        )
        retry = self._gap_run(
            GapDecision(
                action=GapAction.RETRY,
                reason="第二轮查询发生可重试故障",
            )
        )
        resolved = self._gap_run(
            GapDecision(
                action=GapAction.COMMIT,
                reason="第二轮局部重试已解决缺口",
            )
        )
        commit = _RunResult(final_output="committed")
        retry_tool = SimpleNamespace(
            name="get_document_chunk_statistics",
            on_invoke_tool=mock.AsyncMock(
                return_value=self._retry_tool_output(
                    payload={"statistics": {"chunk_count": 3}}
                )
            ),
        )
        pending_results = [
            first,
            collect_more,
            second,
            retry,
            resolved,
            commit,
        ]

        async def run_agent(_agent, agent_input, **_kwargs):
            result = pending_results.pop(0)
            if result is second:
                result._history = [*agent_input, *result._history]
            return result

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(side_effect=run_agent),
        ) as run, mock.patch(
            "app.agents.planner._resolve_evidence_tool",
            return_value=retry_tool,
        ):
            result = await self.runner.run(
                planner_input=self.planner_input,
                context=mock.Mock(),
            )

        self.assertIs(result, commit)
        retry_tool.on_invoke_tool.assert_awaited_once()
        self.assertFalse(json.loads(run.await_args_list[3].args[1])[
            "collect_more_allowed"
        ])
        retried_gap_input = json.loads(run.await_args_list[4].args[1])
        self.assertFalse(retried_gap_input["collect_more_allowed"])
        retried_item = retried_gap_input["evidence_rounds"][1][
            "collector_results"
        ][0]["evidence_items"][0]
        self.assertEqual(retried_item["outcome"], "succeeded")
        self.assertEqual(retried_item["attempt_count"], 2)

    async def test_local_retry_only_reexecutes_failed_evidence(self) -> None:
        succeeded_item = self._evidence_item(
            tool_name="get_document",
            tool_call_id="successful-call",
            arguments={"tool_input": {"document_id": 7}},
            payload={"document": {"id": 7}},
        )
        failed_item = self._evidence_item(
            tool_call_id="failed-call",
            outcome="failed",
            retryable=True,
            message="统计查询超时",
        )
        evidence = self._evidence_run(
            self._collector_result(
                gaps=["切块状态仍未知"],
                evidence_items=[succeeded_item, failed_item],
            ),
            history_label="evidence-1",
        )
        retry = self._gap_run(
            GapDecision(
                action=GapAction.RETRY,
                reason="统计查询可重试",
            )
        )
        resolved = self._gap_run(
            GapDecision(
                action=GapAction.COMMIT,
                reason="失败查询重试成功",
            )
        )
        commit = _RunResult(final_output="committed")
        retry_tool = SimpleNamespace(
            name="get_document_chunk_statistics",
            on_invoke_tool=mock.AsyncMock(
                return_value=self._retry_tool_output(
                    payload={"statistics": {"chunk_count": 3}}
                )
            ),
        )

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(
                side_effect=[evidence, retry, resolved, commit]
            ),
        ) as run, mock.patch(
            "app.agents.planner._resolve_evidence_tool",
            return_value=retry_tool,
        ) as resolve_tool:
            await self.runner.run(
                planner_input=self.planner_input,
                context=mock.Mock(),
            )

        resolve_tool.assert_called_once_with("get_document_chunk_statistics")
        retry_tool.on_invoke_tool.assert_awaited_once()
        retried_items = json.loads(run.await_args_list[2].args[1])[
            "evidence_rounds"
        ][0]["collector_results"][0]["evidence_items"]
        by_name = {item["tool_name"]: item for item in retried_items}
        self.assertEqual(
            by_name["get_document"]["tool_call_id"],
            "successful-call",
        )
        self.assertEqual(
            by_name["get_document_chunk_statistics"]["attempt_count"],
            2,
        )

    async def test_two_local_retries_exhaust_then_request_outer_replan(
        self,
    ) -> None:
        evidence = self._evidence_run(
            self._collector_result(
                gaps=["切块状态仍未知"],
                evidence_items=[
                    self._evidence_item(
                        outcome="failed",
                        retryable=True,
                    )
                ],
            ),
            history_label="evidence-1",
        )
        retry_decisions = [
            self._gap_run(
                GapDecision(
                    action=GapAction.RETRY,
                    reason=f"第 {index} 次判断仍需重试",
                )
            )
            for index in range(1, 4)
        ]
        retry_tool = SimpleNamespace(
            name="get_document_chunk_statistics",
            on_invoke_tool=mock.AsyncMock(
                side_effect=[
                    self._retry_tool_output(
                        outcome="failed",
                        retryable=True,
                        message="第一次局部重试仍超时",
                    ),
                    self._retry_tool_output(
                        outcome="failed",
                        retryable=True,
                        message="第二次局部重试仍超时",
                    ),
                ]
            ),
        )

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(side_effect=[evidence, *retry_decisions]),
        ) as run, mock.patch(
            "app.agents.planner._resolve_evidence_tool",
            return_value=retry_tool,
        ):
            with self.assertRaisesRegex(
                PlanningRetryRequested,
                "第 3 次判断仍需重试",
            ):
                await self.runner.run(
                    planner_input=self.planner_input,
                    context=mock.Mock(),
                )

        self.assertEqual(retry_tool.on_invoke_tool.await_count, 2)
        self.assertEqual(run.await_count, 4)
        exhausted_gap_input = json.loads(run.await_args_list[3].args[1])
        exhausted_item = exhausted_gap_input["evidence_rounds"][0][
            "collector_results"
        ][0]["evidence_items"][0]
        self.assertEqual(exhausted_item["attempt_count"], 3)
        self.assertTrue(exhausted_item["retryable"])

    async def test_non_retryable_failure_cannot_be_labeled_retry(self) -> None:
        evidence = self._evidence_run(
            self._collector_result(
                gaps=["切块状态仍未知"],
                evidence_items=[
                    self._evidence_item(
                        outcome="failed",
                        retryable=False,
                    )
                ],
            ),
            history_label="evidence-1",
        )
        invalid_retry = self._gap_run(
            GapDecision(
                action=GapAction.RETRY,
                reason="不可重试失败不应进入 RETRY",
            )
        )

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(side_effect=[evidence, invalid_retry]),
        ):
            with self.assertRaisesRegex(
                GapDecisionError,
                "retryable=true",
            ):
                await self.runner.run(
                    planner_input=self.planner_input,
                    context=mock.Mock(),
                )

    async def test_non_retryable_registered_tool_failure_is_system_failure(
        self,
    ) -> None:
        registered = find_evidence_tools_handler(
            FindEvidenceToolsInput(query="get_document_chunk_statistics")
        )
        self.assertIn(
            "get_document_chunk_statistics",
            {tool.descriptor.name for tool in registered.tools},
        )
        evidence = self._evidence_run(
            self._collector_result(
                gaps=["切块状态仍未知"],
                evidence_items=[
                    self._evidence_item(
                        outcome="failed",
                        retryable=False,
                    )
                ],
            ),
            history_label="evidence-1",
        )
        system_failure = self._gap_run(
            GapDecision(
                action=GapAction.SYSTEM_FAILURE,
                reason="已注册查询发生不可自动恢复的系统错误",
            )
        )

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(side_effect=[evidence, system_failure]),
        ), mock.patch(
            "app.agents.planner._resolve_evidence_tool"
        ) as resolve_tool:
            with self.assertRaisesRegex(
                RuntimeError,
                "Planning 前置取证发生系统故障",
            ):
                await self.runner.run(
                    planner_input=self.planner_input,
                    context=mock.Mock(),
                )
        resolve_tool.assert_not_called()

    async def test_system_failure_requires_non_retryable_failed_evidence(
        self,
    ) -> None:
        evidence = self._evidence_run(
            self._collector_result(
                gaps=["切块状态仍未知"],
                evidence_items=[
                    self._evidence_item(
                        outcome="failed",
                        retryable=True,
                    )
                ],
            ),
            history_label="evidence-1",
        )
        invalid_system_failure = self._gap_run(
            GapDecision(
                action=GapAction.SYSTEM_FAILURE,
                reason="可重试失败不能归类为系统故障",
            )
        )

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(
                side_effect=[evidence, invalid_system_failure]
            ),
        ):
            with self.assertRaisesRegex(
                GapDecisionError,
                "retryable=false",
            ):
                await self.runner.run(
                    planner_input=self.planner_input,
                    context=mock.Mock(),
                )

    async def test_ambiguous_user_reference_uses_existing_clarification_flow(
        self,
    ) -> None:
        evidence = self._evidence_run(
            self._collector_result(
                gaps=["无法唯一确定用户指的是文档 7 还是文档 8"]
            ),
            history_label="evidence-1",
        )
        clarification = self._gap_run(
            GapDecision(
                action=GapAction.CLARIFICATION,
                reason="selected context 中存在两个合理目标",
                clarification_kind="resource",
                required_information=["需要重新处理的 document_id"],
                known_resource_refs=["document:7", "document:8"],
            )
        )
        question = _RunResult(
            final_output=ClarificationAgentOutput(
                question="需要重新处理文档 7 还是文档 8？"
            )
        )
        context = self._planning_context()

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(
                side_effect=[evidence, clarification, question]
            ),
        ) as run:
            result = await self.runner.run(
                planner_input=self.planner_input,
                context=context,
            )

        self.assertIs(result, question)
        marked = context.planning_services.mark_plan_needs_clarification.execute
        marked.assert_called_once()
        self.assertEqual(marked.call_args.args[0].plan_id, "plan-1")
        self.assertIs(
            run.await_args_list[2].args[0],
            self.runner.clarification_agent,
        )

    async def test_unavailable_required_fact_marks_plan_unsupported(
        self,
    ) -> None:
        self.assertEqual(
            find_evidence_tools_handler(
                FindEvidenceToolsInput(query="get_external_approval_status")
            ).tools,
            [],
        )
        evidence = self._evidence_run(
            self._collector_result(gaps=["无法取得外部审批状态"]),
            history_label="evidence-1",
        )
        unsupported = self._gap_run(
            GapDecision(
                action=GapAction.UNSUPPORTED,
                reason="Registry 没有外部审批状态查询能力",
            )
        )
        context = self._planning_context()

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(side_effect=[evidence, unsupported]),
        ) as run:
            result = await self.runner.run(
                planner_input=self.planner_input,
                context=context,
            )

        self.assertIs(result, unsupported)
        self.assertEqual(run.await_count, 2)
        marked = context.planning_services.mark_plan_unsupported.execute
        marked.assert_called_once()
        self.assertIn("外部审批", marked.call_args.args[0].reason)

    async def test_non_blocking_gap_can_enter_commit(self) -> None:
        evidence = self._evidence_run(
            self._collector_result(gaps=["无法确认首次索引时间"]),
            history_label="evidence-1",
        )
        non_blocking = self._gap_run(
            GapDecision(
                action=GapAction.COMMIT,
                reason="首次索引时间与重新处理文档无关",
            )
        )
        commit = _RunResult(final_output="committed")

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(
                side_effect=[evidence, non_blocking, commit]
            ),
        ) as run:
            result = await self.runner.run(
                planner_input=self.planner_input,
                context=mock.Mock(),
            )

        self.assertIs(result, commit)
        commit_history = run.await_args_list[2].args[1]
        self.assertIn("Gap Decision", commit_history[-1]["content"])

    async def test_second_round_cannot_request_third_collection(self) -> None:
        first = self._evidence_run(
            self._collector_result(gaps=["切块状态未知"]),
            history_label="evidence-1",
        )
        collect_more = self._gap_run(
            GapDecision(
                action=GapAction.COLLECT_MORE,
                reason="第一轮漏查",
                follow_up="确认切块状态",
            )
        )
        second = self._evidence_run(
            self._collector_result(gaps=["切块状态仍未知"]),
            history_label="evidence-2",
        )
        collect_again = self._gap_run(
            GapDecision(
                action=GapAction.COLLECT_MORE,
                reason="仍想补查",
                follow_up="再次确认切块状态",
            )
        )

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(
                side_effect=[first, collect_more, second, collect_again]
            ),
        ) as run:
            with self.assertRaisesRegex(
                GapDecisionError,
                "禁止再次 COLLECT_MORE",
            ):
                await self.runner.run(
                    planner_input=self.planner_input,
                    context=mock.Mock(),
                )

        self.assertEqual(run.await_count, 4)

    async def test_successful_empty_business_result_is_evidence_not_gap(
        self,
    ) -> None:
        evidence = self._evidence_run(
            self._collector_result(
                evidence_items=[
                    self._evidence_item(payload={"document": None})
                ],
                gaps=[],
            ),
            history_label="evidence-1",
        )
        commit = _RunResult(final_output="committed")

        with mock.patch(
            "app.agents.planner.Runner.run",
            new=mock.AsyncMock(side_effect=[evidence, commit]),
        ) as run:
            await self.runner.run(
                planner_input=self.planner_input,
                context=mock.Mock(),
            )

        self.assertEqual(run.await_count, 2)
        self.assertIs(run.await_args_list[1].args[0], self.runner.commit_agent)


class GapKnowledgeToolsTest(unittest.TestCase):
    def test_service_map_markdown_is_the_only_runtime_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            docs_root = Path(temp_dir)
            service_map_path = docs_root / "service_map.md"
            service_map_path.write_text(
                "# Service Map\n\nfirst version",
                encoding="utf-8",
            )
            with mock.patch.object(
                business_docs_module,
                "BUSINESS_DOCS_ROOT",
                docs_root,
            ):
                self.assertEqual(
                    load_service_map(),
                    "# Service Map\n\nfirst version",
                )
                service_map_path.write_text(
                    "# Service Map\n\nsecond version",
                    encoding="utf-8",
                )
                self.assertEqual(
                    load_service_map(),
                    "# Service Map\n\nsecond version",
                )

        self.assertFalse(hasattr(business_docs_module, "SERVICE_MAP_PROMPT"))

    def test_business_docs_and_registry_resolve_chunk_query_path(self) -> None:
        docs = search_business_docs_handler(
            SearchBusinessDocsInput(query="如何确认 Document 的 chunk 状态")
        )
        registry = find_evidence_tools_handler(
            FindEvidenceToolsInput(query="文档切块状态统计")
        )

        self.assertTrue(docs.matches)
        self.assertIn(
            "get_document_chunk_statistics",
            {
                tool.descriptor.name
                for tool in registry.tools
            },
        )

    def test_registry_is_dynamic_read_only_authority(self) -> None:
        registry = list_evidence_tools_handler(ListEvidenceToolsInput())

        self.assertTrue(registry.tools)
        self.assertTrue(
            all(
                tool.descriptor.operation_type == "query"
                and not tool.descriptor.side_effect
                for tool in registry.tools
            )
        )
        self.assertEqual(
            find_evidence_tools_handler(
                FindEvidenceToolsInput(
                    query="get_external_approval_status"
                )
            ).tools,
            [],
        )
        self.assertIn("Tool Registry 优先", GAP_HANDLER_INSTRUCTIONS)
        self.assertIn("不得扩大 Read Set", GAP_HANDLER_INSTRUCTIONS)
        self.assertIn("SYSTEM_FAILURE", GAP_HANDLER_INSTRUCTIONS)
        self.assertIn(
            "Schema Error 只表示模型输出不符合 GapDecision 契约",
            GAP_HANDLER_INSTRUCTIONS,
        )
        self.assertNotIn("应让本次结构化判断失败", GAP_HANDLER_INSTRUCTIONS)

    def test_business_docs_do_not_return_weakly_related_sections(self) -> None:
        result = search_business_docs_handler(
            SearchBusinessDocsInput(query="外星天气发票审批")
        )

        self.assertEqual(result.matches, [])

    def test_registry_wins_when_documented_query_tool_is_missing(self) -> None:
        documented_tool = BusinessDocMatch(
            document="test.md",
            heading="测试查询能力",
            content="可调用 foo_query 查询必要事实。",
        )
        with mock.patch.object(
            business_docs_module,
            "_load_business_doc_sections",
            return_value=(documented_tool,),
        ):
            docs = search_business_docs_handler(
                SearchBusinessDocsInput(query="foo_query")
            )
        self.assertEqual(docs.matches, [documented_tool])

        with mock.patch(
            "app.agent_runtime.tool_registry.list_evidence_tool_views",
            return_value=(),
        ):
            registry = find_evidence_tools_handler(
                FindEvidenceToolsInput(query="foo_query")
            )

        self.assertEqual(registry.tools, [])
        self.assertIn("Tool Registry 优先", GAP_HANDLER_INSTRUCTIONS)
        self.assertIn(
            "系统有已注册查询能力",
            GAP_HANDLER_INSTRUCTIONS,
        )
        self.assertIn(
            "Registry 中没有任何可用查询能力",
            GAP_HANDLER_INSTRUCTIONS,
        )

    def test_registry_capability_prevents_docs_only_unsupported(self) -> None:
        with mock.patch.object(
            business_docs_module,
            "_load_business_doc_sections",
            return_value=(),
        ):
            docs = search_business_docs_handler(
                SearchBusinessDocsInput(query="get_context_chain")
            )
        registry = find_evidence_tools_handler(
            FindEvidenceToolsInput(query="get_context_chain")
        )

        self.assertEqual(docs.matches, [])
        self.assertIn(
            "get_context_chain",
            {tool.descriptor.name for tool in registry.tools},
        )
        self.assertIn(
            "Business Docs 未写清但 Registry 存在对应查询能力",
            GAP_HANDLER_INSTRUCTIONS,
        )

    def test_registered_context_tool_cannot_expand_selected_context(self) -> None:
        registry = find_evidence_tools_handler(
            FindEvidenceToolsInput(query="get_context_chain")
        )
        self.assertIn(
            "get_context_chain",
            {tool.descriptor.name for tool in registry.tools},
        )
        query_service = mock.Mock()
        context = AgentToolContext(
            trace_id="trace-1",
            agent_run_id="run-1",
            agent_name="planner",
            conversation_id="conversation-1",
            turn_id="turn-current",
            task_id=None,
            actor_code="planner_agent",
            permissions=frozenset({"context:read"}),
            document_services=mock.Mock(),
            context_services=ContextToolServices(
                query_service=query_service
            ),
            allowed_context_chain_ids=frozenset({"chain_A"}),
            allowed_context_turn_ids=frozenset({"turn-current"}),
            audit_logger=mock.Mock(),
        )

        output = get_context_chain_handler(
            RunContextWrapper(context),
            GetContextChainToolInput(chain_id="chain_B"),
        )

        self.assertEqual(output.outcome, "rejected")
        self.assertEqual(output.result_code, "task_scope_violation")
        query_service.get_context_chain.assert_not_called()

    def test_gap_decision_requires_action_specific_fields(self) -> None:
        with self.assertRaises(ValidationError):
            GapDecision(
                action=GapAction.COLLECT_MORE,
                reason="需要补查",
            )
        with self.assertRaises(ValidationError):
            GapDecision(
                action=GapAction.CLARIFICATION,
                reason="用户指代不明",
            )
        with self.assertRaises(ValidationError):
            GapDecision(
                action=GapAction.COMMIT,
                reason="缺口不阻塞",
                known_resource_refs=["document:7"],
            )


class PlanningRetryRoutingTest(unittest.IsolatedAsyncioTestCase):
    async def test_retry_decision_is_persisted_by_planning_application(self) -> None:
        runner = SimpleNamespace(
            run=mock.AsyncMock(
                side_effect=PlanningRetryRequested("Evidence 查询临时失败")
            )
        )
        use_case = RunPlanningUseCase(
            ports=mock.Mock(),
            planning_use_cases=mock.Mock(),
            planner_runner=runner,
            document_services=mock.Mock(),
            context_services=mock.Mock(),
            context_resource_service=mock.Mock(),
            context_chain_mapper=mock.Mock(),
            operations_services=mock.Mock(),
        )
        use_case._build_agent_context = mock.Mock(return_value=mock.Mock())
        expected = mock.sentinel.planning_result
        use_case._finish_from_database = mock.Mock(return_value=expected)

        result = await use_case._run_existing_plan(
            RunPlanningInput(
                conversation_id="conversation-1",
                turn_id="turn-1",
            ),
            "plan-1",
            PlannerContextInput(
                current_user_input="索引文档 7",
                context_chains=[],
            ),
        )

        self.assertIs(result, expected)
        use_case._finish_from_database.assert_called_once_with(
            "plan-1",
            "turn-1",
            "Evidence 查询临时失败",
        )

    async def test_system_failure_uses_planning_application_recovery(self) -> None:
        runner = SimpleNamespace(
            run=mock.AsyncMock(
                side_effect=RuntimeError(
                    "Planning 前置取证发生系统故障: 数据库查询失败"
                )
            )
        )
        use_case = RunPlanningUseCase(
            ports=mock.Mock(),
            planning_use_cases=mock.Mock(),
            planner_runner=runner,
            document_services=mock.Mock(),
            context_services=mock.Mock(),
            context_resource_service=mock.Mock(),
            context_chain_mapper=mock.Mock(),
            operations_services=mock.Mock(),
        )
        use_case._build_agent_context = mock.Mock(return_value=mock.Mock())
        expected = mock.sentinel.planning_result
        use_case._finish_from_database = mock.Mock(return_value=expected)

        result = await use_case._run_existing_plan(
            RunPlanningInput(
                conversation_id="conversation-1",
                turn_id="turn-1",
            ),
            "plan-1",
            PlannerContextInput(
                current_user_input="索引文档 7",
                context_chains=[],
            ),
        )

        self.assertIs(result, expected)
        use_case._finish_from_database.assert_called_once_with(
            "plan-1",
            "turn-1",
            "Planner Runner 或 Tool 执行发生系统异常",
        )


if __name__ == "__main__":
    unittest.main()
