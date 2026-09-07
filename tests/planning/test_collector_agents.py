"""Collector Agent 隔离取证、只读 Catalog 与确定性证据（EvidenceItem / CollectorResult）提取测试。

核心业务不变量（遵循 AGENTS.md 规范）：
1. 物理隔离与只读 Catalog 约束：
   - Evidence 阶段三路并行取证（DocumentCollector, ContextCollector, OperationsCollector），各自只能访问其限定的只读查询 Tool，严禁包含任何写命令。
   - Collector LLM 仅负责规划查询并输出 summary 与 gaps。
2. 确定性证据提取（Deterministic Evidence Extraction）：
   - Runtime 从 Collector 的 nested Run 中按 `call_id` 精确配对 ToolCallItem 与 ToolCallOutputItem。
   - 每个 Tool 调用生成一个结构化 `EvidenceItem`，并组合为包含稳定去重 `resource_refs` 与完整 payload 的 `CollectorResult`。
   - 若出现输出非法、call_id 重复或缺少对应 Tool Call，提取器必须 fail-closed 抛错。
"""

from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from typing import Any, Literal

from agents import ModelSettings
from agents.items import ToolCallItem, ToolCallOutputItem
from pydantic import BaseModel

from app.agent_runtime.errors import safe_tool_error_function
from app.agents import EvidenceItem as ExportedEvidenceItem
from app.agents.collectors import (
    CollectorLLMResult,
    CollectorRequest,
    CollectorResult,
    EvidenceItem,
    _build_collector_output_extractor,
    _extract_evidence_items,
    _normalize_tool_output,
    build_collector_agents,
    extract_collector_results,
)


class _SampleToolOutput(BaseModel):
    """覆盖 BaseModel ToolOutput 规范化分支的测试模型。"""

    outcome: Literal["succeeded", "rejected", "failed"]
    result_code: str
    message: str
    retryable: bool
    resource_refs: list[str]
    document: dict[str, Any] | None = None


class CollectorAgentsTest(unittest.IsolatedAsyncioTestCase):
    """验证 Collector Agents 的 Catalog 装配、Tool 输出规范化与按 call_id 证据提取。"""
    def setUp(self) -> None:
        self.collectors = build_collector_agents(
            model="test-model",
            model_settings=ModelSettings(),
        )

    def _tool_call(
        self,
        call_id: str | None,
        tool_name: str | None,
        arguments: Any = "{}",
    ) -> ToolCallItem:
        raw_item: dict[str, Any] = {
            "type": "function_call",
            "arguments": arguments,
        }
        if call_id is not None:
            raw_item["call_id"] = call_id
        if tool_name is not None:
            raw_item["name"] = tool_name
        return ToolCallItem(
            agent=self.collectors.document,
            raw_item=raw_item,
        )

    def _tool_output(
        self,
        call_id: str | None,
        output: Any,
    ) -> ToolCallOutputItem:
        raw_item: dict[str, Any] = {
            "type": "function_call_output",
            "output": "serialized-output",
        }
        if call_id is not None:
            raw_item["call_id"] = call_id
        return ToolCallOutputItem(
            agent=self.collectors.document,
            raw_item=raw_item,
            output=output,
        )

    def test_collectors_have_only_role_specific_read_tools(self) -> None:
        document_tools = {
            tool.name for tool in self.collectors.document.tools
        }
        context_tools = {tool.name for tool in self.collectors.context.tools}
        operations_tools = {
            tool.name for tool in self.collectors.operations.tools
        }

        self.assertIn("get_document", document_tools)
        self.assertNotIn("process_document", document_tools)
        self.assertEqual(
            context_tools,
            {
                "get_conversation_turn",
                "list_conversation_turns",
                "get_context_chain",
                "list_context_chains",
                "list_context_chain_nodes",
                "list_context_chain_resources",
                "list_context_selection_records",
            },
        )
        self.assertIn("query_document_log_events", operations_tools)
        self.assertIn("get_document_operation_timeline", operations_tools)
        self.assertIn("get_document_workflow_timeline", operations_tools)
        self.assertIn("query_agent_tool_audits", operations_tools)

    def test_collectors_expose_only_llm_interpretation_schema(self) -> None:
        for agent in (
            self.collectors.document,
            self.collectors.context,
            self.collectors.operations,
        ):
            self.assertIs(agent.output_type, CollectorLLMResult)
            self.assertEqual(agent.handoffs, [])
            self.assertFalse(agent.model_settings.parallel_tool_calls)

        self.assertIs(ExportedEvidenceItem, EvidenceItem)

    def test_planner_receives_three_agent_as_tools(self) -> None:
        tools = self.collectors.planner_tools

        self.assertEqual(
            {tool.name for tool in tools},
            {
                "collect_document_information",
                "collect_context_information",
                "collect_operation_information",
            },
        )
        self.assertTrue(all(tool._is_agent_tool for tool in tools))
        self.assertTrue(all(tool.needs_approval is False for tool in tools))
        self.assertTrue(
            all(
                tool.params_json_schema["properties"].get("question")
                for tool in tools
            )
        )

    def test_collector_request_keeps_planner_scope_structured(self) -> None:
        request = CollectorRequest(
            question="检查文档 7 的失败原因",
            conversation_id="conversation-1",
            document_ids=[7],
            workflow_ids=["workflow-1"],
        )

        self.assertEqual(request.document_ids, [7])
        self.assertEqual(request.workflow_ids, ["workflow-1"])

    def test_normalize_tool_output_accepts_base_model_and_json(self) -> None:
        model_output = _SampleToolOutput(
            outcome="succeeded",
            result_code="document_found",
            message="已找到文档",
            retryable=False,
            resource_refs=["document:7"],
            document={"id": 7, "status": "processed"},
        )
        json_output = json.dumps(
            {
                "outcome": "rejected",
                "result_code": "document_not_found",
                "message": "文档不存在",
                "retryable": False,
                "resource_refs": ["document:8"],
            }
        )

        self.assertEqual(
            _normalize_tool_output(model_output)["document"],
            {"id": 7, "status": "processed"},
        )
        self.assertEqual(
            _normalize_tool_output(json_output)["outcome"],
            "rejected",
        )

    def test_normalize_tool_output_rejects_non_object_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "必须是 JSON object"):
            _normalize_tool_output("[]")

        with self.assertRaisesRegex(TypeError, "Unsupported"):
            _normalize_tool_output(7)

    def test_multiple_tool_calls_are_paired_by_call_id(self) -> None:
        first_call = self._tool_call("call-1", "get_document")
        second_call = self._tool_call("call-2", "list_documents")
        first_output = self._tool_output(
            "call-1",
            _SampleToolOutput(
                outcome="succeeded",
                result_code="document_found",
                message="已找到文档",
                retryable=False,
                resource_refs=["document:7"],
                document={"id": 7, "status": "processed"},
            ),
        )
        second_output = self._tool_output(
            "call-2",
            {
                "outcome": "succeeded",
                "result_code": "documents_listed",
                "message": "查询成功",
                "retryable": False,
                "resource_refs": ["document:7", "document:8"],
                "documents": [{"id": 7}, {"id": 8}],
                "total": 2,
                "limit": 50,
                "offset": 0,
            },
        )

        evidence_items = _extract_evidence_items(
            [first_call, second_call, second_output, first_output]
        )

        self.assertEqual(
            [item.tool_call_id for item in evidence_items],
            ["call-2", "call-1"],
        )
        self.assertEqual(
            [item.tool_name for item in evidence_items],
            ["list_documents", "get_document"],
        )
        self.assertEqual(
            [item.original_tool_call_id for item in evidence_items],
            ["call-2", "call-1"],
        )
        self.assertTrue(all(item.attempt_count == 1 for item in evidence_items))
        self.assertEqual(
            evidence_items[0].payload,
            {
                "documents": [{"id": 7}, {"id": 8}],
                "total": 2,
                "limit": 50,
                "offset": 0,
            },
        )
        self.assertEqual(
            evidence_items[1].payload,
            {"document": {"id": 7, "status": "processed"}},
        )
        self.assertTrue(
            all(
                "outcome" not in evidence_item.payload
                and "resource_refs" not in evidence_item.payload
                for evidence_item in evidence_items
            )
        )

    def test_same_tool_calls_pair_arguments_and_outputs_by_call_id(
        self,
    ) -> None:
        first_call = self._tool_call(
            "call-1",
            "get_document",
            json.dumps({"document_id": 7}),
        )
        second_call = self._tool_call(
            "call-2",
            "get_document",
            json.dumps({"document_id": 8}),
        )
        first_output = self._tool_output(
            "call-1",
            {
                "outcome": "succeeded",
                "result_code": "document_found",
                "message": "已找到文档 7",
                "retryable": False,
                "resource_refs": ["document:7"],
                "document": {"id": 7},
            },
        )
        second_output = self._tool_output(
            "call-2",
            {
                "outcome": "succeeded",
                "result_code": "document_found",
                "message": "已找到文档 8",
                "retryable": False,
                "resource_refs": ["document:8"],
                "document": {"id": 8},
            },
        )

        evidence_items = _extract_evidence_items(
            [first_call, second_call, second_output, first_output]
        )
        evidence_by_call_id = {
            item.tool_call_id: item for item in evidence_items
        }

        self.assertEqual(
            evidence_by_call_id["call-1"].arguments,
            {"document_id": 7},
        )
        self.assertEqual(
            evidence_by_call_id["call-1"].payload["document"]["id"],
            7,
        )
        self.assertEqual(
            evidence_by_call_id["call-2"].arguments,
            {"document_id": 8},
        )
        self.assertEqual(
            evidence_by_call_id["call-2"].payload["document"]["id"],
            8,
        )

    def test_tool_call_arguments_must_be_json_object(self) -> None:
        valid_output = {
            "outcome": "succeeded",
            "result_code": "ok",
            "message": "ok",
            "retryable": False,
            "resource_refs": [],
        }
        invalid_arguments = {
            "missing": None,
            "non_string_object": {},
            "malformed_json": "{",
            "json_array": "[]",
            "json_string": '"document:7"',
            "json_number": "7",
        }

        for case_name, arguments in invalid_arguments.items():
            with self.subTest(case_name=case_name):
                with self.assertRaises(ValueError):
                    _extract_evidence_items(
                        [
                            self._tool_call(
                                "call-1",
                                "get_document",
                                arguments,
                            ),
                            self._tool_output("call-1", valid_output),
                        ]
                    )

        evidence_items = _extract_evidence_items(
            [
                self._tool_call("call-1", "get_document", "{}"),
                self._tool_output("call-1", valid_output),
            ]
        )
        self.assertEqual(evidence_items[0].arguments, {})

    def test_missing_or_duplicate_call_boundaries_fail_closed(self) -> None:
        valid_output = {
            "outcome": "succeeded",
            "result_code": "ok",
            "message": "ok",
            "retryable": False,
            "resource_refs": [],
        }
        cases = {
            "call_without_output": [
                self._tool_call("call-1", "get_document")
            ],
            "output_without_call": [
                self._tool_output("call-1", valid_output)
            ],
            "duplicate_call_id": [
                self._tool_call("call-1", "get_document"),
                self._tool_call("call-1", "list_documents"),
            ],
            "duplicate_output": [
                self._tool_call("call-1", "get_document"),
                self._tool_output("call-1", valid_output),
                self._tool_output("call-1", valid_output),
            ],
            "call_without_id": [
                self._tool_call(None, "get_document")
            ],
            "output_without_id": [
                self._tool_output(None, valid_output)
            ],
        }

        for case_name, new_items in cases.items():
            with self.subTest(case_name=case_name):
                with self.assertRaises(ValueError):
                    _extract_evidence_items(new_items)

    def test_top_level_collector_boundaries_fail_closed(self) -> None:
        valid_result = CollectorResult(
            collector_code="document_collector",
            summary="文档已查询",
        ).model_dump_json()
        invalid_cases = {
            "output_without_call": [
                self._tool_output("outer-1", valid_result)
            ],
            "unknown_evidence_tool": [
                self._tool_call("outer-1", "get_document"),
                self._tool_output("outer-1", valid_result),
            ],
            "collector_source_mismatch": [
                self._tool_call(
                    "outer-1",
                    "collect_context_information",
                ),
                self._tool_output("outer-1", valid_result),
            ],
        }

        for case_name, new_items in invalid_cases.items():
            with self.subTest(case_name=case_name):
                with self.assertRaises(ValueError):
                    extract_collector_results(new_items)

    def test_invalid_tool_output_envelope_fails_closed(self) -> None:
        invalid_outputs = (
            {
                "outcome": "succeeded",
                "result_code": "ok",
                "message": "ok",
                "retryable": False,
            },
            {
                "outcome": "unknown",
                "result_code": "ok",
                "message": "ok",
                "retryable": False,
                "resource_refs": [],
            },
        )

        for invalid_output in invalid_outputs:
            with self.subTest(invalid_output=invalid_output):
                with self.assertRaises(ValueError):
                    _extract_evidence_items(
                        [
                            self._tool_call("call-1", "get_document"),
                            self._tool_output("call-1", invalid_output),
                        ]
                    )

    def test_safe_tool_error_json_forms_rejected_evidence(self) -> None:
        output = safe_tool_error_function(None, ValueError("private detail"))

        evidence_items = _extract_evidence_items(
            [
                self._tool_call("call-1", "get_document"),
                self._tool_output("call-1", output),
            ]
        )

        self.assertEqual(len(evidence_items), 1)
        self.assertEqual(evidence_items[0].outcome, "rejected")
        self.assertEqual(
            evidence_items[0].result_code,
            "invalid_tool_arguments",
        )
        self.assertEqual(evidence_items[0].payload, {})

    async def test_extractor_combines_llm_and_runtime_evidence(self) -> None:
        extractor = _build_collector_output_extractor(
            "document_collector"
        )
        new_items = [
            self._tool_call("call-1", "get_document"),
            self._tool_output(
                "call-1",
                {
                    "outcome": "succeeded",
                    "result_code": "document_found",
                    "message": "已找到文档",
                    "retryable": False,
                    "resource_refs": ["document:7", "artifact:1"],
                    "document": {"id": 7, "status": "processed"},
                },
            ),
            self._tool_call("call-2", "get_document"),
            self._tool_output(
                "call-2",
                {
                    "outcome": "rejected",
                    "result_code": "document_not_found",
                    "message": "文档不存在",
                    "retryable": False,
                    "resource_refs": ["document:7", "document:8"],
                    "document": None,
                },
            ),
        ]

        output = await extractor(
            SimpleNamespace(
                final_output=CollectorLLMResult(
                    summary="文档状态已查询",
                    gaps=["文档 8 不存在"],
                ),
                new_items=new_items,
            )
        )
        result = CollectorResult.model_validate_json(output)
        output_data = json.loads(output)

        self.assertEqual(result.collector_code, "document_collector")
        self.assertEqual(
            result.resource_refs,
            ["document:7", "artifact:1", "document:8"],
        )
        self.assertEqual(len(result.evidence_items), 2)
        self.assertEqual(result.evidence_items[1].outcome, "rejected")
        self.assertEqual(result.gaps, ["文档 8 不存在"])
        self.assertNotIn("facts", output_data)

    async def test_extractor_allows_collector_without_tool_calls(self) -> None:
        extractor = _build_collector_output_extractor("context_collector")

        output = await extractor(
            SimpleNamespace(
                final_output=CollectorLLMResult(
                    summary="当前请求不需要额外查询",
                ),
                new_items=[],
            )
        )
        result = CollectorResult.model_validate_json(output)

        self.assertEqual(result.evidence_items, [])
        self.assertEqual(result.resource_refs, [])
        self.assertEqual(result.gaps, [])

    async def test_extractor_allows_collector_without_evidence_and_with_gap(
        self,
    ) -> None:
        extractor = _build_collector_output_extractor("context_collector")

        output = await extractor(
            SimpleNamespace(
                final_output=CollectorLLMResult(
                    summary="未取得必要上下文证据",
                    gaps=["无法确认 chain-1 当前状态"],
                ),
                new_items=[],
            )
        )
        result = CollectorResult.model_validate_json(output)

        self.assertEqual(result.evidence_items, [])
        self.assertEqual(result.gaps, ["无法确认 chain-1 当前状态"])

    async def test_extractor_allows_evidence_without_gap(self) -> None:
        extractor = _build_collector_output_extractor("document_collector")
        new_items = [
            self._tool_call(
                "call-1",
                "get_document",
                json.dumps({"document_id": 7}),
            ),
            self._tool_output(
                "call-1",
                {
                    "outcome": "succeeded",
                    "result_code": "document_found",
                    "message": "已找到文档",
                    "retryable": False,
                    "resource_refs": ["document:7"],
                    "document": {"id": 7},
                },
            ),
        ]

        output = await extractor(
            SimpleNamespace(
                final_output=CollectorLLMResult(summary="文档已确认"),
                new_items=new_items,
            )
        )
        result = CollectorResult.model_validate_json(output)

        self.assertEqual(len(result.evidence_items), 1)
        self.assertEqual(result.gaps, [])


if __name__ == "__main__":
    unittest.main()
