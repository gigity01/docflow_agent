"""Conversation 用户消息 HTTP API 接入与错误映射测试。

核心业务不变量（遵循 AGENTS.md 规范）：
1. 异步任务与 HTTP 状态码规范：
   - 当消息成功触发 Context 路由与 Planner，并发布异步 Plan 时，返回 HTTP 202 Accepted（状态为 processing 或 retry_pending）。
   - 响应包含完整的 `ContextSelectionMetadata`、关联 Task ID 列表与 Plan ID。
2. 参数校验与安全边界：
   - message 字段为必填且非空，超过长度限制或 conversation_id 长度超标直接返回 HTTP 422。
   - 依赖缺失（如 LLM Router 服务未配置）返回 HTTP 503 Service Unavailable。
   - 上游路由失败（ContextRoutingError）安全映射为 HTTP 502 Bad Gateway。
3. 澄清回答交互：
   - 携带 `clarification_id` 时复用源轮次，澄清相关业务错误分别映射为 HTTP 400（空回答）、404（不存在）、409（状态冲突）。
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest import mock

import httpx
from fastapi import FastAPI

from app.bootstrap.dependencies import get_container
from app.modules.clarification.application.errors import (
    ClarificationApplicationError,
)
from app.modules.conversation.presentation.router import router
from app.modules.conversation.presentation.dependencies import (
    get_send_conversation_message,
)
from app.modules.conversation.application.dto import (
    ContextSelectionMetadata,
    SendConversationMessageCommand,
    SendConversationMessageResult,
)
from app.modules.context.application.errors import ContextRoutingError
from app.modules.context.domain.enums import ContextSelectionMode


class ConversationMessagesApiTest(unittest.IsolatedAsyncioTestCase):
    """验证 POST /api/conversations/{conversation_id}/messages 的请求适配、响应契约与异常映射。"""

    def setUp(self) -> None:
        """初始化测试用 FastAPI 应用并覆盖应用层 UseCase 依赖。"""
        self.app = FastAPI()
        self.app.include_router(router, prefix="/api")
        self.use_case = mock.Mock()
        self.use_case.execute = mock.AsyncMock()

        async def get_service():
            return self.use_case

        self.app.dependency_overrides[get_send_conversation_message] = (
            get_service
        )

    async def _post(self, path: str, *, json: dict[str, str]):
        """使用 httpx ASGI 传输层发送异步 HTTP POST 请求。"""
        transport = httpx.ASGITransport(app=self.app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.post(path, json=json)

    async def test_message_is_adapted_to_application_command(self) -> None:
        """验证用户消息正确转化为 SendConversationMessageCommand 并返回 HTTP 202 Accepted 与结构化响应。"""
        result = SendConversationMessageResult(
            conversation_id="conv_test_001",
            turn_id="turn-1",
            plan_id="plan-1",
            status="processing",
            task_ids=["task-1"],
            context_selection=ContextSelectionMetadata(
                selection_mode=ContextSelectionMode.NO_CONTEXT,
                relevant_chain_ids=[],
                reason_summary="当前 Conversation 没有历史上下文。",
            ),
        )

        self.use_case.execute.return_value = result

        response = await self._post(
            "/api/conversations/conv_test_001/messages",
            json={
                "message": "继续完善之前的文档处理日志方案",
            },
        )

        self.assertEqual(response.status_code, 202)
        self.assertEqual(
            response.json(),
            {
                "conversation_id": "conv_test_001",
                "turn_id": "turn-1",
                "plan_id": "plan-1",
                "status": "processing",
                "assistant_message": None,
                "task_ids": ["task-1"],
                "clarification_id": None,
                "clarification_round": None,
                "context_selection": {
                    "selection_mode": "no_context",
                    "relevant_chain_ids": [],
                    "reason_summary": (
                        "当前 Conversation 没有历史上下文。"
                    ),
                },
            },
        )
        self.assertNotIn("selected_chains", response.json())
        self.assertNotIn("resource_queue", response.text)
        self.use_case.execute.assert_awaited_once_with(
            SendConversationMessageCommand(
                conversation_id="conv_test_001",
                message="继续完善之前的文档处理日志方案",
                clarification_id=None,
            )
        )

    async def test_message_is_required_and_must_not_be_empty(self) -> None:
        """验证缺少 message 字段或 message 为空字符串时返回 HTTP 422 验证错误。"""
        missing_response = await self._post(
            "/api/conversations/conv_test_001/messages",
            json={},
        )
        empty_response = await self._post(
            "/api/conversations/conv_test_001/messages",
            json={"message": ""},
        )

        self.assertEqual(missing_response.status_code, 422)
        self.assertEqual(empty_response.status_code, 422)

    async def test_routing_error_is_mapped_to_bad_gateway(self) -> None:
        """验证上游 ContextRoutingError 异常被正确映射为 HTTP 502 Bad Gateway。"""
        self.use_case.execute.side_effect = ContextRoutingError(
            "Context Agent 路由失败"
        )

        response = await self._post(
            "/api/conversations/conv_test_001/messages",
            json={"message": "继续之前的方案"},
        )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json(),
            {"detail": "Context Agent 路由失败"},
        )

    async def test_unconfigured_agent_is_service_unavailable(self) -> None:
        """验证当服务容器未配置 Conversation Agent 时返回 HTTP 503 Service Unavailable。"""
        async def get_unconfigured_container():
            return SimpleNamespace(
                context_agent_router=None,
                send_conversation_message=None,
            )

        self.app.dependency_overrides.pop(
            get_send_conversation_message,
            None,
        )
        self.app.dependency_overrides[get_container] = (
            get_unconfigured_container
        )

        response = await self._post(
            "/api/conversations/conv_test_001/messages",
            json={"message": "继续之前的方案"},
        )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json(),
            {"detail": "Conversation Agent 服务未配置"},
        )

    async def test_conversation_id_must_not_exceed_service_limit(self) -> None:
        """验证 conversation_id 长度超过 100 字符时被 Pydantic 校验拦截并返回 HTTP 422。"""
        response = await self._post(
            f"/api/conversations/{'c' * 101}/messages",
            json={"message": "继续之前的方案"},
        )

        self.assertEqual(response.status_code, 422)

    async def test_request_schema_exposes_optional_clarification_id(self) -> None:
        """验证 OpenAPI Schema 正确暴露了可选的 clarification_id 澄清字段。"""
        schema = self.app.openapi()
        request_schema = schema["components"]["schemas"][
            "SendMessageRequest"
        ]

        self.assertEqual(
            set(request_schema["properties"]),
            {"message", "clarification_id"},
        )
        self.assertEqual(request_schema["required"], ["message"])

    async def test_clarification_id_is_adapted_for_clarification_answer(
        self,
    ) -> None:
        """验证携带 clarification_id 时正确适配并触发澄清回答流程，返回 202 retry_pending。"""
        self.use_case.execute.return_value = SendConversationMessageResult(
            conversation_id="conv_test_001",
            turn_id="turn-question",
            plan_id="plan-question",
            status="retry_pending",
        )

        response = await self._post(
            "/api/conversations/conv_test_001/messages",
            json={
                "message": "文档 7",
                "clarification_id": "turn-question",
            },
        )

        self.assertEqual(response.status_code, 202)
        self.use_case.execute.assert_awaited_once_with(
            SendConversationMessageCommand(
                conversation_id="conv_test_001",
                message="文档 7",
                clarification_id="turn-question",
            )
        )

    async def test_clarification_errors_are_mapped_to_safe_4xx(self) -> None:
        """验证 ClarificationApplicationError 分别映射为安全的 HTTP 400、404、409 状态码，避免暴露 500 内部错误。"""
        cases = [
            (400, "Clarification 回答不能为空"),
            (404, "Clarification 不存在"),
            (409, "Clarification 当前状态不允许回答"),
        ]

        for status_code, detail in cases:
            with self.subTest(status_code=status_code):
                self.use_case.execute.side_effect = (
                    ClarificationApplicationError(status_code, detail)
                )
                response = await self._post(
                    "/api/conversations/conv_test_001/messages",
                    json={
                        "message": "文档 7",
                        "clarification_id": "turn-question",
                    },
                )

                self.assertEqual(response.status_code, status_code)
                self.assertEqual(response.json(), {"detail": detail})


if __name__ == "__main__":
    unittest.main()
