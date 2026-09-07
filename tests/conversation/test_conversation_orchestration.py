"""Conversation 协调用例（SendConversationMessageUseCase）状态分派与业务流转测试。

核心业务不变量（遵循 AGENTS.md 规范）：
1. 规划成功（PlanStatus.READY）：
   - 协调器接收到已发布的 Plan，直接返回 status="processing" 与关联 task_ids，不内联执行 Task（由独立 Runtime Worker 异步执行），且不调用 complete_turn。
2. 能力不支持（PlanStatus.UNSUPPORTED）：
   - 规划判定能力不支持时，立即调用 context_service.complete_turn 完成当前 Turn，回写助手回答并保留已有链归属。
3. 澄清请求（PlanStatus.NEEDS_CLARIFICATION）：
   - 规划产生澄清请求时，返回 status="needs_clarification" 与澄清问题，保持当前源 Turn 为 open 状态（不调用 complete_turn）。
4. 澄清回答（clarification_id 回复）：
   - 当用户提交澄清回答时，复用原有 clarification_id，不重复运行 Context 路由，直接写入澄清输入并触发 Replan，返回 retry_pending。
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest import mock

from app.modules.context.application.dto import ContextSelectionResult
from app.modules.context.domain.models import ContextSelectionDecision
from app.modules.conversation.application.dto import (
    SendConversationMessageCommand,
)
from app.modules.conversation.application.send_message import (
    SendConversationMessageUseCase,
)
from app.modules.planning.application.dto import RunPlanningResult
from app.modules.planning.domain.enums import PlanStatus


def _selection(
    relevant_chain_ids: list[str] | None = None,
) -> ContextSelectionResult:
    """构造测试用 ContextSelectionResult 上下文选择结果对象。"""
    relevant_chain_ids = relevant_chain_ids or []
    return ContextSelectionResult(
        conversation_id="conversation-1",
        turn_id="turn-1",
        message="处理文档 7",
        context_chains=[],
        decision=ContextSelectionDecision(
            relevant_chain_ids=relevant_chain_ids,
            reason_summary="不需要历史上下文",
        ),
    )


class ConversationOrchestrationTest(unittest.IsolatedAsyncioTestCase):
    """验证 SendConversationMessageUseCase 在各类 Planner 输出状态下的协调编排逻辑。"""

    def setUp(self) -> None:
        """初始化 ContextService、RunPlanning 与 AnswerClarification 的模拟替身。"""
        self.context = mock.Mock()
        self.context.send_message = mock.AsyncMock(return_value=_selection())
        self.context.complete_turn = mock.AsyncMock()
        self.planning = mock.Mock()
        self.planning.execute = mock.AsyncMock()
        self.answer = mock.Mock()
        self.answer.execute.return_value = None
        self.use_case = SendConversationMessageUseCase(
            context_service=self.context,
            run_planning=self.planning,
            answer_clarification=self.answer,
        )

    async def test_ready_returns_processing_without_running_tasks_inline(self) -> None:
        """验证 Plan 处于 READY 状态时，协调器返回 processing 并保留任务由 Worker 消费，不内联执行或提前完成 Turn。"""
        self.planning.execute.return_value = RunPlanningResult(
            plan_id="plan-1",
            turn_id="turn-1",
            status=PlanStatus.READY,
            task_ids=["task-1"],
            failure_reason=None,
        )
        result = await self.use_case.execute(
            SendConversationMessageCommand(
                conversation_id="conversation-1",
                message="处理文档 7",
            )
        )
        self.assertEqual(result.status, "processing")
        self.assertEqual(result.task_ids, ["task-1"])
        self.context.complete_turn.assert_not_awaited()

    async def test_unsupported_completes_current_turn(self) -> None:
        """验证 Plan 处于 UNSUPPORTED 状态时，协调器直接完成当前 Turn 并返回 unsupported 状态。"""
        self.context.send_message.return_value = _selection(["chain-a"])
        self.planning.execute.return_value = RunPlanningResult(
            plan_id="plan-unsupported",
            turn_id="turn-1",
            status=PlanStatus.UNSUPPORTED,
            task_ids=[],
            failure_reason="当前能力不支持",
        )
        result = await self.use_case.execute(
            SendConversationMessageCommand(
                conversation_id="conversation-1",
                message="执行未支持操作",
            )
        )
        self.assertEqual(result.status, "unsupported")
        command = self.context.complete_turn.await_args.args[1]
        self.assertEqual(command.assistant_content, "当前能力不支持")
        self.assertEqual(
            command.attribution.existing_chain_ids,
            ["chain-a"],
        )
        self.assertFalse(command.attribution.create_new_chain)

    async def test_clarification_question_keeps_source_turn_open(self) -> None:
        """验证 Plan 处于 NEEDS_CLARIFICATION 状态时，返回澄清问题并保持源 Turn 处于未完成状态。"""
        self.planning.execute.return_value = RunPlanningResult(
            plan_id="plan-question",
            turn_id="turn-1",
            status=PlanStatus.NEEDS_CLARIFICATION,
            task_ids=[],
            failure_reason="文档不唯一",
            clarification_question="请确认要处理哪一个文档？",
        )
        result = await self.use_case.execute(
            SendConversationMessageCommand(
                conversation_id="conversation-1",
                message="处理那个文档",
            )
        )
        self.assertEqual(result.status, "needs_clarification")
        self.assertEqual(
            result.assistant_message,
            "请确认要处理哪一个文档？",
        )
        self.context.complete_turn.assert_not_awaited()

    async def test_clarification_answer_reuses_source_turn_and_queues_replan(
        self,
    ) -> None:
        """验证用户回复澄清答案时复用 clarification_id，直接调用 AnswerClarification 并触发 Replan 流程。"""
        self.answer.execute.return_value = SimpleNamespace(plan_id="plan-question", turn_id="turn-question")

        result = await self.use_case.execute(
            SendConversationMessageCommand(
                conversation_id="conversation-1",
                message="文档 7",
                clarification_id="turn-question",
            )
        )

        self.assertEqual(result.status, "retry_pending")
        self.assertEqual(result.turn_id, "turn-question")
        self.assertEqual(result.plan_id, "plan-question")
        self.context.send_message.assert_not_awaited()
        self.planning.execute.assert_not_awaited()
        self.answer.execute.assert_called_once_with(
            conversation_id="conversation-1",
            clarification_id="turn-question",
            answer="文档 7",
        )


if __name__ == "__main__":
    unittest.main()
