"""Context → Planning → 状态分派的用户消息主链路。"""

from __future__ import annotations

import asyncio

from app.modules.context.application.dto import (
    CompleteTurnCommand,
    SendMessageCommand,
)
from app.modules.context.application.attribution_policy import (
    build_read_set_fallback_attribution,
)
from app.modules.context.domain.selection_policy import (
    derive_context_selection_mode,
)
from app.modules.conversation.application.dto import (
    ContextSelectionMetadata,
    SendConversationMessageCommand,
    SendConversationMessageResult,
)
from app.modules.planning.application.dto import RunPlanningInput
from app.modules.planning.domain.enums import PlanStatus


class SendConversationMessageUseCase:
    """处理用户会话消息的核心编排用例。

    编排主流程：
    1. 澄清回复分支：若请求携带 `source_turn_id`，说明用户正在回复先前的澄清提问。
       此时调用 AnswerClarificationUseCase 将回答写入源 Turn，并将澄清标记为 answered，发布 Replan 异步事件。
    2. 普通消息分支：
       - 上下文选择（Context Selection）：调用 ContextService.send_message，通过 Context Agent 判断消息关联的历史链（Read Set）并持久化 Turn。
       - 规划执行（Run Planning）：多阶段运行 Planner（Evidence 取证 -> Gap 缺口分析 -> Commit 决策）。
       - 结果分派：根据 Planner 返回的 PlanStatus（READY / UNSUPPORTED / NEEDS_CLARIFICATION / RETRY_PENDING / FAILED）执行确定性的下游回写或组装响应结果。
    """

    def __init__(
        self,
        *,
        context_service,
        run_planning,
        answer_clarification,
    ) -> None:
        """初始化 SendConversationMessageUseCase。

        Args:
            context_service: ContextService 实例，用于驱动上下文路由与 Turn 生命周期。
            run_planning: RunPlanningUseCase 实例，用于驱动 Planner 规划流程。
            answer_clarification: AnswerClarificationUseCase 实例，用于处理澄清回答。
        """
        self._context_service = context_service
        self._run_planning = run_planning
        self._answer_clarification = answer_clarification

    async def execute(
        self,
        command: SendConversationMessageCommand,
    ) -> SendConversationMessageResult:
        """执行发送会话消息用例。

        Args:
            command: 发送消息命令，包含会话 ID、用户消息内容及可选的澄清源 Turn ID。

        Returns:
            包含会话 ID、Turn ID、Plan ID、状态及上下文选择元数据。

        Raises:
            ClarificationApplicationError: 澄清回答业务异常。
            ContextApplicationError: 上下文路由与会话锁定异常。
            PlanningApplicationError: Planner 规划应用层异常。
        """
        # 分支 1：处理针对已有澄清请求的回复
        if command.source_turn_id is not None:
            plan_id = await asyncio.to_thread(
                self._answer_clarification.execute,
                conversation_id=command.conversation_id,
                source_turn_id=command.source_turn_id,
                answer=command.message,
            )

            return SendConversationMessageResult(
                conversation_id=command.conversation_id,
                turn_id=command.source_turn_id,
                plan_id=plan_id,
                status="retry_pending",
            )

        # 分支 2：普通完整用户消息，执行历史 Context Read Set 选择
        selection = await self._context_service.send_message(
            SendMessageCommand(
                conversation_id=command.conversation_id,
                message=command.message,
            )
        )
        selection_metadata = ContextSelectionMetadata(
            selection_mode=derive_context_selection_mode(
                selection.decision.relevant_chain_ids
            ),
            relevant_chain_ids=list(
                selection.decision.relevant_chain_ids
            ),
            reason_summary=selection.decision.reason_summary,
        )
        fallback_attribution = build_read_set_fallback_attribution(
            selection.decision.relevant_chain_ids
        )

        # 执行 Planner 规划（Evidence -> Gap -> Commit）
        planning = await self._run_planning.execute(
            RunPlanningInput(
                conversation_id=selection.conversation_id,
                turn_id=selection.turn_id,
                revision=1,
            )
        )

        # 结果分派 1：Plan 已就绪（READY），已持久化 Task DAG 并发布 plan_wakeup 事件
        if planning.status == PlanStatus.READY:
            return SendConversationMessageResult(
                conversation_id=selection.conversation_id,
                turn_id=selection.turn_id,
                plan_id=planning.plan_id,
                status="processing",
                task_ids=planning.task_ids,
                context_selection=selection_metadata,
            )

        # 结果分派 2：请求超出当前系统能力范围（UNSUPPORTED），直接完成 Turn 并返回原因
        if planning.status == PlanStatus.UNSUPPORTED:
            message = planning.failure_reason or "当前业务暂不支持该请求。"
            await self._context_service.complete_turn(
                selection.turn_id,
                CompleteTurnCommand(
                    assistant_content=message,
                    assistant_compact=message,
                    task_ids=[],
                    task_result_summary=message,
                    attribution=fallback_attribution,
                ),
            )
            return SendConversationMessageResult(
                conversation_id=selection.conversation_id,
                turn_id=selection.turn_id,
                plan_id=planning.plan_id,
                status="unsupported",
                assistant_message=message,
                context_selection=selection_metadata,
            )

        # 结果分派 3：Planner 发现信息缺口或歧义，已创建 ClarificationRequest（NEEDS_CLARIFICATION）
        if planning.status == PlanStatus.NEEDS_CLARIFICATION:
            question = planning.clarification_question
            if not question:
                raise RuntimeError("Clarification Agent 未生成问题")
            return SendConversationMessageResult(
                conversation_id=selection.conversation_id,
                turn_id=selection.turn_id,
                plan_id=planning.plan_id,
                status="needs_clarification",
                assistant_message=question,
                context_selection=selection_metadata,
            )

        # 结果分派 4：规划异常或外部重试请求（RETRY_PENDING）
        if planning.status == PlanStatus.RETRY_PENDING:
            return SendConversationMessageResult(
                conversation_id=selection.conversation_id,
                turn_id=selection.turn_id,
                plan_id=planning.plan_id,
                status="retry_pending",
                context_selection=selection_metadata,
            )

        # 结果分派 5：规划失败终态（FAILED）
        return SendConversationMessageResult(
            conversation_id=selection.conversation_id,
            turn_id=selection.turn_id,
            plan_id=planning.plan_id,
            status="failed",
            assistant_message=planning.failure_reason,
            context_selection=selection_metadata,
        )
