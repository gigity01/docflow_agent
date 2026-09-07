"""读取 Conversation Turn 与最新 Plan 状态事实的应用层用例。"""

from __future__ import annotations

from app.modules.conversation.application.dto import TurnStatusResult
from app.modules.messaging.application.delivery import delivery_view


class GetTurnStatusUseCase:
    """读取指定 Conversation Turn 及其最新 Plan revision 执行状态的只读用例。

    供客户端通过 GET /api/conversations/{conversation_id}/turns/{turn_id} 轮询异步任务执行进度与最终助手回答。
    """

    def __init__(self, uow_factory) -> None:
        """初始化 GetTurnStatusUseCase。

        Args:
            uow_factory: UnitOfWork 工厂，用于提供只读数据库会话。
        """
        self._uow_factory = uow_factory

    def execute(self, conversation_id: str, turn_id: str) -> TurnStatusResult:
        """查询 Turn 及其关联的最新 Plan Revision 状态与任务结果。

        Args:
            conversation_id: 会话唯一标识。
            turn_id: 轮次唯一标识。

        Returns:
            包含当前 Turn 状态、最新 Plan ID/状态/版本号、Task 列表以及最终助手文本。

        Raises:
            ValueError: 当指定的 Turn 不存在或不属于当前会话时抛出。
        """
        with self._uow_factory() as uow:
            # 1. 查询 ConversationTurn 并核验会话归属
            turn = uow.conversation_turns.get_by_id(turn_id)
            if turn is None or turn.conversation_id != conversation_id:
                raise ValueError("Conversation Turn 不存在")

            # 2. 查询该 Turn 下具有最大 revision 的最新 Plan
            plan = uow.plans.get_latest_by_turn(turn_id)

            tasks = []
            failures = []
            next_action = None
            if plan is not None:
                for history in uow.plans.list_by_turn(turn_id):
                    executions = uow.task_executions.list_by_plan_id(history.plan_id)
                    latest = {execution.task_id: execution for execution in executions}
                    for task in uow.tasks.list_by_plan_id(history.plan_id):
                        execution = latest.get(task.task_id)
                        tasks.append({"task_id": task.task_id, "capability": task.capability_code,
                            "plan_id": history.plan_id, "revision": history.revision,
                            "status": task.status, "attempts": task.attempt_count,
                            "execution_status": execution.status if execution else None,
                            "error_code": execution.error_code if execution else None,
                            "error_message": execution.error_message if execution else None,
                            "compensation_error": execution.compensation_last_error if execution else None,
                            "result": execution.output_json if execution and execution.status == "succeeded" else None})
                failures = [delivery_view(row).model_dump() for row in uow.inbox.list_delivery_failures(plan_id=plan.plan_id)]
                failures.extend({"failure_id": "outbox:" + row.event_id, "stage": "publish",
                    "status": "dead_letter", "attempts": row.attempts,
                    "reason": "消息发布失败，检查 Redis 后恢复",
                    "next_action": "调用失败消息 retry 接口"}
                    for row in uow.outbox.list_failed(plan_id=plan.plan_id))
                if any(task["execution_status"] == "compensation_locked" for task in tasks):
                    next_action = "需要人工处理：修复补偿错误后调用计划 recover 接口；原执行归属仍保留"
                elif failures:
                    next_action = "检查 message_failures 中的原因和恢复方式"
                elif plan.status == "needs_clarification":
                    next_action = "使用当前 open 的 clarification_id 提交回答"
                elif plan.status == "failed":
                    next_action = "检查失败原因后调用计划 recover；澄清额度耗尽时需补全信息并提交新请求"
                elif plan.status in {"unsupported", "cancelled"}:
                    next_action = "补充明确的信息后提交新请求"
                elif plan.status == "completed":
                    next_action = "读取执行结果"
                else:
                    next_action = "等待后台处理；异常中断时可调用计划 recover 接口"
            clarifications = [{"clarification_id": item.clarification_id, "round": item.round,
                "status": item.status, "question": item.question, "answer": item.answer_text,
                "required_information": item.required_information_json}
                for item in uow.clarifications.list_by_turn(turn_id)]
            # 3. 组装状态结果 DTO 并返回
            return TurnStatusResult(
                conversation_id=conversation_id,
                turn_id=turn_id,
                turn_status=turn.status,
                plan_id=None if plan is None else plan.plan_id,
                plan_status=None if plan is None else plan.status,
                revision=None if plan is None else plan.revision,
                task_ids=[task["task_id"] for task in tasks if task["plan_id"] == plan.plan_id] if plan else list(turn.task_ids or []),
                assistant_message=turn.assistant_content,
                failure_code=plan.failure_code if plan else None,
                failure_reason=plan.failure_reason if plan else None,
                next_action=next_action, tasks=tasks, message_failures=failures,
                clarifications=clarifications,
            )
