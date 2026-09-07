"""读取 Conversation Turn 与最新 Plan 状态事实的应用层用例。"""

from __future__ import annotations

from app.modules.conversation.application.dto import TurnStatusResult


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

            # 3. 组装状态结果 DTO 并返回
            return TurnStatusResult(
                conversation_id=conversation_id,
                turn_id=turn_id,
                turn_status=turn.status,
                plan_id=None if plan is None else plan.plan_id,
                plan_status=None if plan is None else plan.status,
                revision=None if plan is None else plan.revision,
                task_ids=list(turn.task_ids or []),
                assistant_message=turn.assistant_content,
            )
