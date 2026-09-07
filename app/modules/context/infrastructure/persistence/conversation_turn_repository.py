"""ConversationTurn 的窄仓储，供跨模块事务协作。"""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.modules.context.infrastructure.persistence.models.conversation_turn import (
    ConversationTurn,
)


class ConversationTurnRepository:
    """提供 Planning 等模块所需的 Turn 查询、行锁锁定、task_ids 与 status 快速更新。"""

    def __init__(self, db: Session) -> None:
        """初始化 ConversationTurnRepository。

        Args:
            db: SQLAlchemy 数据库会话。
        """
        self.db = db

    def get_by_id(self, turn_id: str) -> ConversationTurn | None:
        """根据 turn_id 查询 Turn 实体（不加锁）。

        Args:
            turn_id: Turn 唯一标识。

        Returns:
            命中的实体或 None。
        """
        return (
            self.db.query(ConversationTurn)
            .filter(ConversationTurn.turn_id == turn_id)
            .first()
        )

    def get_by_id_for_update(
        self,
        turn_id: str,
    ) -> ConversationTurn | None:
        """根据 turn_id 获取带有行级排他锁的 Turn 实体。

        Args:
            turn_id: Turn 唯一标识。

        Returns:
            锁定的实体或 None。
        """
        return (
            self.db.query(ConversationTurn)
            .filter(ConversationTurn.turn_id == turn_id)
            .with_for_update()
            .first()
        )

    def set_task_ids(
        self,
        turn: ConversationTurn,
        task_ids: list[str],
    ) -> ConversationTurn:
        """更新 Turn 关联的任务 ID 列表并刷新至会话。

        Args:
            turn: ConversationTurn 实体实例。
            task_ids: Task ID 列表。

        Returns:
            更新后的实体。
        """
        turn.task_ids = list(task_ids)
        self.db.flush()
        return turn

    def set_status(self, turn: ConversationTurn, status: str) -> ConversationTurn:
        """更新 Turn 的生命周期状态并刷新至会话。

        Args:
            turn: ConversationTurn 实体实例。
            status: 新状态字符串（如 ContextTurnStatus）。

        Returns:
            更新后的实体。
        """
        turn.status = status
        self.db.flush()
        return turn
