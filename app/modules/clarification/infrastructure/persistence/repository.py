"""ClarificationRequest 仓储实现。"""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.modules.clarification.infrastructure.persistence.models import (
    ClarificationRequest,
)


class ClarificationRepository:
    """ClarificationRequest 数据库仓储。

    提供 ClarificationRequest 实体的增删改查及带行级锁的事务查询。
    遵从规范：仓储内部不自行提交事务（commit），仅执行 flush 操作。
    """

    def __init__(self, db: Session) -> None:
        """初始化 ClarificationRepository。

        Args:
            db: SQLAlchemy Session 数据库会话实例。
        """
        self.db = db

    def add(self, request: ClarificationRequest) -> ClarificationRequest:
        """添加并持久化新的澄清请求记录（flush 到数据库）。

        Args:
            request: 待添加的 ClarificationRequest 实体实例。

        Returns:
            添加后的实体对象。
        """
        self.db.add(request)
        self.db.flush()
        return request

    def get_open_for_conversation_for_update(
        self, conversation_id: str
    ) -> ClarificationRequest | None:
        """查询指定会话中处于 open 状态的最晚一条澄清请求，并加行级排他锁。

        Args:
            conversation_id: 会话唯一标识。

        Returns:
            命中的澄清请求实体，若无则返回 None。
        """
        return (
            self.db.query(ClarificationRequest)
            .filter(
                ClarificationRequest.conversation_id == conversation_id,
                ClarificationRequest.status == "open",
            )
            .order_by(ClarificationRequest.created_at.desc())
            .with_for_update()
            .first()
        )

    def get_by_source_turn_id_for_update(
        self,
        source_turn_id: str,
    ) -> ClarificationRequest | None:
        """根据源 Turn ID 查询澄清请求，并加行级排他锁。

        Args:
            source_turn_id: 发起澄清提问的源 Turn 标识。

        Returns:
            命中的澄清请求实体，若无则返回 None。
        """
        return (
            self.db.query(ClarificationRequest)
            .filter(ClarificationRequest.source_turn_id == source_turn_id)
            .with_for_update()
            .first()
        )

    def get_by_plan_id(self, plan_id: str) -> ClarificationRequest | None:
        """根据源 Plan ID 查询澄清请求（只读查询，不加锁）。

        Args:
            plan_id: 发起澄清的源 Plan 标识。

        Returns:
            命中的澄清请求实体，若无则返回 None。
        """
        return (
            self.db.query(ClarificationRequest)
            .filter(ClarificationRequest.source_plan_id == plan_id)
            .first()
        )

    def get_by_plan_id_for_update(
        self, plan_id: str
    ) -> ClarificationRequest | None:
        """根据源 Plan ID 查询澄清请求，并加行级排他锁。

        Args:
            plan_id: 发起澄清的源 Plan 标识。

        Returns:
            命中的澄清请求实体，若无则返回 None。
        """
        return (
            self.db.query(ClarificationRequest)
            .filter(ClarificationRequest.source_plan_id == plan_id)
            .with_for_update()
            .first()
        )
