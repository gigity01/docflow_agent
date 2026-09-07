"""文档模块知识库主表（KnowledgeBase）ORM 模型的只读查询仓储。"""

from sqlalchemy.orm import Session

from app.modules.document.infrastructure.persistence.models.knowledge_base import (
    KnowledgeBase,
)


class KnowledgeBaseRepository:
    """提供知识库实体查询的数据访问仓储类。"""

    def __init__(self, db: Session) -> None:
        """初始化知识库仓储。

        Args:
            db: SQLAlchemy 数据库会话。
        """
        self.db = db

    def get_by_id(self, kb_id: int) -> KnowledgeBase | None:
        """根据主键 ID 查询知识库实体。

        Args:
            kb_id: 知识库主键 ID。

        Returns:
            找到返回实体，否则返回 None。
        """
        return (
            self.db.query(KnowledgeBase)
            .filter(KnowledgeBase.id == kb_id)
            .first()
        )
