"""文档模块父级语义块（ParentBlock）ORM 模型的持久化与查询仓储。

封装父级语义块的创建、批量新增、切块重建时的物理级联删除、多条件检索与语义组查询。
"""

from sqlalchemy import String, cast, func, or_
from sqlalchemy.orm import Session

from app.modules.document.application.dto import ParentBlockSearchQuery
from app.modules.document.infrastructure.persistence.models.parent_block import (
    ParentBlock,
)


class ParentBlockRepository:
    """父级语义块数据访问仓储类。"""

    def __init__(self, db: Session) -> None:
        """初始化父块仓储。

        Args:
            db: SQLAlchemy 数据库会话。
        """
        self.db = db

    def create(self, parent_block: ParentBlock) -> ParentBlock:
        """持久化单个父级语义块实体并 flush。

        Args:
            parent_block: 待插入的父块实体。

        Returns:
            包含自增主键的实体。
        """
        self.db.add(parent_block)
        self.db.flush()
        return parent_block

    def create_many(
        self,
        parent_blocks: list[ParentBlock],
    ) -> list[ParentBlock]:
        """批量持久化父级语义块实体列表并统一 flush。

        Args:
            parent_blocks: 待插入的父块实体列表。

        Returns:
            包含主键的实体列表。
        """
        if not parent_blocks:
            return []

        self.db.add_all(parent_blocks)
        self.db.flush()
        return parent_blocks

    def delete_by_doc_id(self, doc_id: int) -> None:
        """物理删除指定文档关联的全部父块记录。

        Args:
            doc_id: 文档 ID。
        """
        (
            self.db.query(ParentBlock)
            .filter(ParentBlock.doc_id == doc_id)
            .delete(synchronize_session=False)
        )

    def count_active_by_doc_id(self, doc_id: int) -> int:
        """统计文档当前 active 状态的父块数量。

        Args:
            doc_id: 文档 ID。

        Returns:
            活跃父块数。
        """
        return (
            self.db.query(ParentBlock.id)
            .filter(
                ParentBlock.doc_id == doc_id,
                ParentBlock.status == "active",
            )
            .count()
        )

    def search(self, filters: ParentBlockSearchQuery) -> list[ParentBlock]:
        """按受限多条件检索父块列表并按 (doc_id, block_index) 升序分页。

        Args:
            filters: 查询过滤条件 DTO。

        Returns:
            父块实体列表。
        """
        return (
            self._search_query(filters)
            .order_by(
                ParentBlock.doc_id.asc(),
                ParentBlock.block_index.asc(),
                ParentBlock.id.asc(),
            )
            .offset(filters.offset)
            .limit(filters.limit)
            .all()
        )

    def count_search(self, filters: ParentBlockSearchQuery) -> int:
        """统计多条件检索匹配的父块总记录数。

        Args:
            filters: 查询过滤条件 DTO。

        Returns:
            匹配总记录数。
        """
        return self._search_query(filters).count()

    def _search_query(self, filters: ParentBlockSearchQuery):
        """构建父块多条件查询 SQLAlchemy Query。"""
        query = self.db.query(ParentBlock)
        list_filters = (
            (ParentBlock.doc_id, filters.document_ids),
            (ParentBlock.id, filters.parent_ids),
            (ParentBlock.kb_id, filters.kb_ids),
            (ParentBlock.block_type, filters.block_types),
            (ParentBlock.status, filters.statuses),
        )
        for column, values in list_filters:
            if values:
                query = query.filter(column.in_(values))
        section_path = (
            filters.section_path_contains.strip()
            if filters.section_path_contains
            else None
        )
        if section_path:
            query = query.filter(
                cast(ParentBlock.section_path, String).contains(
                    section_path,
                    autoescape=True,
                )
            )
        keyword = filters.keyword.strip() if filters.keyword else None
        if keyword:
            query = query.filter(
                or_(
                    ParentBlock.title.contains(keyword, autoescape=True),
                    ParentBlock.content.contains(keyword, autoescape=True),
                )
            )
        return query

    def count_by_status_for_document(self, doc_id: int) -> dict[str, int]:
        """按状态（active/inactive）统计文档内父块数量分布。

        Args:
            doc_id: 文档 ID。

        Returns:
            状态 -> 数量的字典映射。
        """
        rows = (
            self.db.query(ParentBlock.status, func.count(ParentBlock.id))
            .filter(ParentBlock.doc_id == doc_id)
            .group_by(ParentBlock.status)
            .all()
        )
        return {status: count for status, count in rows}

    def count_active_for_kb(self, kb_id: int) -> int:
        """统计知识库当前所有 active 状态的父块总数。

        Args:
            kb_id: 知识库 ID。

        Returns:
            活跃父块总数。
        """
        return (
            self.db.query(ParentBlock.id)
            .filter(
                ParentBlock.kb_id == kb_id,
                ParentBlock.status == "active",
            )
            .count()
        )

    def list_by_semantic_group(
        self,
        *,
        doc_id: int,
        semantic_group_index: int,
    ) -> list[ParentBlock]:
        """按组内分段顺序（segment_index）返回指定文档中某一语义组的全部 active 父块。

        Args:
            doc_id: 文档 ID。
            semantic_group_index: 语义组序号。

        Returns:
            语义组内的父块列表。
        """
        return (
            self.db.query(ParentBlock)
            .filter(
                ParentBlock.doc_id == doc_id,
                ParentBlock.semantic_group_index == semantic_group_index,
                ParentBlock.status == "active",
            )
            .order_by(ParentBlock.segment_index.asc())
            .all()
        )
