"""文档模块子块（ChildChunk）ORM 模型的持久化与向量状态管理仓储。

封装可向量化子块的新增、重建全量替换删除、多维条件检索、行锁锁定及向量索引状态机推进（pending -> indexing -> indexed / failed）。
"""

from collections.abc import Iterable
from datetime import datetime

from sqlalchemy import String, cast, func, or_
from sqlalchemy.orm import Session

from app.modules.document.application.dto import ChildChunkSearchQuery
from app.modules.document.infrastructure.persistence.models.child_chunk import (
    ChildChunk,
)


class ChildChunkRepository:
    """可向量化子块数据访问仓储类。"""

    def __init__(self, db: Session) -> None:
        """初始化子块仓储。

        Args:
            db: SQLAlchemy 数据库会话。
        """
        self.db = db

    def create(self, child_chunk: ChildChunk) -> ChildChunk:
        """持久化单个子块并 flush。

        Args:
            child_chunk: 待插入的子块实体。

        Returns:
            包含自增主键的子块实体。
        """
        self.db.add(child_chunk)
        self.db.flush()
        return child_chunk

    def create_many(self, child_chunks: list[ChildChunk]) -> list[ChildChunk]:
        """批量持久化子块实体列表并统一 flush。

        Args:
            child_chunks: 子块实体列表。

        Returns:
            插入后的子块实体列表。
        """
        if not child_chunks:
            return []

        self.db.add_all(child_chunks)
        self.db.flush()
        return child_chunks

    def delete_by_doc_id(self, doc_id: int) -> None:
        """物理删除指定文档关联的所有子块记录（切块重建时调用）。

        Args:
            doc_id: 文档 ID。
        """
        (
            self.db.query(ChildChunk)
            .filter(ChildChunk.doc_id == doc_id)
            .delete(synchronize_session=False)
        )

    def exists_by_doc_id(self, doc_id: int) -> bool:
        """检查指定文档是否已存在 active 状态的子块记录。

        Args:
            doc_id: 文档 ID。

        Returns:
            存在返回 True，否则返回 False。
        """
        return (
            self.db.query(ChildChunk.id)
            .filter(
                ChildChunk.doc_id == doc_id,
                ChildChunk.status == "active",
            )
            .first()
            is not None
        )

    def exists_by_doc_id_and_vector_status(
        self,
        doc_id: int,
        vector_status: str,
    ) -> bool:
        """检查文档是否存在特定向量状态的 active 子块。

        Args:
            doc_id: 文档 ID。
            vector_status: 向量状态（如 'pending', 'indexing', 'failed'）。

        Returns:
            存在返回 True，否则返回 False。
        """
        return (
            self.db.query(ChildChunk.id)
            .filter(
                ChildChunk.doc_id == doc_id,
                ChildChunk.status == "active",
                ChildChunk.vector_status == vector_status,
            )
            .first()
            is not None
        )

    def count_active_not_indexed_by_doc_id(self, doc_id: int) -> int:
        """统计文档中尚未完成向量索引（vector_status != 'indexed'）的 active 子块数。

        Args:
            doc_id: 文档 ID。

        Returns:
            待索引/失败的子块数量。
        """
        return (
            self.db.query(ChildChunk.id)
            .filter(
                ChildChunk.doc_id == doc_id,
                ChildChunk.status == "active",
                ChildChunk.vector_status != "indexed",
            )
            .count()
        )

    def count_by_vector_status_for_document(
        self,
        doc_id: int,
    ) -> dict[str, int]:
        """按向量状态（pending / indexing / indexed / failed）统计文档内 active 子块分布。

        Args:
            doc_id: 文档 ID。

        Returns:
            状态 -> 数量的字典映射。
        """
        rows = (
            self.db.query(
                ChildChunk.vector_status,
                func.count(ChildChunk.id),
            )
            .filter(
                ChildChunk.doc_id == doc_id,
                ChildChunk.status == "active",
            )
            .group_by(ChildChunk.vector_status)
            .all()
        )
        return {status: count for status, count in rows}

    def search(self, filters: ChildChunkSearchQuery) -> list[ChildChunk]:
        """按复合条件检索子块列表并稳定分页。

        Args:
            filters: 查询过滤条件 DTO。

        Returns:
            子块实体列表。
        """
        return (
            self._search_query(filters)
            .order_by(
                ChildChunk.doc_id.asc(),
                ChildChunk.parent_id.asc(),
                ChildChunk.chunk_index.asc(),
                ChildChunk.id.asc(),
            )
            .offset(filters.offset)
            .limit(filters.limit)
            .all()
        )

    def count_search(self, filters: ChildChunkSearchQuery) -> int:
        """统计复合查询条件匹配的子块总数。

        Args:
            filters: 查询过滤条件 DTO。

        Returns:
            总记录数。
        """
        return self._search_query(filters).count()

    def _search_query(self, filters: ChildChunkSearchQuery):
        """构建子块多条件组合查询 SQLAlchemy Query。"""
        query = self.db.query(ChildChunk)
        scalar_filters = (
            (ChildChunk.doc_id, filters.document_id),
            (ChildChunk.parent_id, filters.parent_id),
            (ChildChunk.kb_id, filters.kb_id),
        )
        for column, value in scalar_filters:
            if value is not None:
                query = query.filter(column == value)
        if filters.vector_statuses:
            query = query.filter(
                ChildChunk.vector_status.in_(filters.vector_statuses)
            )
        if filters.statuses:
            query = query.filter(ChildChunk.status.in_(filters.statuses))
        section_path = (
            filters.section_path_contains.strip()
            if filters.section_path_contains
            else None
        )
        if section_path:
            query = query.filter(
                cast(ChildChunk.section_path, String).contains(
                    section_path,
                    autoescape=True,
                )
            )
        if filters.source_row_from is not None:
            query = query.filter(
                ChildChunk.source_row_index >= filters.source_row_from
            )
        if filters.source_row_to is not None:
            query = query.filter(
                ChildChunk.source_row_index <= filters.source_row_to
            )
        if filters.has_vector_id is True:
            query = query.filter(ChildChunk.qdrant_point_id.is_not(None))
        elif filters.has_vector_id is False:
            query = query.filter(ChildChunk.qdrant_point_id.is_(None))
        keyword = filters.keyword.strip() if filters.keyword else None
        if keyword:
            query = query.filter(
                or_(
                    ChildChunk.content.contains(keyword, autoescape=True),
                    ChildChunk.embedding_text.contains(
                        keyword,
                        autoescape=True,
                    ),
                )
            )
        return query

    def count_by_status_for_document(self, doc_id: int) -> dict[str, int]:
        """统计文档内所有子块的 status（active/inactive）分布。"""
        rows = (
            self.db.query(ChildChunk.status, func.count(ChildChunk.id))
            .filter(ChildChunk.doc_id == doc_id)
            .group_by(ChildChunk.status)
            .all()
        )
        return {status: count for status, count in rows}

    def count_all_by_vector_status_for_document(
        self,
        doc_id: int,
    ) -> dict[str, int]:
        """统计文档内所有（含非 active）子块的 vector_status 分布。"""
        rows = (
            self.db.query(
                ChildChunk.vector_status,
                func.count(ChildChunk.id),
            )
            .filter(ChildChunk.doc_id == doc_id)
            .group_by(ChildChunk.vector_status)
            .all()
        )
        return {status: count for status, count in rows}

    def count_by_chunk_type_for_document(
        self,
        doc_id: int,
    ) -> dict[str, int]:
        """统计文档内按 chunk_type（如 text, csv_row）划分的子块数量分布。"""
        rows = (
            self.db.query(ChildChunk.chunk_type, func.count(ChildChunk.id))
            .filter(ChildChunk.doc_id == doc_id)
            .group_by(ChildChunk.chunk_type)
            .all()
        )
        return {chunk_type: count for chunk_type, count in rows}

    def count_vector_id_presence_for_document(
        self,
        doc_id: int,
    ) -> tuple[int, int]:
        """统计文档中已关联 Qdrant Point ID 与未关联的子块数量元组 (with_id, without_id)。"""
        with_vector_id = (
            self.db.query(ChildChunk.id)
            .filter(
                ChildChunk.doc_id == doc_id,
                ChildChunk.qdrant_point_id.is_not(None),
            )
            .count()
        )
        without_vector_id = (
            self.db.query(ChildChunk.id)
            .filter(
                ChildChunk.doc_id == doc_id,
                ChildChunk.qdrant_point_id.is_(None),
            )
            .count()
        )
        return with_vector_id, without_vector_id

    def count_active_for_kb(self, kb_id: int) -> int:
        """统计知识库当前所有 active 状态的子块总数。"""
        return (
            self.db.query(ChildChunk.id)
            .filter(
                ChildChunk.kb_id == kb_id,
                ChildChunk.status == "active",
            )
            .count()
        )

    def count_by_vector_status_for_kb(self, kb_id: int) -> dict[str, int]:
        """按向量状态统计知识库内所有 active 子块的分布字典。"""
        rows = (
            self.db.query(
                ChildChunk.vector_status,
                func.count(ChildChunk.id),
            )
            .filter(
                ChildChunk.kb_id == kb_id,
                ChildChunk.status == "active",
            )
            .group_by(ChildChunk.vector_status)
            .all()
        )
        return {status: count for status, count in rows}

    def list_indexable_by_doc_id(
        self,
        doc_id: int,
        statuses: set[str],
    ) -> list[ChildChunk]:
        """查询文档中处于指定向量状态（如 {'pending', 'failed'}）的 active 子块列表。

        按 (parent_id, chunk_index) 稳定升序排序。

        Args:
            doc_id: 文档 ID。
            statuses: 待领取的向量状态集合。

        Returns:
            待处理子块列表。
        """
        if not statuses:
            return []

        return (
            self.db.query(ChildChunk)
            .filter(
                ChildChunk.doc_id == doc_id,
                ChildChunk.status == "active",
                ChildChunk.vector_status.in_(statuses),
            )
            .order_by(
                ChildChunk.parent_id.asc(),
                ChildChunk.chunk_index.asc(),
            )
            .all()
        )

    def list_by_ids_for_update(
        self,
        doc_id: int,
        chunk_ids: Iterable[int],
    ) -> list[ChildChunk]:
        """按主键升序加悲观行锁（FOR UPDATE）查询指定文档的一组子块。

        用于在 Finalize 短事务中复核并提交索引结果。

        Args:
            doc_id: 文档 ID。
            chunk_ids: 待锁定的子块 ID 集合。

        Returns:
            锁定后的子块列表。
        """
        ordered_ids = sorted(set(chunk_ids))
        if not ordered_ids:
            return []

        return (
            self.db.query(ChildChunk)
            .filter(
                ChildChunk.doc_id == doc_id,
                ChildChunk.id.in_(ordered_ids),
            )
            .order_by(ChildChunk.id)
            .with_for_update()
            .all()
        )

    def mark_indexing(self, chunks: list[ChildChunk]) -> None:
        """将一组子块的 vector_status 切换为 'indexing' 并 flush。

        Args:
            chunks: 待推进状态的子块列表。
        """
        for chunk in chunks:
            chunk.vector_status = "indexing"

        self.db.flush()

    def mark_indexed_many(self, chunks: list[ChildChunk]) -> None:
        """将一组子块的 vector_status 标记为 'indexed'，设置 qdrant_point_id=str(id)，并记录 indexed_at。

        Args:
            chunks: 向量写入成功的子块列表。
        """
        indexed_at = datetime.now()
        for chunk in chunks:
            chunk.vector_status = "indexed"
            chunk.qdrant_point_id = str(chunk.id)
            chunk.indexed_at = indexed_at

        self.db.flush()

    def mark_failed(self, chunks: list[ChildChunk]) -> None:
        """仅将当前处于 'indexing' 状态的子块标记为 'failed' 并 flush。

        Args:
            chunks: 待标记失败的子块列表。
        """
        for chunk in chunks:
            if chunk.vector_status == "indexing":
                chunk.vector_status = "failed"

        self.db.flush()
