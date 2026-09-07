"""文档主表（Document）ORM 模型的数据库访问与持久化仓储。

集中封装文档建档、主键行锁锁定、知识库去重哈希查询、多条件高级检索、状态机推进与生命周期停用（deactivate）。
"""

from collections.abc import Iterable

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.modules.document.application.dto import DocumentSearchQuery
from app.modules.document.domain.enums import (
    DocumentLifecycleStatus,
    DocumentStorageStatus,
)
from app.modules.document.infrastructure.persistence.models.document import Document


class DocumentRepository:
    """集中处理文档主表 CRUD、行锁与状态推进的数据访问仓储类。"""

    def __init__(self, db: Session) -> None:
        """绑定当前请求或任务使用的数据库会话。

        Args:
            db: SQLAlchemy 数据库会话。
        """
        self.db = db

    def create(self, document: Document) -> Document:
        """持久化文档并刷新以取得数据库生成字段。

        仓储层只 flush，不 commit；事务提交边界由外层 Application UseCase 统一控制。

        Args:
            document: 待创建的文档实体。

        Returns:
            包含自增主键与默认时间戳的文档实体。
        """
        self.db.add(document)
        self.db.flush()
        self.db.refresh(document)
        return document

    def get_by_id(self, document_id: int) -> Document | None:
        """根据主键 ID 查询单个文档实体。

        Args:
            document_id: 文档主键 ID。

        Returns:
            找到返回实体，否则返回 None。
        """
        return (
            self.db.query(Document)
            .filter(Document.id == document_id)
            .first()
        )

    def list_filtered(
        self,
        *,
        kb_id: int,
        status: str | None = None,
        source_type: str | None = None,
        lifecycle_status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Document]:
        """按知识库及基础状态轴筛选文档，按 ID 降序稳定分页。

        Args:
            kb_id: 所属知识库 ID。
            status: 可选流水线技术状态过滤。
            source_type: 可选源文件类型过滤。
            lifecycle_status: 可选业务生命周期过滤。
            limit: 返回限制数量。
            offset: 偏移量。

        Returns:
            文档实体列表。
        """
        query = self._filtered_query(
            kb_id=kb_id,
            status=status,
            source_type=source_type,
            lifecycle_status=lifecycle_status,
        )
        return (
            query.order_by(Document.id.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )

    def count_filtered(
        self,
        *,
        kb_id: int,
        status: str | None = None,
        source_type: str | None = None,
        lifecycle_status: str | None = None,
    ) -> int:
        """统计与基础列表查询相同过滤条件下的文档总数。

        Args:
            kb_id: 所属知识库 ID。
            status: 流水线技术状态。
            source_type: 源格式。
            lifecycle_status: 业务生命周期状态。

        Returns:
            匹配的文档总记录数。
        """
        return self._filtered_query(
            kb_id=kb_id,
            status=status,
            source_type=source_type,
            lifecycle_status=lifecycle_status,
        ).count()

    def search(self, query: DocumentSearchQuery) -> list[Document]:
        """按多维受限白名单字段筛选并以显式排序稳定分页。

        Args:
            query: 高级检索参数 DTO。

        Returns:
            匹配的文档实体列表。
        """
        filtered = self._search_query(query)
        sort_column = {
            "id": Document.id,
            "created_at": Document.created_at,
            "updated_at": Document.updated_at,
            "indexed_at": Document.indexed_at,
            "title": Document.title,
        }[query.sort_by]
        order = sort_column.asc if query.sort_order == "asc" else sort_column.desc
        id_order = Document.id.asc if query.sort_order == "asc" else Document.id.desc
        order_by = [order()]
        if query.sort_by != "id":
            order_by.append(id_order())
        return (
            filtered.order_by(*order_by)
            .offset(query.offset)
            .limit(query.limit)
            .all()
        )

    def count_search(self, query: DocumentSearchQuery) -> int:
        """统计与高级查询同一过滤条件下的文档数量。

        Args:
            query: 高级检索参数 DTO。

        Returns:
            匹配总记录数。
        """
        return self._search_query(query).count()

    def count_for_kb(
        self,
        kb_id: int,
        *,
        status: str | None = None,
        lifecycle_status: str | None = None,
    ) -> int:
        """按知识库和可选状态轴统计文档总数。

        Args:
            kb_id: 知识库 ID。
            status: 可选流水线技术状态。
            lifecycle_status: 可选业务生命周期状态。

        Returns:
            匹配文档数。
        """
        query = self.db.query(Document.id).filter(Document.kb_id == kb_id)
        if status is not None:
            query = query.filter(Document.status == status)
        if lifecycle_status is not None:
            query = query.filter(
                Document.lifecycle_status == lifecycle_status
            )
        return query.count()

    def _search_query(self, filters: DocumentSearchQuery):
        """构建文档高级复合查询 SQLAlchemy Query。"""
        query = self.db.query(Document)
        list_filters = (
            (Document.kb_id, filters.kb_ids),
            (Document.id, filters.document_ids),
            (Document.doc_code, filters.doc_codes),
            (Document.domain_code, filters.domain_codes),
            (Document.business_scene, filters.business_scenes),
            (Document.status, filters.statuses),
            (Document.lifecycle_status, filters.lifecycle_statuses),
            (Document.storage_status, filters.storage_statuses),
            (Document.source_type, filters.source_types),
            (Document.risk_level, filters.risk_levels),
        )
        for column, values in list_filters:
            if values:
                query = query.filter(column.in_(values))

        keyword = filters.keyword.strip() if filters.keyword else None
        if keyword:
            query = query.filter(
                or_(
                    Document.title.contains(keyword, autoescape=True),
                    Document.doc_code.contains(keyword, autoescape=True),
                    Document.original_filename.contains(
                        keyword,
                        autoescape=True,
                    ),
                )
            )
        original_filename = (
            filters.original_filename.strip()
            if filters.original_filename
            else None
        )
        if original_filename:
            query = query.filter(
                Document.original_filename.contains(
                    original_filename,
                    autoescape=True,
                )
            )
        if filters.created_by_actor_code is not None:
            query = query.filter(
                Document.created_by_actor_code
                == filters.created_by_actor_code
            )

        range_filters = (
            (Document.created_at, filters.created_from, filters.created_to),
            (Document.updated_at, filters.updated_from, filters.updated_to),
            (Document.indexed_at, filters.indexed_from, filters.indexed_to),
        )
        for column, start, end in range_filters:
            if start is not None:
                query = query.filter(column >= start)
            if end is not None:
                query = query.filter(column <= end)
        if filters.effective_at_before is not None:
            query = query.filter(
                Document.effective_at <= filters.effective_at_before
            )
        if filters.expired_at_before is not None:
            query = query.filter(
                Document.expired_at <= filters.expired_at_before
            )

        nullable_filters = (
            (Document.cleaned_uri, filters.has_cleaned_output),
            (Document.active_content_hash, filters.has_active_content_hash),
        )
        for column, expected in nullable_filters:
            if expected is True:
                query = query.filter(column.is_not(None))
            elif expected is False:
                query = query.filter(column.is_(None))
        if filters.replaced_by is not None:
            query = query.filter(Document.replaced_by == filters.replaced_by)
        return query

    def _filtered_query(
        self,
        *,
        kb_id: int,
        status: str | None,
        source_type: str | None,
        lifecycle_status: str | None,
    ):
        """构建基础列表过滤 SQLAlchemy Query。"""
        query = self.db.query(Document).filter(Document.kb_id == kb_id)
        if status is not None:
            query = query.filter(Document.status == status)
        if source_type is not None:
            query = query.filter(Document.source_type == source_type)
        if lifecycle_status is not None:
            query = query.filter(
                Document.lifecycle_status == lifecycle_status
            )
        return query

    def get_active_by_hash_in_kb(
        self,
        kb_id: int,
        content_hash: str,
    ) -> Document | None:
        """根据知识库 ID 和 active_content_hash 查询占用去重槽位的活跃文档。

        Args:
            kb_id: 知识库 ID。
            content_hash: 文件 SHA-256 哈希。

        Returns:
            冲突的文档实体或 None。
        """
        return (
            self.db.query(Document)
            .filter(
                Document.kb_id == kb_id,
                Document.active_content_hash == content_hash,
            )
            .first()
        )

    def get_by_id_for_update(self, document_id: int) -> Document | None:
        """根据主键 ID 加悲观行锁（FOR UPDATE）查询文档。

        Args:
            document_id: 目标文档 ID。

        Returns:
            锁定后的文档实体或 None。
        """
        return (
            self.db.query(Document)
            .filter(Document.id == document_id)
            .with_for_update()
            .first()
        )

    def get_by_ids_for_update(
        self,
        document_ids: Iterable[int],
    ) -> list[Document]:
        """按主键升序加悲观行锁（FOR UPDATE）锁定多份文档（避免替换操作死锁）。

        Args:
            document_ids: 待锁定的文档 ID 迭代器。

        Returns:
            锁定后的文档实体列表。
        """
        ordered_ids = sorted(set(document_ids))
        if not ordered_ids:
            return []

        return (
            self.db.query(Document)
            .filter(Document.id.in_(ordered_ids))
            .order_by(Document.id)
            .with_for_update()
            .all()
        )

    def update_status(
        self,
        document: Document,
        status: str,
    ) -> Document:
        """在当前事务中更新文档的流水线技术处理状态并 flush。

        Args:
            document: 文档实体。
            status: 目标状态字符串。

        Returns:
            更新后的文档实体。
        """
        document.status = status
        self.db.flush()
        return document

    def update_cleaned_uri(
        self,
        document: Document,
        cleaned_uri: str,
        status: str,
    ) -> Document:
        """在当前事务中回写清洗产物路径及状态并 flush。

        Args:
            document: 文档实体。
            cleaned_uri: 标准化清洗文本物理存储路径 URI。
            status: 目标状态字符串（通常为 'processed'）。

        Returns:
            更新后的文档实体。
        """
        document.cleaned_uri = cleaned_uri
        document.status = status
        self.db.flush()
        return document

    def deactivate(
        self,
        document: Document,
        lifecycle_status: str,
        *,
        replaced_by: int | None = None,
    ) -> Document:
        """将文档标记为业务失效：更新 lifecycle_status、清空 active_content_hash 并将 storage_status 置为 archiving。

        Args:
            document: 目标文档实体。
            lifecycle_status: 失效原因枚举值（expired / replaced / deleted）。
            replaced_by: 若原因为 replaced，传入替代文档的 ID。

        Returns:
            更新后的文档实体。
        """
        document.lifecycle_status = lifecycle_status
        document.active_content_hash = None
        document.storage_status = DocumentStorageStatus.ARCHIVING.value

        if lifecycle_status == DocumentLifecycleStatus.REPLACED.value:
            document.replaced_by = replaced_by

        self.db.flush()
        return document
