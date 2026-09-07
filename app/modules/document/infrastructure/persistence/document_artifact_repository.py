"""文档模块派生产物（DocumentArtifact）ORM 模型的持久化与查询仓储。

管理清洗/转换过程中的派生产物写入、多条件复合检索以及新版本激活时的 superseded 状态更新。
"""

from sqlalchemy.orm import Session

from app.modules.document.infrastructure.persistence.models.document_artifact import (
    DocumentArtifact,
)
from app.modules.document.application.dto import (
    DocumentArtifactCreate,
    DocumentArtifactSearchQuery,
)


class DocumentArtifactRepository:
    """管理文档派生产物记录的数据访问仓储类。"""

    def __init__(self, db: Session) -> None:
        """绑定当前数据库会话。

        Args:
            db: SQLAlchemy 数据库会话。
        """
        self.db = db

    def create(self, data: DocumentArtifactCreate) -> DocumentArtifact:
        """依据创建 DTO 实例化 DocumentArtifact，加入会话并 flush。

        Args:
            data: 包含产物元数据的创建 DTO。

        Returns:
            插入并包含主键的派生产物实体。
        """
        artifact = DocumentArtifact(
            document_id=data.document_id,
            artifact_code=data.artifact_code,
            artifact_type=data.artifact_type,
            artifact_role=data.artifact_role,
            artifact_format=data.artifact_format,
            artifact_uri=data.artifact_uri,
            artifact_hash=data.artifact_hash,
            hash_algorithm=data.hash_algorithm,
            provider=data.provider,
            processor=data.processor,
            file_size=data.file_size,
            char_count=data.char_count,
            line_count=data.line_count,
            status=data.status,
            metadata_json=data.metadata,
            created_by_actor_code=data.created_by_actor_code,
        )

        self.db.add(artifact)
        self.db.flush()

        return artifact

    def get_by_id(self, artifact_id: int) -> DocumentArtifact | None:
        """根据主键 ID 查询单个派生产物实体。

        Args:
            artifact_id: 产物主键 ID。

        Returns:
            找到返回实体，否则返回 None。
        """
        return (
            self.db.query(DocumentArtifact)
            .filter(DocumentArtifact.id == artifact_id)
            .first()
        )

    def get_by_code(self, artifact_code: str) -> DocumentArtifact | None:
        """根据业务编号（artifact_code）查询派生产物实体。

        Args:
            artifact_code: 产物业务编号。

        Returns:
            找到返回实体，否则返回 None。
        """
        return (
            self.db.query(DocumentArtifact)
            .filter(DocumentArtifact.artifact_code == artifact_code)
            .first()
        )

    def list_by_document_id(
        self,
        document_id: int,
    ) -> list[DocumentArtifact]:
        """按创建时间升序返回指定文档的全部派生产物。

        Args:
            document_id: 所属文档 ID。

        Returns:
            派生产物实体列表。
        """
        return (
            self.db.query(DocumentArtifact)
            .filter(DocumentArtifact.document_id == document_id)
            .order_by(
                DocumentArtifact.created_at.asc(),
                DocumentArtifact.id.asc(),
            )
            .all()
        )

    def search(
        self,
        filters: DocumentArtifactSearchQuery,
    ) -> list[DocumentArtifact]:
        """按受限多条件组合筛选派生产物并稳定降序分页。

        Args:
            filters: 产物高级检索参数 DTO。

        Returns:
            匹配的产物实体列表。
        """
        return (
            self._search_query(filters)
            .order_by(
                DocumentArtifact.created_at.desc(),
                DocumentArtifact.id.desc(),
            )
            .offset(filters.offset)
            .limit(filters.limit)
            .all()
        )

    def count_search(self, filters: DocumentArtifactSearchQuery) -> int:
        """统计与派生产物高级检索相同过滤条件下的命中总记录数。

        Args:
            filters: 产物高级检索参数 DTO。

        Returns:
            匹配总记录数。
        """
        return self._search_query(filters).count()

    def _search_query(self, filters: DocumentArtifactSearchQuery):
        """构建产物高级多条件查询 SQLAlchemy Query。"""
        query = self.db.query(DocumentArtifact)
        list_filters = (
            (DocumentArtifact.document_id, filters.document_ids),
            (DocumentArtifact.artifact_type, filters.artifact_types),
            (DocumentArtifact.artifact_role, filters.artifact_roles),
            (DocumentArtifact.artifact_format, filters.artifact_formats),
            (DocumentArtifact.status, filters.statuses),
            (DocumentArtifact.provider, filters.providers),
            (DocumentArtifact.processor, filters.processors),
        )
        for column, values in list_filters:
            if values:
                query = query.filter(column.in_(values))
        if filters.created_from is not None:
            query = query.filter(
                DocumentArtifact.created_at >= filters.created_from
            )
        if filters.created_to is not None:
            query = query.filter(
                DocumentArtifact.created_at <= filters.created_to
            )
        if filters.active_only is True:
            query = query.filter(DocumentArtifact.status == "active")
        elif filters.active_only is False:
            query = query.filter(DocumentArtifact.status != "active")
        return query

    def get_latest_active(
        self,
        *,
        document_id: int,
        artifact_type: str,
        artifact_role: str | None = None,
        artifact_format: str | None = None,
    ) -> DocumentArtifact | None:
        """查询指定文档下匹配类型、角色与格式的最新的 active 派生产物。

        Args:
            document_id: 文档 ID。
            artifact_type: 产物类型。
            artifact_role: 可选产物角色。
            artifact_format: 可选文件格式。

        Returns:
            最新的活跃产物实体。
        """
        query = (
            self.db.query(DocumentArtifact)
            .filter(DocumentArtifact.document_id == document_id)
            .filter(DocumentArtifact.artifact_type == artifact_type)
            .filter(DocumentArtifact.status == "active")
        )

        if artifact_role is not None:
            query = query.filter(DocumentArtifact.artifact_role == artifact_role)

        if artifact_format is not None:
            query = query.filter(DocumentArtifact.artifact_format == artifact_format)

        return query.order_by(DocumentArtifact.created_at.desc()).first()

    def mark_active_as_superseded(
        self,
        *,
        document_id: int,
        artifact_type: str,
        artifact_role: str | None = None,
        artifact_format: str | None = None,
    ) -> int:
        """将同类型、同用途的原活跃产物置为 'superseded' 状态并 flush。

        Args:
            document_id: 文档 ID。
            artifact_type: 产物类型。
            artifact_role: 可选产物角色。
            artifact_format: 可选文件格式。

        Returns:
            被更新为 superseded 的记录数。
        """
        query = (
            self.db.query(DocumentArtifact)
            .filter(DocumentArtifact.document_id == document_id)
            .filter(DocumentArtifact.artifact_type == artifact_type)
            .filter(DocumentArtifact.status == "active")
        )

        if artifact_role is not None:
            query = query.filter(DocumentArtifact.artifact_role == artifact_role)

        if artifact_format is not None:
            query = query.filter(DocumentArtifact.artifact_format == artifact_format)

        count = query.update(
            {
                "status": "superseded",
            },
            synchronize_session=False,
        )

        self.db.flush()

        return count
