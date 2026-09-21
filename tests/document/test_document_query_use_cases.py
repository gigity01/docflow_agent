"""Document 领域 10 个独立只读查询用例（Query Use Cases）单元测试。

核心业务不变量：
1. 用例职责单一化与 DTO 映射：
   - 每个查询用例（如 GetDocumentUseCase, ListDocumentsUseCase, SearchDocumentsUseCase, GetDocumentPipelineStateUseCase 等）独立封装特定查询边界，将仓储模型安全转化为只读 DTO。
2. 异常隔离：
   - 资源不存在时安全抛出 DocumentApplicationError(404)，参数非法时返回 DocumentApplicationError(400)。
"""

from __future__ import annotations

import unittest
from datetime import datetime
from types import SimpleNamespace

from app.modules.document.application.dto import (
    ChildChunkSearchQuery,
    DocumentArtifactSearchQuery,
    DocumentListQuery,
    DocumentSearchQuery,
    ParentBlockSearchQuery,
)
from app.modules.document.application.errors import DocumentApplicationError
from app.modules.document.application.use_cases.get_document import (
    GetDocumentUseCase,
)
from app.modules.document.application.use_cases.get_document_overview import (
    GetDocumentOverviewUseCase,
)
from app.modules.document.application.use_cases.get_chunk_statistics import (
    GetDocumentChunkStatisticsUseCase,
)
from app.modules.document.application.use_cases.get_knowledge_base_statistics import (
    GetKnowledgeBaseStatisticsUseCase,
)
from app.modules.document.application.use_cases.get_pipeline_state import (
    GetDocumentPipelineStateUseCase,
)
from app.modules.document.application.use_cases.list_artifacts import (
    ListDocumentArtifactsUseCase,
)
from app.modules.document.application.use_cases.list_child_chunks import (
    ListChildChunksUseCase,
)
from app.modules.document.application.use_cases.list_documents import (
    ListDocumentsUseCase,
)
from app.modules.document.application.use_cases.list_parent_blocks import (
    ListParentBlocksUseCase,
)
from app.modules.document.application.use_cases.search_artifacts import (
    SearchDocumentArtifactsUseCase,
)
from app.modules.document.application.use_cases.search_documents import (
    SearchDocumentsUseCase,
)


NOW = datetime(2026, 8, 2, 12, 0, 0)


def _document(document_id: int = 7):
    return SimpleNamespace(
        id=document_id,
        doc_code=f"DOC_{document_id}",
        kb_id=3,
        domain_code="policy",
        business_scene=None,
        title="测试文档",
        original_filename="test.txt",
        file_size=128,
        source_type="txt",
        source_uri=f"storage/raw/local/DOC_{document_id}.txt",
        cleaned_uri=f"storage/cleaned/DOC_{document_id}.cleaned.txt",
        content_hash="a" * 64,
        active_content_hash="a" * 64,
        lifecycle_status="active",
        storage_status="active",
        version=1,
        status="chunked",
        replaced_by=None,
        risk_level="low",
        effective_at=None,
        expired_at=None,
        created_by_actor_code="actor",
        created_at=NOW,
        updated_at=NOW,
        indexed_at=None,
    )


def _artifact(document_id: int = 7):
    return SimpleNamespace(
        id=11,
        document_id=document_id,
        artifact_code="ART_11",
        artifact_type="cleaned_text",
        artifact_role="process_output",
        artifact_format="txt",
        artifact_uri="storage/cleaned/DOC_7.cleaned.txt",
        artifact_hash="b" * 64,
        hash_algorithm="sha256",
        provider=None,
        processor="TextProcessor",
        file_size=100,
        char_count=90,
        line_count=5,
        status="active",
        metadata_json={"warnings": []},
        created_at=NOW,
        updated_at=NOW,
    )


def _parent_block():
    return SimpleNamespace(
        id=21,
        parent_code="PB_21",
        kb_id=3,
        doc_id=7,
        domain_code="policy",
        business_scene=None,
        block_type="section",
        title="第一章",
        section_path=["第一章"],
        content="父块正文",
        content_hash="c" * 32,
        block_index=0,
        semantic_group_index=0,
        segment_index=0,
        status="active",
        version=1,
        created_at=NOW,
        updated_at=NOW,
    )


def _child_chunk():
    return SimpleNamespace(
        id=31,
        chunk_code="CC_31",
        parent_id=21,
        doc_id=7,
        kb_id=3,
        domain_code="policy",
        business_scene=None,
        chunk_index=0,
        chunk_type="text",
        section_path=["第一章"],
        source_row_index=None,
        content="子块正文",
        embedding_text="第一章\n子块正文",
        token_count=10,
        vector_status="indexed",
        qdrant_point_id="31",
        status="active",
        version=1,
        created_at=NOW,
        updated_at=NOW,
        indexed_at=NOW,
    )


class _Documents:
    def __init__(self, documents):
        self.documents = documents
        self.list_kwargs = None
        self.count_kwargs = None
        self.search_query = None
        self.count_search_query = None
        self.kb_count_calls = []

    def get_by_id(self, document_id):
        return next(
            (item for item in self.documents if item.id == document_id),
            None,
        )

    def list_filtered(self, **kwargs):
        self.list_kwargs = kwargs
        return self.documents

    def count_filtered(self, **kwargs):
        self.count_kwargs = kwargs
        return len(self.documents)

    def search(self, query):
        self.search_query = query
        return self.documents

    def count_search(self, query):
        self.count_search_query = query
        return len(self.documents)

    def count_for_kb(self, kb_id, **kwargs):
        self.kb_count_calls.append((kb_id, kwargs))
        if kwargs.get("status") == "failed":
            return 1
        if kwargs.get("status") == "indexed":
            return 2
        if kwargs.get("lifecycle_status") == "active":
            return 4
        return 5


class _Artifacts:
    def __init__(self, items):
        self.items = items
        self.search_query = None

    def list_by_document_id(self, document_id):
        return [item for item in self.items if item.document_id == document_id]

    def search(self, query):
        self.search_query = query
        return self.items

    def count_search(self, query):
        return len(self.items)


class _ParentBlocks:
    def __init__(self, items=None):
        self.items = items or []
        self.search_query = None

    def count_active_by_doc_id(self, document_id):
        return 2

    def search(self, query):
        self.search_query = query
        return self.items

    def count_search(self, query):
        return len(self.items)

    def count_by_status_for_document(self, document_id):
        return {"active": 2, "superseded": 1}

    def count_active_for_kb(self, kb_id):
        return 8


class _ChildChunks:
    def __init__(self, items=None):
        self.items = items or []
        self.search_query = None

    def count_by_vector_status_for_document(self, document_id):
        return {"pending": 1, "indexed": 3}

    def search(self, query):
        self.search_query = query
        return self.items

    def count_search(self, query):
        return len(self.items)

    def count_by_status_for_document(self, document_id):
        return {"active": 4, "inactive": 1}

    def count_all_by_vector_status_for_document(self, document_id):
        return {"pending": 1, "indexed": 3, "failed": 1}

    def count_by_chunk_type_for_document(self, document_id):
        return {"text": 4, "csv_row": 1}

    def count_vector_id_presence_for_document(self, document_id):
        return 3, 2

    def count_active_for_kb(self, kb_id):
        return 12

    def count_by_vector_status_for_kb(self, kb_id):
        return {"pending": 2, "failed": 1, "indexed": 9}


class _KnowledgeBases:
    def __init__(self, item):
        self.item = item

    def get_by_id(self, kb_id):
        if self.item is not None and self.item.id == kb_id:
            return self.item
        return None


class _Uow:
    def __init__(
        self,
        documents,
        artifacts=None,
        parent_blocks=None,
        child_chunks=None,
        knowledge_base=None,
    ):
        self.documents = documents
        self.document_artifacts = _Artifacts(artifacts or [])
        self.parent_blocks = _ParentBlocks(parent_blocks)
        self.child_chunks = _ChildChunks(child_chunks)
        self.knowledge_bases = _KnowledgeBases(knowledge_base)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class DocumentQueryUseCasesTest(unittest.TestCase):
    def test_get_document_returns_complete_application_dto(self) -> None:
        documents = _Documents([_document()])
        use_case = GetDocumentUseCase(
            uow_factory=lambda: _Uow(documents)
        )

        result = use_case.execute(7)

        self.assertEqual(result.id, 7)
        self.assertEqual(result.status, "chunked")
        self.assertEqual(result.cleaned_uri, "storage/cleaned/DOC_7.cleaned.txt")

    def test_get_document_rejects_missing_document(self) -> None:
        use_case = GetDocumentUseCase(
            uow_factory=lambda: _Uow(_Documents([]))
        )

        with self.assertRaises(DocumentApplicationError) as raised:
            use_case.execute(404)

        self.assertEqual(raised.exception.status_code, 404)

    def test_list_documents_maps_filters_and_pagination(self) -> None:
        documents = _Documents([_document(9)])
        use_case = ListDocumentsUseCase(
            uow_factory=lambda: _Uow(documents)
        )
        query = DocumentListQuery(
            kb_id=3,
            status="chunked",
            source_type="txt",
            lifecycle_status="active",
            limit=20,
            offset=5,
        )

        result = use_case.execute(query)

        expected = {
            "kb_id": 3,
            "status": "chunked",
            "source_type": "txt",
            "lifecycle_status": "active",
        }
        self.assertEqual(
            documents.list_kwargs,
            {**expected, "limit": 20, "offset": 5},
        )
        self.assertEqual(documents.count_kwargs, expected)
        self.assertEqual(result.total, 1)
        self.assertEqual(result.items[0].id, 9)

    def test_search_documents_passes_full_query_to_repository(self) -> None:
        documents = _Documents([_document(9)])
        use_case = SearchDocumentsUseCase(
            uow_factory=lambda: _Uow(documents)
        )
        query = DocumentSearchQuery(
            kb_ids=[3],
            statuses=["chunked"],
            keyword="测试",
            created_from=NOW,
            sort_by="updated_at",
            sort_order="asc",
            limit=20,
            offset=5,
        )

        result = use_case.execute(query)

        self.assertIs(documents.search_query, query)
        self.assertIs(documents.count_search_query, query)
        self.assertEqual(result.total, 1)
        self.assertEqual(result.limit, 20)
        self.assertEqual(result.items[0].id, 9)

    def test_pipeline_state_aggregates_parent_and_vector_counts(self) -> None:
        documents = _Documents([_document()])
        use_case = GetDocumentPipelineStateUseCase(
            uow_factory=lambda: _Uow(documents)
        )

        result = use_case.execute(7)

        self.assertEqual(result.parent_count, 2)
        self.assertEqual(result.child_count, 4)
        self.assertEqual(
            result.vector_status_counts,
            {"pending": 1, "indexed": 3},
        )

    def test_list_artifacts_maps_metadata_without_exposing_orm(self) -> None:
        documents = _Documents([_document()])
        use_case = ListDocumentArtifactsUseCase(
            uow_factory=lambda: _Uow(documents, [_artifact()])
        )

        result = use_case.execute(7)

        self.assertEqual(result.document_id, 7)
        self.assertEqual(result.source_uri, "storage/raw/local/DOC_7.txt")
        self.assertEqual(result.items[0].artifact_code, "ART_11")
        self.assertEqual(result.items[0].metadata, {"warnings": []})

    def test_search_artifacts_returns_stable_page(self) -> None:
        uow = _Uow(_Documents([]), [_artifact()])
        use_case = SearchDocumentArtifactsUseCase(
            uow_factory=lambda: uow
        )
        query = DocumentArtifactSearchQuery(
            document_ids=[7],
            statuses=["active"],
            active_only=True,
            limit=10,
        )

        result = use_case.execute(query)

        self.assertIs(uow.document_artifacts.search_query, query)
        self.assertEqual(result.total, 1)
        self.assertEqual(result.items[0].metadata, {"warnings": []})

    def test_list_parent_blocks_returns_content_and_order_metadata(self) -> None:
        uow = _Uow(_Documents([]), parent_blocks=[_parent_block()])
        query = ParentBlockSearchQuery(
            document_ids=[7],
            section_path_contains="第一章",
        )
        result = ListParentBlocksUseCase(
            uow_factory=lambda: uow
        ).execute(query)

        self.assertIs(uow.parent_blocks.search_query, query)
        self.assertEqual(result.items[0].content, "父块正文")
        self.assertEqual(result.items[0].block_index, 0)

    def test_list_child_chunks_returns_vector_and_source_metadata(self) -> None:
        uow = _Uow(_Documents([]), child_chunks=[_child_chunk()])
        query = ChildChunkSearchQuery(
            document_id=7,
            vector_statuses=["indexed"],
            has_vector_id=True,
        )
        result = ListChildChunksUseCase(
            uow_factory=lambda: uow
        ).execute(query)

        self.assertIs(uow.child_chunks.search_query, query)
        self.assertEqual(result.items[0].qdrant_point_id, "31")
        self.assertEqual(result.items[0].section_path, ["第一章"])

    def test_document_chunk_statistics_combines_all_dimensions(self) -> None:
        uow = _Uow(_Documents([_document()]))
        result = GetDocumentChunkStatisticsUseCase(
            uow_factory=lambda: uow
        ).execute(7)

        self.assertEqual(result.parent_count, 3)
        self.assertEqual(result.child_count, 5)
        self.assertEqual(result.vector_status_counts["failed"], 1)
        self.assertEqual(result.chunks_with_vector_id, 3)
        self.assertEqual(result.chunks_without_vector_id, 2)

    def test_knowledge_base_statistics_combines_document_and_chunk_counts(
        self,
    ) -> None:
        knowledge_base = SimpleNamespace(
            id=3,
            kb_code="KB_3",
            name="政策知识库",
            domain_code="policy",
            business_scene=None,
            status="active",
            visibility="external",
        )
        documents = _Documents([])
        uow = _Uow(
            documents,
            knowledge_base=knowledge_base,
        )

        result = GetKnowledgeBaseStatisticsUseCase(
            uow_factory=lambda: uow
        ).execute(3)

        self.assertEqual(result.document_count, 5)
        self.assertEqual(result.active_document_count, 4)
        self.assertEqual(result.failed_document_count, 1)
        self.assertEqual(result.indexed_document_count, 2)
        self.assertEqual(result.parent_count, 8)
        self.assertEqual(result.child_count, 12)
        self.assertEqual(result.vector_status_counts["indexed"], 9)

    def test_document_overview_raises_404_when_document_not_found(self) -> None:
        documents = _Documents([])
        use_case = GetDocumentOverviewUseCase(uow_factory=lambda: _Uow(documents))
        with self.assertRaises(DocumentApplicationError) as ctx:
            use_case.execute(999999)
        self.assertEqual(ctx.exception.status_code, 404)
        self.assertEqual(ctx.exception.detail, "文档不存在")

    def test_document_overview_empty_initial_state(self) -> None:
        doc = _document(7)
        doc.status = "uploaded"

        class _EmptyPB:
            def count_active_by_doc_id(self, doc_id):
                return 0

            def count_by_status_for_document(self, doc_id):
                return {}

        class _EmptyCC:
            def count_by_vector_status_for_document(self, doc_id):
                return {}

            def count_by_status_for_document(self, doc_id):
                return {}

            def count_all_by_vector_status_for_document(self, doc_id):
                return {}

            def count_vector_id_presence_for_document(self, doc_id):
                return 0, 0

            def count_by_chunk_type_for_document(self, doc_id):
                return {}

        uow = _Uow(_Documents([doc]))
        uow.parent_blocks = _EmptyPB()
        uow.child_chunks = _EmptyCC()
        use_case = GetDocumentOverviewUseCase(uow_factory=lambda: uow)
        result = use_case.execute(7)

        self.assertEqual(result.document.status, "uploaded")
        self.assertEqual(result.pipeline_state.document_status, "uploaded")
        self.assertEqual(result.pipeline_state.parent_count, 0)
        self.assertEqual(result.pipeline_state.child_count, 0)
        self.assertEqual(result.pipeline_state.vector_status_counts, {})
        self.assertEqual(result.chunk_statistics.parent_count, 0)
        self.assertEqual(result.chunk_statistics.child_count, 0)
        self.assertEqual(result.chunk_statistics.parent_status_counts, {})
        self.assertEqual(result.chunk_statistics.child_status_counts, {})
        self.assertEqual(result.chunk_statistics.vector_status_counts, {})
        self.assertEqual(result.chunk_statistics.chunk_type_counts, {})
        self.assertEqual(result.chunk_statistics.chunks_with_vector_id, 0)
        self.assertEqual(result.chunk_statistics.chunks_without_vector_id, 0)

    def test_document_overview_in_progress_statuses_converge(self) -> None:
        for status in ("processing", "chunking", "indexing"):
            with self.subTest(status=status):
                doc = _document(7)
                doc.status = status
                uow = _Uow(_Documents([doc]))
                use_case = GetDocumentOverviewUseCase(uow_factory=lambda: uow)
                result = use_case.execute(7)

                self.assertEqual(result.document.status, "in_progress")
                self.assertEqual(result.pipeline_state.document_status, "in_progress")
                self.assertEqual(result.document.lifecycle_status, "active")
                self.assertEqual(result.document.storage_status, "active")
                self.assertEqual(result.pipeline_state.lifecycle_status, "active")
                self.assertEqual(result.pipeline_state.storage_status, "active")

    def test_document_overview_stable_statuses_preserved(self) -> None:
        for status in ("uploaded", "processed", "chunked", "indexed", "failed"):
            with self.subTest(status=status):
                doc = _document(7)
                doc.status = status
                uow = _Uow(_Documents([doc]))
                use_case = GetDocumentOverviewUseCase(uow_factory=lambda: uow)
                result = use_case.execute(7)

                self.assertEqual(result.document.status, status)
                self.assertEqual(result.pipeline_state.document_status, status)

    def test_document_overview_aggregates_all_components_consistently(self) -> None:
        doc = _document(7)
        doc.status = "indexed"
        uow = _Uow(_Documents([doc]))
        use_case = GetDocumentOverviewUseCase(uow_factory=lambda: uow)
        result = use_case.execute(7)

        self.assertEqual(result.document.id, 7)
        self.assertEqual(result.document.doc_code, "DOC_7")
        self.assertEqual(result.pipeline_state.parent_count, 2)
        self.assertEqual(result.pipeline_state.child_count, 4)
        self.assertEqual(
            result.pipeline_state.vector_status_counts,
            {"pending": 1, "indexed": 3},
        )
        self.assertEqual(result.chunk_statistics.parent_count, 3)
        self.assertEqual(result.chunk_statistics.child_count, 5)
        self.assertEqual(result.chunk_statistics.chunks_with_vector_id, 3)
        self.assertEqual(result.chunk_statistics.chunks_without_vector_id, 2)


if __name__ == "__main__":
    unittest.main()
