"""上传参数与 Markdown 围栏的回归测试。"""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock

import httpx
from fastapi import FastAPI

from app.modules.document.presentation.router import router
from app.modules.document.presentation.dependencies import get_upload_document_use_case
from app.modules.document.infrastructure.parsing.markdown import MdProcessor
from app.modules.document.infrastructure.chunking.markdown import MarkdownChunker
from app.modules.document.domain.models import ChunkBuildInput


class UploadValidationTest(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_metadata_is_422_before_any_business_write(self):
        app = FastAPI()
        app.include_router(router)
        use_case = AsyncMock()
        app.dependency_overrides[get_upload_document_use_case] = lambda: use_case
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            for override in ({"kb_id": "0"}, {"title": "x" * 256}, {"domain_code": "x" * 256}):
                response = await client.post("/admin/documents/upload",
                    data={"title": "test", "kb_id": "1", "domain_code": "demo", **override},
                    files={"file": ("demo.md", b"# Demo", "text/markdown")})
                self.assertEqual(response.status_code, 422, response.text)
        use_case.execute.assert_not_called()


class MarkdownFenceTest(unittest.TestCase):
    def test_fences_preserve_code_and_share_section_rules(self):
        for opening, closing in (("```python", "```"), ("~~~~", "~~~~"), ("````", "````")):
            with self.subTest(opening=opening), tempfile.TemporaryDirectory() as directory:
                source, cleaned = Path(directory) / "source.md", Path(directory) / "cleaned.md"
                code = "  # code comment  \n\n\nprint(1)\n"
                source.write_text(f"# Guide\n{opening}\n{code}{closing}\n## Next\nContent\n", encoding="utf-8")
                result = MdProcessor().process(source, cleaned)
                self.assertEqual(result.metadata["heading_count"], 2)
                self.assertIn(code, cleaned.read_text(encoding="utf-8"))
                chunker = MarkdownChunker()
                paths = []
                for metadata in (result.metadata, {}):
                    chunks = chunker.build(ChunkBuildInput(cleaned, "Demo", None, metadata))
                    paths.append([p.section_path for p in chunks.parents])
                self.assertEqual(paths[0], [["Guide"], ["Guide", "Next"]])
                self.assertEqual(paths[0], paths[1])

    def test_unclosed_or_shorter_fence_does_not_create_heading(self):
        processor = MdProcessor()
        for text in ("# Guide\n~~~\n# code\n", "# Guide\n````\n```\n# code\n"):
            self.assertEqual(len(processor._extract_sections(text)), 1)
