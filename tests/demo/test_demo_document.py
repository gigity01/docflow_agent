"""演示只在结果完整时成功，并明确区分无模型路径。"""

import argparse
from contextlib import redirect_stdout
import io
from pathlib import Path
import unittest
from unittest.mock import patch

import httpx

from scripts.demo_document import run_demo, wait_for_turn


class DocumentDemoTest(unittest.TestCase):
    def run_case(self, *, mode="direct", skip_index=False, vector_count=2,
                 agent_status="processing"):
        paths = []

        def respond(request):
            path = request.url.path
            paths.append(path)
            if path.endswith("/upload"):
                result = {"id": 1}
            elif path.endswith("/messages"):
                result = {"status": agent_status, "turn_id": "demo-turn"}
            elif path.endswith("/pipeline-state"):
                result = {"parent_count": 1, "child_count": 2,
                          "document_status": "chunked" if skip_index else "indexed"}
            elif path.endswith("/chunk-statistics"):
                result = {"child_count": 2, "chunks_with_vector_id": vector_count}
            elif path.endswith("/index-vectors"):
                result = {"indexed_chunks": 2, "failed_chunks": 0}
            else:
                result = {}
            return httpx.Response(200, json=result)

        args = argparse.Namespace(
            timeout=1, mode=mode, skip_index=skip_index, kb_id=1,
            base_url="http://demo.test",
            file=Path(__file__).resolve().parents[2] / "examples/onboarding.md",
        )
        client = httpx.Client(base_url=args.base_url, transport=httpx.MockTransport(respond))
        output = io.StringIO()
        with patch("scripts.demo_document.httpx.Client", return_value=client), redirect_stdout(output):
            run_demo(args)
        return paths, output.getvalue()

    def test_no_model_demo_does_not_call_indexing_or_agent(self):
        paths, output = self.run_case(skip_index=True, vector_count=0)
        self.assertFalse(any(path.endswith(("/messages", "/index-vectors")) for path in paths))
        self.assertIn("未验证模型与向量索引", output)

    def test_incomplete_vectors_cannot_report_success(self):
        with self.assertRaisesRegex(RuntimeError, "完整索引"):
            self.run_case(vector_count=1)

    def test_clarification_is_not_success(self):
        with self.assertRaisesRegex(RuntimeError, "规划未进入执行"):
            self.run_case(mode="agent", agent_status="needs_clarification")

    def test_polling_timeout_is_not_success(self):
        with patch("scripts.demo_document.time.monotonic", side_effect=[0, 2]):
            with self.assertRaises(TimeoutError):
                wait_for_turn(None, "conversation", "turn", timeout=1)
