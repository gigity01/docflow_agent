"""通过真实 HTTP API 演示文档流水线；Agent 模式需要真实模型和独立 Worker。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
from uuid import uuid4

import httpx


def request(client: httpx.Client, method: str, path: str, **kwargs) -> dict:
    response = client.request(method, path, **kwargs)
    response.raise_for_status()
    result = response.json()
    print(json.dumps({"path": path, "status_code": response.status_code, "result": result},
                     ensure_ascii=False, indent=2))
    return result


def wait_for_turn(client: httpx.Client, conversation_id: str, turn_id: str,
                  timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    last_status = None
    while time.monotonic() < deadline:
        response = client.get(f"/api/conversations/{conversation_id}/turns/{turn_id}")
        response.raise_for_status()
        result = response.json()
        status = result["turn_status"]
        if status != last_status:
            print(json.dumps(result, ensure_ascii=False, indent=2))
            last_status = status
        if status == "completed":
            return result
        if status in {"failed", "needs_clarification"}:
            raise RuntimeError(f"任务未完成：{status}；请按输出的 turn_id 查询或回复澄清")
        time.sleep(min(2, max(0, deadline - time.monotonic())))
    raise TimeoutError("等待任务超时；任务可能仍在运行，请检查 Worker 与 Turn 状态")


def run_demo(args: argparse.Namespace) -> None:
    if args.timeout <= 0:
        raise ValueError("timeout 必须大于 0")
    if args.mode == "agent" and args.skip_index:
        raise ValueError("--skip-index 仅适用于 direct 模式")
    sample = args.file.read_text(encoding="utf-8")
    run_id = uuid4().hex
    # 每次演示生成不同内容，避免重复上传触发真实的内容唯一约束。
    payload = (sample + f"\n\n演示批次：{run_id}\n").encode("utf-8")
    with httpx.Client(base_url=args.base_url.rstrip("/"), timeout=args.timeout) as client:
        document = request(client, "POST", "/api/admin/documents/upload",
                           data={"title": "DocFlow 入库演示", "kb_id": str(args.kb_id),
                                 "domain_code": "demo"},
                           files={"file": ("onboarding.md", payload, "text/markdown")})
        document_id = document["id"]
        if args.mode == "agent":
            conversation_id = f"demo-{run_id}"
            result = request(client, "POST", f"/api/conversations/{conversation_id}/messages",
                             json={"message": f"请处理文档 document_id={document_id}，"
                                   f"知识库 kb_id={args.kb_id}，完成清洗、切块并建立向量索引。"})
            if result["status"] not in {"processing", "retry_pending", "completed"}:
                raise RuntimeError("规划未进入执行，请检查上方助手回复")
            wait_for_turn(client, conversation_id, result["turn_id"], args.timeout)
        else:
            for operation in ("process", "build-chunks"):
                request(client, "POST", f"/api/admin/documents/{document_id}/{operation}")
            if not args.skip_index:
                result = request(client, "POST", f"/api/admin/documents/{document_id}/index-vectors")
                if result["failed_chunks"] or result["indexed_chunks"] <= 0:
                    raise RuntimeError("向量索引未全部成功")
        state = request(client, "GET", f"/api/admin/documents/{document_id}/pipeline-state")
        statistics = request(client, "GET", f"/api/admin/documents/{document_id}/chunk-statistics")
        if state["parent_count"] <= 0 or state["child_count"] <= 0:
            raise RuntimeError("未生成有效切块")
        if not args.skip_index and (
            state["document_status"] != "indexed"
            or statistics["chunks_with_vector_id"] != statistics["child_count"]
        ):
            raise RuntimeError("持久化状态尚未达到完整索引结果")
    print("DEMO_OK" + (" (仅上传、清洗、切块；未验证模型与向量索引)" if args.skip_index else ""))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--kb-id", type=int, default=1)
    parser.add_argument("--file", type=Path,
                        default=Path(__file__).resolve().parents[1] / "examples/onboarding.md")
    parser.add_argument("--mode", choices=("agent", "direct"), default="agent")
    parser.add_argument("--skip-index", action="store_true")
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args()
    try:
        run_demo(args)
    except (httpx.HTTPError, RuntimeError, ValueError, OSError) as exc:
        parser.exit(1, f"DEMO_FAILED: {exc}\n")


if __name__ == "__main__":
    main()
