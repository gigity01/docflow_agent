"""文档模块 Docling 远程转换服务的 HTTP 客户端实现。

负责将复杂的办公格式（PDF, DOC, DOCX, PPT, PPTX）通过 HTTP POST multipart/form-data
调用外部 Docling 服务转换为标准化 Markdown 文本，并返回 MarkdownConvertResult 领域对象。
"""

from pathlib import Path
from typing import Any

import requests

from app.config.settings import (
    DOCLING_CONVERT_ENDPOINT,
    DOCLING_OUTPUT_TYPE,
    DOCLING_TIMEOUT_SECONDS,
)
from app.modules.document.domain.models import MarkdownConvertResult


class DoclingClient:
    """封装 Docling 文件转 Markdown 远程调用、结果校验与异常处理的客户端。"""

    provider = "docling"

    def __init__(
        self,
        convert_endpoint: str = DOCLING_CONVERT_ENDPOINT,
        timeout_seconds: int = DOCLING_TIMEOUT_SECONDS,
    ) -> None:
        """初始化 Docling 客户端。

        Args:
            convert_endpoint: Docling 转换接口地址。
            timeout_seconds: HTTP 请求超时秒数。
        """
        self.convert_endpoint = convert_endpoint.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def convert_to_markdown(
        self,
        source_path: Path,
        source_type: str,
        *,
        do_ocr: bool = False,
        table_mode: str = "fast",
    ) -> MarkdownConvertResult:
        """上传本地文件至 Docling 远程服务并转换为 Markdown 文本。

        Args:
            source_path: 本地待转换源文件路径。
            source_type: 源文件类型（如 'pdf', 'docx'）。
            do_ocr: 是否启用 OCR 识别（默认 False）。
            table_mode: 表格解析模式（'fast' 或 'accurate'，默认 'fast'）。

        Returns:
            转换成功的领域结果对象。

        Raises:
            FileNotFoundError: 源文件不存在。
            RuntimeError: 服务调用超时、网络错误或转换结果为空/失败。
        """
        self._validate_source_path(source_path)

        normalized_source_type = self._normalize_format(source_type)

        payload = self._request_convert(
            source_path=source_path,
            source_format=normalized_source_type,
            do_ocr=do_ocr,
            table_mode=table_mode,
        )

        status = payload.get("status")
        if status not in {"success", "partial_success"}:
            errors = payload.get("errors") or payload.get("warnings")
            raise RuntimeError(
                f"Docling convert failed. status={status}, errors={errors}"
            )

        markdown = payload.get("document", {}).get("md_content")
        if not markdown or not markdown.strip():
            warnings = payload.get("warnings")
            raise RuntimeError(
                f"Docling markdown empty. status={status}, warnings={warnings}"
            )

        return MarkdownConvertResult(
            source_path=source_path,
            source_format=normalized_source_type,
            markdown=markdown.strip() + "\n",
            provider=self.provider,
            status=status,
            metadata={
                "warnings": payload.get("warnings"),
                "errors": payload.get("errors"),
                "output_type": DOCLING_OUTPUT_TYPE,
                "do_ocr": do_ocr,
                "table_mode": table_mode,
            },
        )

    def _request_convert(
        self,
        *,
        source_path: Path,
        source_format: str,
        do_ocr: bool = False,
        table_mode: str = "fast",
    ) -> dict[str, Any]:
        """发送 multipart/form-data POST 转换请求并解析 JSON 响应。"""
        url = self.convert_endpoint

        try:
            with source_path.open("rb") as file:
                response = requests.post(
                    url=url,
                    files={
                        "files": (
                            source_path.name,
                            file,
                            "application/octet-stream",
                        )
                    },
                    data={
                        "from_formats": source_format,
                        "to_formats": DOCLING_OUTPUT_TYPE,
                        "do_ocr": str(do_ocr).lower(),
                        "image_export_mode": "placeholder",
                        "table_mode": table_mode,
                    },
                    timeout=self.timeout_seconds,
                )

            response.raise_for_status()
            return response.json()

        except requests.Timeout as exc:
            raise RuntimeError(
                f"Docling request timeout. url={url}, source_format={source_format}"
            ) from exc

        except requests.RequestException as exc:
            raise RuntimeError(
                f"Docling request failed. url={url}, source_format={source_format}"
            ) from exc

        except ValueError as exc:
            raise RuntimeError(
                f"Docling response is not valid JSON. url={url}, source_format={source_format}"
            ) from exc

    def _validate_source_path(self, source_path: Path) -> None:
        """校验待转换的源文件路径存在且为普通文件。"""
        if not source_path.exists():
            raise FileNotFoundError(f"源文件不存在: {source_path}")

        if not source_path.is_file():
            raise ValueError(f"源路径不是有效文件: {source_path}")

    def _normalize_format(self, source_type: str) -> str:
        """归一化源文件格式（小写并去除前导点号）。"""
        return source_type.lower().lstrip(".")
