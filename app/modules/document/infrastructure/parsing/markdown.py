"""文档模块 Markdown 文本规范化与标题提取处理器。

清洗与提取逻辑：
1. 规范换行符为 \\n，剔除 NUL 字符。
2. 折叠连续多余空行，去除每行行尾多余空白。
3. 规范化 ATX 标题格式（如 '## 标题'）。
4. 递归解析 Markdown 标题树，提取 1-based 行范围及完整标题面包屑路径（section_path）。
"""

import re
from pathlib import Path
from typing import Any

from app.modules.document.infrastructure.parsing.base import (
    BaseProcessor,
    ProcessResult,
)


class MdProcessor(BaseProcessor):
    """Markdown 文本标准化与 ATX 标题层级提取处理器。"""

    source_type = "md"

    # 匹配 1 到 6 级 ATX 标题正则
    HEADING_PATTERN = re.compile(
        r"^[ \t]{0,3}(#{1,6})[ \t]+(.+?)\s*$"
    )

    def process(
        self,
        source_path: Path,
        cleaned_path: Path,
    ) -> ProcessResult:
        """严格读取 UTF-8/UTF-8-SIG Markdown，规范文本、提取标题并写入 cleaned 文件。

        Args:
            source_path: 输入 Markdown 文件路径。
            cleaned_path: 清洗后输出的标准 Markdown 路径。

        Returns:
            处理结果对象（包含章节标题元数据 sections、heading_count 等）。
        """
        source_path = self.validate_source_path(source_path)
        cleaned_path = self.prepare_cleaned_path(cleaned_path)

        text = source_path.read_text(
            encoding="utf-8-sig",
            errors="strict",
        )

        cleaned_text = self._normalize_text(text)
        sections = self._extract_sections(cleaned_text)

        cleaned_path.write_text(cleaned_text, encoding="utf-8")

        return ProcessResult(
            source_path=source_path,
            cleaned_path=cleaned_path,
            source_type=self.source_type,
            char_count=len(cleaned_text),
            line_count=len(cleaned_text.splitlines()),
            metadata={
                "encoding": "utf-8",
                "heading_count": sum(
                    1
                    for section in sections
                    if section["heading_line"] is not None
                ),
                "section_count": len(sections),
                "sections": sections,
                "cleaning_strategy": (
                    "normalize_markdown_text_extract_heading_paths"
                ),
            },
        )

    def _normalize_text(self, text: str) -> str:
        """剔除 NUL 字符、统一 \\n 换行、折叠连续空行并标准化 ATX 标题行空格。"""
        text = (
            text.replace("\x00", "")
            .replace("\r\n", "\n")
            .replace("\r", "\n")
        )

        cleaned_lines: list[str] = []

        for raw_line in text.split("\n"):
            line = raw_line.rstrip()

            # 折叠连续空行
            if not line.strip():
                if cleaned_lines and cleaned_lines[-1] != "":
                    cleaned_lines.append("")
                continue

            heading_match = self.HEADING_PATTERN.match(line)
            if heading_match:
                marks = heading_match.group(1)
                title = heading_match.group(2).strip()
                cleaned_lines.append(f"{marks} {title}")
                continue

            cleaned_lines.append(line)

        # 移除文件开头与结尾的空行
        while cleaned_lines and cleaned_lines[0] == "":
            cleaned_lines.pop(0)

        while cleaned_lines and cleaned_lines[-1] == "":
            cleaned_lines.pop()

        cleaned_text = "\n".join(cleaned_lines)

        if cleaned_text:
            cleaned_text += "\n"

        return cleaned_text

    def _extract_sections(self, text: str) -> list[dict[str, Any]]:
        """按 ATX 标题解析章节树，并记录每个 section 的 1-based 起止行号与标题路径。"""
        lines = text.splitlines()

        if not lines:
            return []

        sections: list[dict[str, Any]] = []
        heading_stack: list[tuple[int, str]] = []
        current_section: dict[str, Any] | None = None

        for line_number, line in enumerate(lines, start=1):
            match = self.HEADING_PATTERN.match(line)

            if match:
                if current_section is not None:
                    current_section["end_line"] = line_number - 1
                    sections.append(current_section)

                level = len(match.group(1))
                title = match.group(2).strip()

                # 维护祖先栈
                while heading_stack and heading_stack[-1][0] >= level:
                    heading_stack.pop()

                heading_stack.append((level, title))

                current_section = {
                    "level": level,
                    "title": title,
                    "section_path": [
                        item_title for _, item_title in heading_stack
                    ],
                    "heading_line": line_number,
                    "start_line": line_number,
                    "end_line": line_number,
                }
                continue

            # 处理文档最开头的无标题前言
            if current_section is None and line.strip():
                current_section = {
                    "level": None,
                    "title": None,
                    "section_path": [],
                    "heading_line": None,
                    "start_line": line_number,
                    "end_line": line_number,
                }

        if current_section is not None:
            current_section["end_line"] = len(lines)
            sections.append(current_section)

        return sections
