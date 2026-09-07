"""文档模块纯文本文件（TXT）标准化清洗处理器。

处理策略：
1. 移除 NUL (\x00) 字符，统一换行符为 \\n。
2. 裁剪每行行尾多余空白（rstrip），但严格保留行首缩进格式。
3. 折叠连续多余空行，去除文件首尾空行。
4. 统计自然段落数量（按 \\n\\n 划分）与空行数。
"""

from pathlib import Path

from app.modules.document.infrastructure.parsing.base import (
    BaseProcessor,
    ProcessResult,
)


class TxtProcessor(BaseProcessor):
    """纯文本文件清洗与标准化处理器实现。"""

    source_type = "txt"

    def process(
        self,
        source_path: Path,
        cleaned_path: Path,
    ) -> ProcessResult:
        """严格读取 UTF-8/UTF-8-SIG 文本，执行清洗并写入 cleaned 目标文件。

        Args:
            source_path: 原始 TXT 文件路径。
            cleaned_path: 清洗后输出的标准 TXT 路径。

        Returns:
            包含字符数、行数、段落数及空行数的处理结果对象。
        """
        source_path = self.validate_source_path(source_path)
        cleaned_path = self.prepare_cleaned_path(cleaned_path)

        text = source_path.read_text(
            encoding="utf-8-sig",
            errors="strict",
        )
        cleaned_text, metadata = self._clean_text(text)

        cleaned_path.write_text(cleaned_text, encoding="utf-8")

        return ProcessResult(
            source_path=source_path,
            cleaned_path=cleaned_path,
            source_type=self.source_type,
            char_count=len(cleaned_text),
            line_count=len(cleaned_text.splitlines()),
            metadata=metadata,
        )

    def _clean_text(self, text: str) -> tuple[str, dict]:
        """规范换行和空行，移除 NUL 字符与行尾空白，保留行首缩进。"""
        text = (
            text.replace("\x00", "")
            .replace("\r\n", "\n")
            .replace("\r", "\n")
        )

        cleaned_lines: list[str] = []
        blank_line_count = 0

        for raw_line in text.split("\n"):
            line = raw_line.rstrip()

            # 折叠连续空行
            if not line.strip():
                blank_line_count += 1

                if cleaned_lines and cleaned_lines[-1] != "":
                    cleaned_lines.append("")

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

        paragraph_count = len(
            [
                paragraph
                for paragraph in cleaned_text.strip().split("\n\n")
                if paragraph.strip()
            ]
        )

        return cleaned_text, {
            "encoding": "utf-8",
            "paragraph_count": paragraph_count,
            "blank_line_count": blank_line_count,
            "cleaning_strategy": (
                "normalize_newlines_remove_nul_"
                "rstrip_lines_collapse_blank_lines"
            ),
        }
