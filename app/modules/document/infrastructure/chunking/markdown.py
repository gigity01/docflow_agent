"""文档模块按 Markdown 标题层级构造父块和子块的切分器。

处理策略：
1. 维护标题栈构建稳定的 section_path（如 ['一、项目背景', '1.1 现状分析']）。
2. 将章节正文（不含标题行自身）按 4,000 字符聚合为 ParentBlockData（block_type='section'）。
3. 父块内部按 600 字符上限进一步切分子块（ChildChunkData，chunk_type='text'）。
4. 子块 embedding_text 拼接章节路径（'标题路径：...\\n正文：...'），以丰富检索语义。
"""

import re

from app.modules.document.infrastructure.chunking.base import (
    BaseChunker,
    ChunkBuildInput,
    ChunkBuildResult,
    ParentBlockData,
    ChildChunkData,
)
from app.modules.document.infrastructure.chunking.common import (
    split_text_to_parent_segments,
    split_text_to_child_chunks,
    build_embedding_text,
)

# Markdown ATX 标题正则（匹配 1 到 6 级 # 标题）
HEADING_PATTERN = re.compile(r"^(#{1,6})\s+(.+?)\s*$")


class MarkdownChunker(BaseChunker):
    """Markdown 格式文档切块策略实现类。

    依据标题树结构维护层级路径，构建以章节为边界的父级语义块与细粒度子块。
    """

    def build(
        self,
        input_data: ChunkBuildInput,
    ) -> ChunkBuildResult:
        """解析 Markdown 章节结构并生成分层父子切块。

        Args:
            input_data: 切块输入对象，包含 cleaned_path 与元数据。

        Returns:
            构建完成的父块与子块结果。
        """
        text = input_data.cleaned_path.read_text(
            encoding="utf-8",
            errors="strict",
        )
        lines = text.splitlines()

        # 优先复用 Process 阶段解析出的章节元数据
        sections = input_data.process_metadata.get("sections")
        if not sections:
            sections = self._parse_sections(lines)

        parents: list[ParentBlockData] = []
        children_by_parent_index: dict[int, list[ChildChunkData]] = {}
        block_index = 0

        for semantic_group_index, section in enumerate(sections):
            section_content = self._extract_section_content(lines, section)

            if not section_content:
                continue

            section_path = section.get("section_path") or None
            # 将章节正文按父块上限（4,000 字符）分段
            parent_segments = split_text_to_parent_segments(section_content)

            for segment_index, parent_content in enumerate(parent_segments):
                parent = ParentBlockData(
                    block_type="section",
                    title=section.get("title"),
                    section_path=section_path,
                    content=parent_content,
                    block_index=block_index,
                    semantic_group_index=semantic_group_index,
                    segment_index=segment_index,
                )
                parents.append(parent)

                # 将父块正文按子块上限（600 字符）切分
                child_texts = split_text_to_child_chunks(parent_content)
                children: list[ChildChunkData] = []

                for child_index, child_text in enumerate(child_texts):
                    embedding_text = build_embedding_text(
                        section_path=section_path,
                        content=child_text,
                    )

                    children.append(
                        ChildChunkData(
                            content=child_text,
                            embedding_text=embedding_text,
                            chunk_index=child_index,
                            section_path=section_path,
                            source_row_index=None,
                            chunk_type="text",
                        )
                    )

                children_by_parent_index[block_index] = children
                block_index += 1

        return ChunkBuildResult(
            parents=parents,
            children_by_parent_index=children_by_parent_index,
        )

    def _extract_section_content(
        self,
        lines: list[str],
        section: dict,
    ) -> str:
        """依据 Processor 的 1-based 行范围提取章节正文（不包含标题行自身）。

        Args:
            lines: 文本所有行列表。
            section: 章节元数据字典（包含 heading_line, start_line, end_line）。

        Returns:
            章节正文字符串。
        """
        heading_line = section.get("heading_line")
        start_line = section["start_line"]
        end_line = section["end_line"]

        if heading_line is not None:
            content_start_line = heading_line + 1
        else:
            content_start_line = start_line

        return "\n".join(
            lines[content_start_line - 1:end_line]
        ).strip()

    def _parse_sections(self, lines: list[str]) -> list[dict]:
        """在缺失 Processor 预处理元信息时，依据 ATX 标题重新解析 Markdown 章节树。

        Args:
            lines: 文本行列表。

        Returns:
            章节元数据字典列表（包含 level, title, section_path, start_line, end_line 等）。
        """
        if not lines:
            return []

        heading_stack: list[tuple[int, str]] = []
        sections: list[dict] = []
        current_section: dict | None = None

        for line_number, line in enumerate(lines, start=1):
            match = HEADING_PATTERN.match(line)

            if match:
                if current_section is not None:
                    current_section["end_line"] = line_number - 1
                    sections.append(current_section)

                level = len(match.group(1))
                title = match.group(2).strip()

                # 维护祖先标题栈：同级或更高层级出现时，弹出栈顶
                while heading_stack and heading_stack[-1][0] >= level:
                    heading_stack.pop()

                heading_stack.append((level, title))

                current_section = {
                    "level": level,
                    "title": title,
                    "section_path": [item[1] for item in heading_stack],
                    "heading_line": line_number,
                    "start_line": line_number,
                    "end_line": line_number,
                }
                continue

            # 处理未在任何标题下的前置正文
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
