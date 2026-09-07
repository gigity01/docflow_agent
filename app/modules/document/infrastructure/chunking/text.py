"""文档模块纯文本（Plain Text）按段落构造父块和子块的切分器。

处理策略：
1. 依据双换行空行（split_paragraphs）切分自然段。
2. 将每个段落按 4,000 字符限制聚合为 ParentBlockData（block_type='paragraph'）。
3. 在父块内部按 600 字符限制切分为细粒度 ChildChunkData（chunk_type='text'）。
4. 纯文本子块的 embedding_text 等同于 content（无 section_path 标题前缀）。
"""

from app.modules.document.infrastructure.chunking.base import (
    BaseChunker,
    ChunkBuildInput,
    ChunkBuildResult,
    ParentBlockData,
    ChildChunkData,
)
from app.modules.document.infrastructure.chunking.common import (
    split_paragraphs,
    split_text_to_parent_segments,
    split_text_to_child_chunks,
)


class TextChunker(BaseChunker):
    """纯文本格式切块策略实现类。

    以自然段落为基本语义边界，生成父级段落块与子切块。
    """

    def build(
        self,
        input_data: ChunkBuildInput,
    ) -> ChunkBuildResult:
        """读取纯文本 cleaned 文件并按段落构建父子切块。

        Args:
            input_data: 切块输入对象，包含 cleaned_path。

        Returns:
            构建完成的父块与子块结果。
        """
        text = input_data.cleaned_path.read_text(
            encoding="utf-8",
            errors="strict",
        )
        paragraphs = split_paragraphs(text)

        parents: list[ParentBlockData] = []
        children_by_parent_index: dict[int, list[ChildChunkData]] = {}
        block_index = 0

        for semantic_group_index, paragraph in enumerate(paragraphs):
            # 将段落按父块上限（4,000 字符）切分为片段
            parent_segments = split_text_to_parent_segments(paragraph)

            for segment_index, parent_content in enumerate(parent_segments):
                parent = ParentBlockData(
                    block_type="paragraph",
                    title=None,
                    section_path=None,
                    content=parent_content,
                    block_index=block_index,
                    semantic_group_index=semantic_group_index,
                    segment_index=segment_index,
                )
                parents.append(parent)

                # 将父块正文按子块上限（600 字符）切分
                child_texts = split_text_to_child_chunks(parent_content)
                children = [
                    ChildChunkData(
                        content=child_text,
                        embedding_text=child_text,
                        chunk_index=child_index,
                        section_path=None,
                        source_row_index=None,
                        chunk_type="text",
                    )
                    for child_index, child_text in enumerate(child_texts)
                ]

                children_by_parent_index[block_index] = children
                block_index += 1

        return ChunkBuildResult(
            parents=parents,
            children_by_parent_index=children_by_parent_index,
        )
