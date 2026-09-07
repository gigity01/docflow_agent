"""文档模块 CSV 格式数据的分层父子块切分器。

切分规则：
- 一条 CSV 数据行对应一个可向量化子块（ChildChunkData），chunk_type='csv_row'。
- 子块 content 为该行记录紧凑序列化的 JSON 字符串（最大 8,000 字符）。
- 子块 embedding_text 为非空字段的 '列名：值' 键值对文本。
- 父级语义块（ParentBlockData）按相邻连续数据行批量聚合：上限最多 50 行（CSV_PARENT_MAX_ROWS）
  或最多 12,000 字符（CSV_PARENT_MAX_CHARS），包含 schema 表头元数据与 row_start/row_end 索引。
"""

import csv
import json

from app.modules.document.infrastructure.chunking.base import (
    BaseChunker,
    ChildChunkData,
    ChunkBuildInput,
    ChunkBuildResult,
    ParentBlockData,
)
from app.modules.document.infrastructure.chunking.common import (
    CSV_CHILD_MAX_CHARS,
    CSV_PARENT_MAX_CHARS,
    CSV_PARENT_MAX_ROWS,
)


def build_csv_child_content(record: dict[str, str]) -> str:
    """将单行 CSV 记录序列化为紧凑稳定的 JSON 字符串（保留空值字段）。

    Args:
        record: 单行字段名与字段值的映射字典。

    Returns:
        紧凑 JSON 字符串。
    """
    return json.dumps(
        record,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def build_csv_embedding_text(record: dict[str, str]) -> str:
    """将单行记录中的非空字段转换为适合 Embedding 模型的多行键值对文本。

    Args:
        record: 字段名与值的映射字典。

    Returns:
        由 '列名：值' 换行拼接构成的待向量化正文。
    """
    return "\n".join(
        f"{field}：{value}"
        for field, value in record.items()
        if value
    )


class CsvChunker(BaseChunker):
    """CSV 表格数据切块策略实现类。

    单条记录对应一个 Child Chunk，相邻记录按批次规则组成 Parent Block。
    """

    def build(self, input_data: ChunkBuildInput) -> ChunkBuildResult:
        """流式解析清洗后的 CSV 文件并构建父子块。

        Args:
            input_data: 切块输入对象，包含 cleaned_path 与元数据。

        Returns:
            构建完成的父块与子块结果。

        Raises:
            ValueError: 表头缺失、表头不匹配或单行数据超限时抛出。
        """
        expected_headers = input_data.process_metadata.get("headers")

        parents: list[ParentBlockData] = []
        children_by_parent_index: dict[int, list[ChildChunkData]] = {}

        block_index = 0
        batch_records: list[dict[str, str]] = []
        batch_children: list[ChildChunkData] = []
        batch_start_row = 1

        with input_data.cleaned_path.open(
            "r",
            encoding="utf-8",
            errors="strict",
            newline="",
        ) as stream:
            reader = csv.DictReader(stream)
            headers = reader.fieldnames

            if not headers:
                raise ValueError("CSV cleaned 文件缺少表头")

            if expected_headers and headers != expected_headers:
                raise ValueError("CSV cleaned 文件表头与处理产物元信息不一致")

            for source_row_index, raw_record in enumerate(reader, start=1):
                # 补全空字段
                record = {
                    header: raw_record.get(header) or ""
                    for header in headers
                }
                child_content = build_csv_child_content(record)

                if len(child_content) > CSV_CHILD_MAX_CHARS:
                    raise ValueError(
                        f"CSV 第 {source_row_index} 条记录内容过大"
                    )

                child = ChildChunkData(
                    content=child_content,
                    embedding_text=build_csv_embedding_text(record),
                    chunk_index=len(batch_children),
                    section_path=None,
                    source_row_index=source_row_index,
                    chunk_type="csv_row",
                )

                candidate_records = [*batch_records, record]
                candidate_content = self._build_parent_content(
                    headers=headers,
                    row_start=batch_start_row,
                    row_end=source_row_index,
                    records=candidate_records,
                )
                exceeds_row_limit = (
                    len(candidate_records) > CSV_PARENT_MAX_ROWS
                )
                exceeds_char_limit = (
                    len(candidate_content) > CSV_PARENT_MAX_CHARS
                )

                # 若加入当前记录后超出父块批次上限，则先行写入已累积的批次
                if batch_records and (exceeds_row_limit or exceeds_char_limit):
                    block_index = self._flush_batch(
                        parents=parents,
                        children_by_parent_index=children_by_parent_index,
                        block_index=block_index,
                        headers=headers,
                        row_start=batch_start_row,
                        row_end=source_row_index - 1,
                        records=batch_records,
                        children=batch_children,
                    )
                    batch_records = []
                    batch_children = []
                    batch_start_row = source_row_index
                    child.chunk_index = 0

                    candidate_content = self._build_parent_content(
                        headers=headers,
                        row_start=batch_start_row,
                        row_end=source_row_index,
                        records=[record],
                    )

                if (
                    not batch_records
                    and len(candidate_content) > CSV_PARENT_MAX_CHARS
                ):
                    raise ValueError(
                        f"CSV 第 {source_row_index} 条记录超过父块大小限制"
                    )

                batch_records.append(record)
                batch_children.append(child)

        # 写入最后一个未满批次
        if batch_records:
            self._flush_batch(
                parents=parents,
                children_by_parent_index=children_by_parent_index,
                block_index=block_index,
                headers=headers,
                row_start=batch_start_row,
                row_end=batch_start_row + len(batch_records) - 1,
                records=batch_records,
                children=batch_children,
            )

        return ChunkBuildResult(
            parents=parents,
            children_by_parent_index=children_by_parent_index,
        )

    def _flush_batch(
        self,
        *,
        parents: list[ParentBlockData],
        children_by_parent_index: dict[int, list[ChildChunkData]],
        block_index: int,
        headers: list[str],
        row_start: int,
        row_end: int,
        records: list[dict[str, str]],
        children: list[ChildChunkData],
    ) -> int:
        """将当前累积的记录批次持久化为一个 ParentBlockData，并返回下一个序号。"""
        parent_content = self._build_parent_content(
            headers=headers,
            row_start=row_start,
            row_end=row_end,
            records=records,
        )

        parents.append(
            ParentBlockData(
                block_type="csv_rows",
                title=f"第 {row_start}-{row_end} 行",
                section_path=None,
                content=parent_content,
                block_index=block_index,
                semantic_group_index=block_index,
                segment_index=0,
            )
        )
        children_by_parent_index[block_index] = children

        return block_index + 1

    def _build_parent_content(
        self,
        *,
        headers: list[str],
        row_start: int,
        row_end: int,
        records: list[dict[str, str]],
    ) -> str:
        """构造包含 schema、起止行号和多行记录的父块紧凑 JSON 文本。"""
        return json.dumps(
            {
                "schema": headers,
                "row_start": row_start,
                "row_end": row_end,
                "records": records,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
