"""根据标准化文件类型动态选择分词切块器的工厂模块。

支持 txt (TextChunker)、md/markdown (MarkdownChunker) 以及 csv (CsvChunker)。
注意：复杂源格式（PDF, DOCX, PPTX 等）已在 Process 阶段由 Docling 转换为 Markdown，
因此在切块时以 'md' 格式进入 MarkdownChunker 处理。
"""

from fastapi import HTTPException

from app.modules.document.domain.policies import normalize_source_type
from app.modules.document.infrastructure.chunking.base import BaseChunker
from app.modules.document.infrastructure.chunking.csv import CsvChunker
from app.modules.document.infrastructure.chunking.markdown import MarkdownChunker
from app.modules.document.infrastructure.chunking.text import TextChunker


def get_chunker(source_type: str) -> BaseChunker:
    """根据源文件类型返回匹配的 BaseChunker 切块策略实例。

    Args:
        source_type: 原始或中间文件格式（如 'txt', 'md', 'markdown', 'csv'）。

    Returns:
        对应的切块器实例。

    Raises:
        HTTPException: 当传入不支持的文件格式时抛出 400 错误。
    """
    chunkers: dict[str, BaseChunker] = {
        "txt": TextChunker(),
        "md": MarkdownChunker(),
        "csv": CsvChunker(),
    }

    normalized_source_type = normalize_source_type(source_type)
    chunker = chunkers.get(normalized_source_type)

    if chunker is None:
        raise HTTPException(
            status_code=400,
            detail=f"当前暂不支持该文件类型切块: {source_type}",
        )

    return chunker
