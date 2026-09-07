"""本地格式文件清洗处理器工厂模块。

按规范化后的 source_type（txt, md, csv）分发对应的清洗处理器实例。
"""

from fastapi import HTTPException

from app.modules.document.domain.policies import normalize_source_type
from app.modules.document.infrastructure.parsing.base import BaseProcessor
from app.modules.document.infrastructure.parsing.csv import CsvProcessor
from app.modules.document.infrastructure.parsing.markdown import MdProcessor
from app.modules.document.infrastructure.parsing.text import TxtProcessor

# 本地清洗处理器注册映射
PROCESSOR_MAP: dict[str, type[BaseProcessor]] = {
    "txt": TxtProcessor,
    "md": MdProcessor,
    "csv": CsvProcessor,
}


def get_processor(source_type: str) -> BaseProcessor:
    """获取指定文件格式对应的 BaseProcessor 清洗处理器实例。

    Args:
        source_type: 源文件类型字符串（如 'txt', 'md', 'markdown', 'csv'）。

    Returns:
        实例化后的清洗处理器。

    Raises:
        HTTPException: 当传入不支持清洗的文件类型时抛出 400。
    """
    normalized_source_type = normalize_source_type(source_type)

    processor_cls = PROCESSOR_MAP.get(normalized_source_type)

    if processor_cls is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"当前暂不支持处理该文件类型: {source_type}。"
                "请联系上级管理员，或按照知识库管理规则转换为支持的文件类型后再上传。"
            ),
        )

    return processor_cls()


def get_processor_output_type(source_type: str) -> str:
    """返回指定源类型经本地处理器清洗后生成的标准落盘扩展名。

    Args:
        source_type: 源格式。

    Returns:
        清洗后的格式（如 'txt', 'md', 'csv'）。

    Raises:
        HTTPException: 不支持的源格式抛出 400。
    """
    normalized_source_type = normalize_source_type(source_type)

    if normalized_source_type in PROCESSOR_MAP:
        return normalized_source_type

    raise HTTPException(
        status_code=400,
        detail=f"当前暂不支持处理该文件类型: {source_type}",
    )
