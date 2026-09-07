"""文档原始文件清洗处理器的统一抽象基类。"""

from abc import ABC, abstractmethod
from pathlib import Path

from app.modules.document.domain.models import ProcessResult


class BaseProcessor(ABC):
    """文件清洗处理器抽象基类。

    职责：将 source_path 处的原始文件或二级转换文件读取、清洗并输出到 cleaned_path。
    """

    source_type: str

    @abstractmethod
    def process(
        self,
        source_path: Path,
        cleaned_path: Path,
    ) -> ProcessResult:
        """读取源文件、执行文本清洗与结构提取，写入标准 cleaned 文件并返回处理结果。

        Args:
            source_path: 待清洗的输入文件路径。
            cleaned_path: 清洗后输出的标准文本文件路径。

        Returns:
            ProcessResult: 处理结果对象（包含字符数、行数及元数据）。

        Raises:
            FileNotFoundError: 源文件不存在。
            ValueError: 格式损坏或内容非法。
        """
        raise NotImplementedError

    def validate_source_path(self, source_path: Path) -> Path:
        """校验输入源路径存在且为普通文件，返回其绝对路径。

        Args:
            source_path: 待校验路径。

        Returns:
            解析后的绝对路径。

        Raises:
            FileNotFoundError: 路径不存在。
            ValueError: 路径为目录或非普通文件。
        """
        if not source_path.exists():
            raise FileNotFoundError(f"源文件不存在: {source_path}")

        if not source_path.is_file():
            raise ValueError(f"源路径不是有效文件: {source_path}")
        return source_path.resolve()

    def prepare_cleaned_path(self, cleaned_path: Path) -> Path:
        """准备清洗输出文件路径（递归创建父级目录），并拒绝目录作为输出。

        Args:
            cleaned_path: 输出目标路径。

        Returns:
            解析后的绝对输出路径。

        Raises:
            ValueError: 输出目标已存在且为目录。
        """
        if cleaned_path.exists() and cleaned_path.is_dir():
            raise ValueError(f"清洗输出路径不能是目录: {cleaned_path}")

        cleaned_path.parent.mkdir(parents=True, exist_ok=True)
        return cleaned_path.resolve()
