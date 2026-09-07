"""为复杂文档生成可交给本地处理器的二级 Markdown 源文件。

流水线阶段 2：Process/Prepare 预备阶段
对于 PDF、Word、PPT 等复杂办公格式，在事务外调用 Docling 等外部转换服务，
生成 Markdown 格式的二级文本（secondary_text）并写入 operation staging 目录，
构造 PendingArtifact 待后续完成事务中持久化。
对于 txt、md、csv 等本地格式，直接返回原始文件路径，不产生二级产物。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from app.modules.document.application.ports import DocumentApplicationPorts
from app.modules.document.application.settings import (
    DocumentProcessingSettings,
)
from app.modules.document.domain.policies import (
    normalize_source_type,
    requires_external_processing,
)

if TYPE_CHECKING:
    from app.modules.document.application.use_cases.process_document import (
        ProcessingContext,
    )


@dataclass(frozen=True)
class PendingArtifact:
    """事务外生成、等待在完成短事务中登记入库的派生产物元数据结构。

    Attributes:
        artifact_type: 产物类型（如 'secondary_text', 'cleaned_text'）。
        artifact_role: 产物角色（如 'process_input', 'process_output'）。
        artifact_format: 产物格式扩展名（如 'md', 'txt', 'csv'）。
        artifact_uri: 产物文件落盘绝对路径。
        artifact_hash: 产物文件 SHA-256 哈希值。
        provider: 转换/提取服务提供方标识（如 'docling'）。
        processor: 处理器类名。
        file_size: 产物文件大小（字节数）。
        char_count: 文本总字符数。
        line_count: 文本总行数。
        metadata: 额外提取元数据字典。
    """

    artifact_type: str
    artifact_role: str
    artifact_format: str
    artifact_uri: str
    artifact_hash: str
    provider: str | None
    processor: str | None
    file_size: int
    char_count: int
    line_count: int
    metadata: dict[str, Any] | None


@dataclass(frozen=True)
class PreparedProcessSource:
    """供文本清洗处理器（Processor）使用的标准化输入文件及其待登记产物信息。

    Attributes:
        source_path: 准备好的输入文件物理路径（对于复杂格式为生成的 .md 文件，对于本地格式为原始文件）。
        source_type: 输入文件格式（'md', 'txt', 'csv'）。
        generated_secondary_text: 是否通过外部转换生成了二级文本。
        secondary_artifact: 待在事务中持久化的二级产物元数据（若未生成则为 None）。
    """

    source_path: Path
    source_type: str
    generated_secondary_text: bool = False
    secondary_artifact: PendingArtifact | None = None


def prepare_process_source(
    document: ProcessingContext,
    *,
    ports: DocumentApplicationPorts,
    settings: DocumentProcessingSettings,
    output_dir: Path,
) -> PreparedProcessSource:
    """在数据库事务外将复杂文档格式转换为 Markdown 并返回内存产物数据，完全不持有数据库锁。

    业务规则：
    - txt, md, csv: 本地可处理格式，不生成二级产物，直接返回原件。
    - pdf, doc, docx, ppt, pptx: 调用 Docling Client 转换为 Markdown，写入 output_dir，
      并生成 PendingArtifact 供后续登记。

    Args:
        document: 领取事务创建的不可变文档处理上下文。
        ports: 外部依赖端口容器（提供 docling_factory 与 calculate_file_hash）。
        settings: 处理配置参数（保留签名兼容）。
        output_dir: 二级产物写入的目标 operation 目录。

    Returns:
        包含准备好的输入文件路径及待登记产物。
    """
    source_type = normalize_source_type(document.source_type)

    # 本地可处理格式不生成二级产物，保留原件作为清洗处理器的直接输入
    if not requires_external_processing(source_type):
        return PreparedProcessSource(
            source_path=document.source_path,
            source_type=source_type,
        )

    # 复杂办公格式通过 Docling 转换为 Markdown
    docling_client = ports.docling_factory()
    markdown_result = docling_client.convert_to_markdown(
        source_path=document.source_path,
        source_type=source_type,
    )
    del settings
    output_dir.mkdir(parents=True, exist_ok=True)

    artifact_code = _generate_artifact_code(document.doc_code)
    secondary_path = output_dir / f"{artifact_code}.md"

    # 将转换出的 Markdown 纯文本落盘
    secondary_path.write_text(markdown_result.markdown, encoding="utf-8")
    pending_artifact = PendingArtifact(
        artifact_type="secondary_text",
        artifact_role="process_input",
        artifact_format="md",
        artifact_uri=str(secondary_path),
        artifact_hash=ports.calculate_file_hash(secondary_path),
        provider=markdown_result.provider,
        processor=docling_client.__class__.__name__,
        file_size=secondary_path.stat().st_size,
        char_count=len(markdown_result.markdown),
        line_count=len(markdown_result.markdown.splitlines()),
        metadata=markdown_result.metadata,
    )

    return PreparedProcessSource(
        source_path=secondary_path,
        source_type="md",
        generated_secondary_text=True,
        secondary_artifact=pending_artifact,
    )


def _generate_artifact_code(doc_code: str) -> str:
    """生成不超过 Artifact 编码字段长度限制的唯一业务编号。

    Args:
        doc_code: 文档业务编码。

    Returns:
        生成的产物业务编码字符串。
    """
    suffix = f"_ART_DOCLING_MD_{uuid4().hex[:12].upper()}"
    return f"{doc_code[:100 - len(suffix)]}{suffix}"
