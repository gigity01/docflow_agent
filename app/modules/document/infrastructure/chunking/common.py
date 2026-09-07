"""文档模块切块器共享的文本切分、长度限制与向量文本组装工具。

统一定义父子块长度与行数约束：
- PARENT_BLOCK_MAX_CHARS: 普通文本父块最大字符数（4,000）
- CHILD_CHUNK_MAX_CHARS: 普通文本/Markdown 子块最大字符数（600）
- CSV_PARENT_MAX_ROWS: CSV 父块最大记录行数（50 行）
- CSV_PARENT_MAX_CHARS: CSV 父块最大字符数（12,000）
- CSV_CHILD_MAX_CHARS: CSV 子块单条记录最大字符数（8,000）
"""

import re

from app.modules.document.domain.policies import md5_text

# 普通文本父块最大字符数
PARENT_BLOCK_MAX_CHARS = 4_000
# 普通文本与 Markdown 可向量化子块最大字符数
CHILD_CHUNK_MAX_CHARS = 600

# CSV 批量父块记录上限
CSV_PARENT_MAX_ROWS = 50
# CSV 批量父块字符上限
CSV_PARENT_MAX_CHARS = 12_000
# CSV 单条记录子块字符上限
CSV_CHILD_MAX_CHARS = 8_000


def normalize_text(text: str) -> str:
    """归一化换行符并去除两端空白，保持正文语义内容不变。

    Args:
        text: 待归一化的输入字符串。

    Returns:
        统一使用 '\n' 换行且两端无冗余空白的字符串。
    """
    return (
        text.replace("\r\n", "\n")
        .replace("\r", "\n")
        .strip()
    )


def split_paragraphs(text: str) -> list[str]:
    """按双换行（空行）划分非空自然段落。

    Args:
        text: 待划分的全文文本。

    Returns:
        非空段落字符串列表。
    """
    text = normalize_text(text)
    paragraphs = re.split(r"\n\s*\n", text)
    return [p.strip() for p in paragraphs if p.strip()]


def split_text_to_child_chunks(
    text: str,
    max_len: int = CHILD_CHUNK_MAX_CHARS,
) -> list[str]:
    """将文本切分为长度受限的可向量化子块（Child Chunk）。

    切分规则策略：
    1. 优先在句末标点（。！？；.!?;）处断句。
    2. 贪心合并短句至不超过 max_len（默认 600 字符）。
    3. 若单个句子仍超过 max_len，优先在空格处切分；若仍超长则硬切。

    Args:
        text: 待切分子块的文本。
        max_len: 单个子块最大字符数（默认 600）。

    Returns:
        切分后的子块文本列表。
    """
    text = normalize_text(text)

    if not text:
        return []

    sentence_endings = set("。！？；.!?;")
    sentences: list[str] = []
    buffer = ""

    # 第一阶段：按标点符号断句
    for char in text:
        buffer += char
        if char in sentence_endings:
            sentence = buffer.strip()
            if sentence:
                sentences.append(sentence)
            buffer = ""

    if buffer.strip():
        sentences.append(buffer.strip())

    chunks: list[str] = []
    current = ""

    # 第二阶段：贪心聚合句子至子块上限
    for sentence in sentences:
        if len(sentence) > max_len:
            if current.strip():
                chunks.append(current.strip())
                current = ""

            chunks.extend(_split_long_sentence(sentence, max_len))
            continue

        candidate = sentence if not current else f"{current} {sentence}"

        if len(candidate) <= max_len:
            current = candidate
        else:
            if current.strip():
                chunks.append(current.strip())
            current = sentence

    if current.strip():
        chunks.append(current.strip())

    return chunks


def split_text_to_parent_segments(
    text: str,
    max_chars: int = PARENT_BLOCK_MAX_CHARS,
) -> list[str]:
    """优先按自然段落聚合父级语义块，并为超长段落提供分段保护。

    Args:
        text: 输入文本。
        max_chars: 单个父块最大字符数（默认 4,000）。

    Returns:
        聚合后的父块片段列表。
    """
    text = text.strip()

    if not text:
        return []

    if len(text) <= max_chars:
        return [text]

    paragraphs = [
        item.strip()
        for item in text.split("\n\n")
        if item.strip()
    ]

    segments: list[str] = []
    current_parts: list[str] = []
    current_length = 0

    for paragraph in paragraphs:
        separator_length = 2 if current_parts else 0
        addition_length = separator_length + len(paragraph)

        # 若累积段落超出上限，先 flush 当前父块
        if current_parts and current_length + addition_length > max_chars:
            segments.append("\n\n".join(current_parts))
            current_parts = []
            current_length = 0

        # 若单段未超限，加入当前批次
        if len(paragraph) <= max_chars:
            separator_length = 2 if current_parts else 0
            current_parts.append(paragraph)
            current_length += separator_length + len(paragraph)
            continue

        # 若单段本身已超长，先 flush 之前内容，再对该段进行深度切分
        if current_parts:
            segments.append("\n\n".join(current_parts))
            current_parts = []
            current_length = 0

        segments.extend(
            split_text_to_child_chunks(
                paragraph,
                max_len=max_chars,
            )
        )

    if current_parts:
        segments.append("\n\n".join(current_parts))

    return segments


def _split_long_sentence(text: str, max_len: int) -> list[str]:
    """优先在空格处分割超长单句；无空格语言则交由硬切分保证长度上限。"""
    parts = text.split(" ")

    if len(parts) <= 1:
        return _hard_split(text, max_len)

    chunks: list[str] = []
    current = ""

    for part in parts:
        if len(part) > max_len:
            if current.strip():
                chunks.append(current.strip())
                current = ""
            chunks.extend(_hard_split(part, max_len))
            continue

        candidate = part if not current else current + " " + part

        if len(candidate) <= max_len:
            current = candidate
        else:
            if current.strip():
                chunks.append(current.strip())
            current = part

    if current.strip():
        chunks.append(current.strip())

    return chunks


def _hard_split(text: str, max_len: int) -> list[str]:
    """在没有可用语义边界时按严格字符步长硬切分，作为底层兜底保护。"""
    return [
        text[i:i + max_len].strip()
        for i in range(0, len(text), max_len)
        if text[i:i + max_len].strip()
    ]


def build_embedding_text(
    section_path: list[str] | None,
    content: str,
) -> str:
    """将章节面包屑标题路径拼入待向量化文本，增强向量索引的语义上下文。

    注意：子块的 content 字段仍保存纯净的正文原始切片；
    仅有 embedding_text 拼接了前缀 '标题路径：...\n正文：...'。

    Args:
        section_path: 章节路径列表（如 ['第1章 概述', '1.1 背景']）。
        content: 子块正文内容。

    Returns:
        拼接后的待 Embedding 向量化文本。
    """
    if section_path:
        return f"标题路径：{'>'.join(section_path)}\n正文：{content}"

    return content
