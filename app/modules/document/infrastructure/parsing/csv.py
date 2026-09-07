"""文档模块 CSV 表格文件编码嗅探、方言检测与标准化清洗处理器。

处理流程：
1. 自动嗅探字符编码（utf-8-sig, utf-8, gb18030）。
2. 使用 csv.Sniffer 自动识别分隔符（逗号、分号、制表符、竖线）。
3. 严格校验表头：去除多余空白、拒绝空列名、拒绝重复列名。
4. 流式逐行清洗数据单元格（移除 NUL 字符、统一 \\n 换行、去除两端空白）。
5. 过滤全空白行并校验每行字段列数与表头严格一致。
6. 原子写入标准 UTF-8、逗号分隔、带双引号转义的 cleaned CSV 文件。
"""

import codecs
import csv
import re
from pathlib import Path
from typing import TextIO

from app.modules.document.infrastructure.parsing.base import (
    BaseProcessor,
    ProcessResult,
)


class CsvProcessError(ValueError):
    """CSV 文件处理异常，表示文件无法安全完成标准化清洗。"""


class _CharacterCountingWriter:
    """包装文本流 write 方法并累计 Unicode 字符数的辅助写入流。"""

    def __init__(self, stream: TextIO) -> None:
        """初始化计数写入流。

        Args:
            stream: 底层文件写入流。
        """
        self._stream = stream
        self.char_count = 0

    def write(self, value: str) -> int:
        """写入字符串并累加字符计数。"""
        written = self._stream.write(value)
        self.char_count += written
        return written


class CsvProcessor(BaseProcessor):
    """CSV 表格数据清洗与标准化处理器实现。"""

    source_type = "csv"

    # 候选解码编码列表（优先级从高到低）
    CANDIDATE_ENCODINGS = (
        "utf-8-sig",
        "utf-8",
        "gb18030",
    )
    # 支持嗅探的分隔符
    SUPPORTED_DELIMITERS = (
        ",",
        ";",
        "\t",
        "|",
    )
    # 编码与方言嗅探样本大小（64 KiB）
    SAMPLE_SIZE = 64 * 1024

    def process(
        self,
        source_path: Path,
        cleaned_path: Path,
    ) -> ProcessResult:
        """流式读取源 CSV，完成编码检测、方言识别、表头校验及清洗并写入 cleaned 文件。

        Args:
            source_path: 原始 CSV 文件路径。
            cleaned_path: 清洗后输出的标准 CSV 目标路径。

        Returns:
            包含表头元数据、行列数统计的处理结果。

        Raises:
            CsvProcessError: 格式损坏、表头非法或行列数不匹配时抛出。
        """
        source_path = self.validate_source_path(source_path)
        cleaned_path = self.prepare_cleaned_path(cleaned_path)

        if source_path.stat().st_size == 0:
            raise CsvProcessError("CSV 文件为空")

        # 1. 嗅探编码与方言
        source_encoding = self._detect_encoding(source_path)
        sample = self._read_decoded_sample(source_path, source_encoding)
        dialect = self._detect_dialect(sample)
        source_delimiter = dialect.delimiter

        # 2. 创建临时文件写入流
        temporary_path = cleaned_path.with_name(f"{cleaned_path.name}.tmp")
        row_count = 0
        blank_row_count = 0
        reader = None

        try:
            with source_path.open(
                "r",
                encoding=source_encoding,
                errors="strict",
                newline="",
            ) as source_stream:
                reader = csv.reader(source_stream, dialect=dialect, strict=True)

                try:
                    raw_headers = next(reader)
                except StopIteration as exc:
                    raise CsvProcessError("CSV 表头为空") from exc

                headers = self._normalize_headers(raw_headers)
                column_count = len(headers)

                with temporary_path.open(
                    "w",
                    encoding="utf-8",
                    newline="",
                ) as output_stream:
                    counting_stream = _CharacterCountingWriter(output_stream)
                    writer = csv.writer(
                        counting_stream,
                        delimiter=",",
                        quotechar='"',
                        quoting=csv.QUOTE_MINIMAL,
                        lineterminator="\n",
                    )
                    writer.writerow(headers)

                    # 3. 逐行清洗与校验
                    for data_record_number, row in enumerate(reader, start=1):
                        cleaned_row = [self._clean_cell(cell) for cell in row]

                        if self._is_blank_row(cleaned_row):
                            blank_row_count += 1
                            continue

                        if len(cleaned_row) != column_count:
                            raise CsvProcessError(
                                f"CSV 第 {data_record_number} 条数据记录字段数量错误"
                                f"（物理行位置: {reader.line_num}），"
                                f"期望 {column_count} 列，实际 {len(cleaned_row)} 列"
                            )

                        writer.writerow(cleaned_row)
                        row_count += 1

            # 4. 原子重命名生效
            temporary_path.replace(cleaned_path)
        except UnicodeDecodeError as exc:
            temporary_path.unlink(missing_ok=True)
            raise CsvProcessError(
                f"CSV 文件无法使用检测到的编码 {source_encoding} 完整解码"
            ) from exc
        except csv.Error as exc:
            temporary_path.unlink(missing_ok=True)
            physical_line = reader.line_num if reader is not None else 0
            if "field larger than field limit" in str(exc):
                raise CsvProcessError(
                    f"CSV 单条记录超出安全限制（物理行位置: {physical_line}）"
                ) from exc
            raise CsvProcessError(
                f"CSV 格式损坏（物理行位置: {physical_line}）: {exc}"
            ) from exc
        except CsvProcessError:
            temporary_path.unlink(missing_ok=True)
            raise
        except OSError as exc:
            temporary_path.unlink(missing_ok=True)
            raise CsvProcessError(f"CSV 文件读写失败: {exc}") from exc

        return ProcessResult(
            source_path=source_path,
            cleaned_path=cleaned_path,
            source_type=self.source_type,
            char_count=counting_stream.char_count,
            line_count=row_count + 1,
            metadata={
                "format": "csv",
                "source_encoding": source_encoding,
                "output_encoding": "utf-8",
                "source_delimiter": source_delimiter,
                "output_delimiter": ",",
                "headers": headers,
                "column_count": column_count,
                "row_count": row_count,
                "blank_row_count": blank_row_count,
            },
        )

    def _detect_encoding(self, source_path: Path) -> str:
        """读取文件前 64 KiB 字节嗅探匹配的编码格式。"""
        with source_path.open("rb") as source_stream:
            sample = source_stream.read(self.SAMPLE_SIZE)

        for encoding in self.CANDIDATE_ENCODINGS:
            # utf-8-sig 在无 BOM 时也能解码普通 UTF-8，需优先校验 BOM
            if encoding == "utf-8-sig" and not sample.startswith(codecs.BOM_UTF8):
                continue

            try:
                decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
                decoder.decode(sample, final=False)
            except UnicodeDecodeError:
                continue

            return encoding

        attempted = ", ".join(self.CANDIDATE_ENCODINGS)
        raise CsvProcessError(f"无法识别 CSV 编码，已尝试: {attempted}")

    def _read_decoded_sample(self, source_path: Path, encoding: str) -> str:
        """按已确定的编码解码样本字节，用于方言嗅探。"""
        with source_path.open("rb") as source_stream:
            sample = source_stream.read(self.SAMPLE_SIZE)

        decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
        return decoder.decode(sample, final=False)

    def _detect_dialect(self, sample: str) -> csv.Dialect:
        """识别 CSV 分隔符方言；嗅探失败时回退至标准逗号 Excel 方言。"""
        normalized_sample = sample.replace("\x00", "")
        samples_to_sniff = [normalized_sample]
        first_non_blank_line = next(
            (line for line in normalized_sample.splitlines() if line.strip()),
            "",
        )
        if first_non_blank_line and first_non_blank_line != normalized_sample:
            samples_to_sniff.append(first_non_blank_line)

        for candidate_sample in samples_to_sniff:
            try:
                return csv.Sniffer().sniff(
                    candidate_sample,
                    delimiters="".join(self.SUPPORTED_DELIMITERS),
                )
            except csv.Error:
                continue

        return csv.get_dialect("excel")

    def _normalize_headers(self, headers: list[str]) -> list[str]:
        """规范化表头列名：压缩空白、校验非空与查重。"""
        if not headers:
            raise CsvProcessError("CSV 表头为空")

        cleaned_headers = [
            re.sub(r"\s+", " ", self._clean_cell(header))
            for header in headers
        ]
        if not any(cleaned_headers):
            raise CsvProcessError("CSV 表头为空")

        normalized_headers: list[str] = []
        seen_headers: set[str] = set()

        for column_number, normalized_header in enumerate(cleaned_headers, start=1):
            if not normalized_header:
                raise CsvProcessError(f"CSV 第 {column_number} 列表头为空")

            if normalized_header in seen_headers:
                raise CsvProcessError(
                    f"CSV 表头存在重复字段: {normalized_header}"
                )

            normalized_headers.append(normalized_header)
            seen_headers.add(normalized_header)

        return normalized_headers

    def _clean_cell(self, value: str) -> str:
        """剔除 NUL、统一 \\n 换行并去除单元格两端空白。"""
        return (
            value.replace("\x00", "")
            .replace("\r\n", "\n")
            .replace("\r", "\n")
            .strip()
        )

    def _is_blank_row(self, row: list[str]) -> bool:
        """检查整行是否全为空单元格。"""
        return all(not cell for cell in row)
