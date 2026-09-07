"""受控目录下日分割 JSONL 的只读查询 Repository 实现。

扫描由 Bootstrap 显式注入的固定目录和文件前缀，进行安全的只读日志读取：
- 严格做路径解析与目录防越界校验（防止软链接或路径遍历逃逸）。
- 解析每行 JSONL 记录为结构化事件，结合 UTC 时间与断言函数过滤。
- 基于稳定的 5 元组排序键 (created_at, event_id, source_id, filename, line_number) 实现严格全序。
- 提供 Base64 安全编码的游标（Cursor）分页能力，防篡改校验来源签名与排序方向。
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.modules.operations.application.dto import JsonlScanPage
from app.modules.operations.application.errors import OperationsQueryError
from app.modules.operations.application.ports import LogPredicate


@dataclass(frozen=True)
class JsonlLogSource:
    """受控 JSONL 日志源配置。

    Attributes:
        source_id: 日志源唯一标识（如 "doc_logs", "agent_audits"）。
        directory: 日志文件所在的绝对路径目录。
        file_prefix: 日志文件名前缀（如 "document_business_events"）。
    """

    source_id: str
    directory: Path
    file_prefix: str


@dataclass(frozen=True)
class _IndexedEvent:
    """携带全序排序键的内部索引事件封装。

    Attributes:
        event: 原始事件字典。
        sort_key: 5 元组排序键 (created_at ISO, event_id, source_id, filename, line_number)。
    """

    event: dict[str, Any]
    sort_key: tuple[str, str, str, str, int]


class JsonlLogRepository:
    """基于文件系统日切 JSONL 的只读仓储。

    只扫描 Bootstrap 注入的固定目录和文件前缀，实现 JsonlLogQueryPort 协议。
    """

    def __init__(self, sources: tuple[JsonlLogSource, ...]) -> None:
        """初始化 JSONL 仓储。

        Args:
            sources: 允许扫描的受控日志源元组。
        """
        self._sources = sources

    def scan(
        self,
        *,
        predicate: LogPredicate,
        created_from: datetime | None,
        created_to: datetime | None,
        limit: int,
        cursor: str | None = None,
        ascending: bool = False,
    ) -> JsonlScanPage:
        """扫描并过滤受控源中的 JSONL 日志事件，支持游标分页与正反序排序。

        Args:
            predicate: 用于逐条判定事件是否匹配的回调函数。
            created_from: 起始时间（含）。
            created_to: 截止时间（含）。
            limit: 单次拉取最大返回条数。
            cursor: 分页游标。
            ascending: 是否按时间升序（True 为升序，False 默认为降序/最新优先）。

        Returns:
            分页事件字典列表与下一页游标。

        Raises:
            OperationsQueryError: 当 limit < 1 或 cursor 解码校验失败时抛出。
        """
        if limit < 1:
            raise OperationsQueryError(400, "limit 必须大于 0")

        # 1. 解码并校验游标
        cursor_key = self._decode_cursor(cursor, ascending=ascending)
        indexed: list[_IndexedEvent] = []

        # 2. 遍历所有配置的受控日志源并读取匹配事件
        for source in self._sources:
            indexed.extend(
                self._read_source(
                    source,
                    predicate=predicate,
                    created_from=created_from,
                    created_to=created_to,
                )
            )

        # 3. 按全序 sort_key 排序（默认降序，即最新在前）
        indexed.sort(key=lambda item: item.sort_key, reverse=not ascending)

        # 4. 根据游标位置过滤已经消费过的记录
        if cursor_key is not None:
            if ascending:
                indexed = [item for item in indexed if item.sort_key > cursor_key]
            else:
                indexed = [item for item in indexed if item.sort_key < cursor_key]

        # 5. 截取当前页并计算是否存在下一页
        page = indexed[: limit + 1]
        has_more = len(page) > limit
        page = page[:limit]

        # 6. 生成下一页游标
        next_cursor = None
        if has_more and page:
            next_cursor = self._encode_cursor(
                page[-1].sort_key,
                ascending=ascending,
            )
        return JsonlScanPage(
            events=[item.event for item in page],
            next_cursor=next_cursor,
        )

    def _read_source(
        self,
        source: JsonlLogSource,
        *,
        predicate: LogPredicate,
        created_from: datetime | None,
        created_to: datetime | None,
    ) -> list[_IndexedEvent]:
        """读取单个日志源目录下所有匹配模式的日切文件。

        Args:
            source: 日志源配置。
            predicate: 断言函数。
            created_from: 开始时间。
            created_to: 截止时间。

        Returns:
            读取并匹配的索引事件列表。

        Raises:
            OperationsQueryError: 当目录访问出现 OSError 时抛出。
        """
        directory = source.directory.resolve()
        if not directory.is_dir():
            return []
        events: list[_IndexedEvent] = []
        pattern = f"{source.file_prefix}-*.jsonl"
        try:
            paths = sorted(directory.glob(pattern))
        except OSError:
            raise OperationsQueryError(503, "日志目录读取失败") from None

        # 遍历文件并防止符号链接越界
        for path in paths:
            if path.is_symlink() or not path.is_file():
                continue
            resolved = path.resolve()
            if resolved.parent != directory:
                continue
            events.extend(
                self._read_file(
                    source,
                    resolved,
                    predicate=predicate,
                    created_from=created_from,
                    created_to=created_to,
                )
            )
        return events

    def _read_file(
        self,
        source: JsonlLogSource,
        path: Path,
        *,
        predicate: LogPredicate,
        created_from: datetime | None,
        created_to: datetime | None,
    ) -> list[_IndexedEvent]:
        """读取单个 JSONL 文件的每一行并应用时间区间与断言过滤。

        Args:
            source: 日志源配置。
            path: 日志文件路径。
            predicate: 断言函数。
            created_from: 开始时间。
            created_to: 截止时间。

        Returns:
            文件内匹配的索引事件列表。

        Raises:
            OperationsQueryError: 当文件打开读取失败时抛出。
        """
        events: list[_IndexedEvent] = []
        try:
            with path.open("r", encoding="utf-8") as file:
                for line_number, line in enumerate(file, start=1):
                    indexed = self._parse_line(
                        source,
                        path.name,
                        line_number,
                        line,
                    )
                    if indexed is None:
                        continue
                    created_at = self._parse_datetime(
                        indexed.event.get("created_at")
                    )
                    if created_at is None:
                        continue
                    # UTC 时间范围裁剪
                    if created_from is not None and created_at < self._utc(
                        created_from
                    ):
                        continue
                    if created_to is not None and created_at > self._utc(
                        created_to
                    ):
                        continue
                    # 业务断言匹配
                    if predicate(indexed.event):
                        events.append(indexed)
        except OSError:
            raise OperationsQueryError(503, "日志文件读取失败") from None
        return events

    def _parse_line(
        self,
        source: JsonlLogSource,
        filename: str,
        line_number: int,
        line: str,
    ) -> _IndexedEvent | None:
        """解析单行文本为 JSON 字典并构造 _IndexedEvent。

        Args:
            source: 日志源。
            filename: 文件名。
            line_number: 行号（1-based）。
            line: 文本行内容。

        Returns:
            解析成功返回包装实体；非法 JSON 或非字典返回 None。
        """
        try:
            event = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(event, dict):
            return None
        created_at = self._parse_datetime(event.get("created_at"))
        if created_at is None:
            return None
        event_id = str(event.get("event_id") or "")
        return _IndexedEvent(
            event=event,
            sort_key=(
                created_at.isoformat(),
                event_id,
                source.source_id,
                filename,
                line_number,
            ),
        )

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        """将 ISO 8601 时间字符串安全解析为 UTC datetime。

        Args:
            value: 时间字符串（支持含 'Z' 格式）。

        Returns:
            datetime | None: 解析后的 UTC datetime 对象；失败返回 None。
        """
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return JsonlLogRepository._utc(parsed)

    @staticmethod
    def _utc(value: datetime) -> datetime:
        """将任意 datetime 规范化为显式 UTC 时区。

        Args:
            value: 输入 datetime。

        Returns:
            datetime: 带 UTC 时区信息的 datetime。
        """
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _encode_cursor(
        self,
        sort_key: tuple[str, str, str, str, int],
        *,
        ascending: bool,
    ) -> str:
        """将 5 元组排序键与版本、方向、源签名编码为 URL 安全的 Base64 游标。

        Args:
            sort_key: 5 元组排序键。
            ascending: 排序方向。

        Returns:
            Base64 编码的游标字符串。
        """
        payload = json.dumps(
            {
                "version": 1,
                "ascending": ascending,
                "sources": self._source_signature,
                "key": sort_key,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")

    def _decode_cursor(
        self,
        cursor: str | None,
        *,
        ascending: bool,
    ) -> tuple[str, str, str, str, int] | None:
        """解码并严格校验 Base64 游标。

        检查版本、排序方向及源签名是否与当前仓储配置一致。

        Args:
            cursor: 游标字符串。
            ascending: 期望的排序方向。

        Returns:
            5 元组排序键；若 cursor 为 None 则返回 None。

        Raises:
            OperationsQueryError: 当游标格式损坏、版本不匹配或签名不一致时抛出 400 异常。
        """
        if cursor is None:
            return None
        try:
            padding = "=" * (-len(cursor) % 4)
            payload = json.loads(
                base64.urlsafe_b64decode(cursor + padding).decode("utf-8")
            )
            key = payload["key"]
            if (
                payload.get("version") != 1
                or payload.get("ascending") is not ascending
                or payload.get("sources") != self._source_signature
                or not isinstance(key, list)
                or len(key) != 5
                or not isinstance(key[4], int)
                or not all(isinstance(value, str) for value in key[:4])
            ):
                raise ValueError
            return key[0], key[1], key[2], key[3], key[4]
        except (
            ValueError,
            KeyError,
            TypeError,
            UnicodeDecodeError,
            binascii.Error,
            json.JSONDecodeError,
        ):
            raise OperationsQueryError(400, "日志查询 cursor 无效") from None

    @property
    def _source_signature(self) -> list[str]:
        """生成当前仓储绑定的日志源指纹签名列表，用于防游标混用。"""
        return [
            f"{source.source_id}:{source.file_prefix}"
            for source in self._sources
        ]
