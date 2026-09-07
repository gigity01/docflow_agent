"""Operations Application 外部能力端口（Port）协议定义。

定义日志扫描存储层所需遵循的查询接口协议。
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Protocol

from app.modules.operations.application.dto import JsonlScanPage


# 日志事件断言回调函数类型别名：接收单个日志事件字典，返回 bool 判定是否匹配
LogPredicate = Callable[[dict], bool]


class JsonlLogQueryPort(Protocol):
    """底层 JSONL 日志检索仓储端口协议。

    规定在指定目录或文件源中执行带条件过滤、时间区间裁剪与游标分页的扫描能力。
    """

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
        """扫描并过滤 JSONL 日志记录。

        Args:
            predicate: 用于逐条过滤日志事件字典的断言函数。
            created_from: 记录起始时间（含，UTC 比较）。
            created_to: 记录截止时间（含，UTC 比较）。
            limit: 单次拉取最大返回条数。
            cursor: 分页游标字符串（用于断点继续扫描）。
            ascending: 是否按时间升序排列（默认 False，即倒序排列）。

        Returns:
            包含匹配原始字典列表和下一页游标的分页对象。

        Raises:
            OperationsQueryError: 当参数非法、游标无效或文件读取异常时抛出。
        """
        ...
