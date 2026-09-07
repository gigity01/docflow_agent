"""应用全局共享时间辅助工具模块。

职责说明：
- 提供获取毫秒级 Unix 时间戳与标准 UTC ISO-8601 时间格式字符串的辅助函数。
"""

import time
from datetime import datetime, timezone


def now_ms() -> int:
    """获取当前 Unix 时间戳（单位：毫秒）。

    返回:
        int: 当前时间的毫秒时间戳。
    """
    return time.time_ns() // 1_000_000


def now_utc_iso() -> str:
    """获取当前时间的标准 UTC ISO-8601 格式字符串（包含时区信息）。

    返回:
        str: 如 `2026-08-25T10:00:00.000000+00:00` 的时间字符串。
    """
    return datetime.now(timezone.utc).isoformat()


def utc_now() -> datetime:
    """数据库 DATETIME 统一保存无时区的 UTC，跨进程比较使用同一基准。"""
    return datetime.now(timezone.utc).replace(tzinfo=None)
