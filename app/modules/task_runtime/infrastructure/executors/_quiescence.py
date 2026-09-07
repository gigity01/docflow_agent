"""延迟传播取消，直到执行器内部的副作用操作结束。

取消等待协程不会终止线程池中的同步操作。必须先等待旧操作退出，
再允许调用方启动补偿，避免清理与写入并发。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TypeVar

ResultT = TypeVar("ResultT")


async def await_side_effect_quiescence(
    awaitable: Awaitable[ResultT],
) -> ResultT:
    """收到取消后先排空内部执行，避免补偿与旧副作用并发。

    Args:
        awaitable: 需要受保护执行的协程或 Future。

    Returns:
        内部 awaitable 执行成功后的返回结果。

    Raises:
        asyncio.CancelledError: 当外部发生取消且内部执行已完全静默排空后抛出。
        BaseException: 内部执行本身抛出的其他异常。
    """
    task = asyncio.ensure_future(awaitable)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # 重复取消也不能提前放行补偿。
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        # 取出内部异常以免产生未处理告警，仍向调用方传播原始取消。
        if not task.cancelled():
            task.exception()
        raise
