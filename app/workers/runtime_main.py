"""Outbox Publisher 与 Runtime Event Consumer 独立 Worker 进程入口模块。

职责说明：
- 作为独立进程运行可靠事件驱动循环：
  1. `run_outbox_loop`: 持续轮询 MySQL `outbox_events` 表，将待发布事件可靠推送到 Redis Streams；
  2. `run_stream_loop`: 通过 Redis Stream Consumer Group 持续监听消费事件，驱动 `RuntimeEventDispatcher` 执行任务领取、执行、Replan 与 Plan 聚合。
- 注册 SIGINT / SIGTERM 操作系统信号监听器，保证收到停机指令时优雅退出循环并安全释放数据库和 Redis 连接。
- 保证 FastAPI Web 进程与异步执行 Worker 进程物理解耦，单进程部署时不消耗 Web 线程池。
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
from uuid import uuid4

from app.bootstrap.lifespan import build_container
from app.modules.messaging.infrastructure.redis_streams import (
    RedisStreamWorker,
)

LOGGER = logging.getLogger(__name__)

# 循环与退避时间常量（秒/毫秒）
OUTBOX_IDLE_SECONDS = 0.5
INITIAL_RETRY_SECONDS = 1.0
MAX_RETRY_SECONDS = 30.0
STREAM_BLOCK_MILLISECONDS = 5_000


def build_consumer_name() -> str:
    """生成跨主机、进程及进程重启均全局唯一的 Redis 消费者名称。

    格式: `<hostname>:<pid>:<uuid_prefix>`

    返回:
        str: 唯一的 consumer_name 标识。
    """
    return f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:12]}"


async def _wait_or_stop(
    stop_event: asyncio.Event,
    timeout_seconds: float,
) -> bool:
    """等待指定超时时间或在收到停止事件时立即返回。

    参数:
        stop_event: 停机通知事件对象。
        timeout_seconds: 等待超时秒数。

    返回:
        bool: 若被停止事件唤醒返回 True，超时返回 False。
    """
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=timeout_seconds)
    except TimeoutError:
        return False
    return True


async def run_outbox_loop(
    publisher,
    *,
    stop_event: asyncio.Event,
) -> None:
    """Outbox 发布器长循环：持续批量发布 MySQL Outbox 事件到 Redis Streams。

    遇到循环级异常时按指数退避重试（1.0s -> 2.0s -> ... -> 30.0s）。

    参数:
        publisher: OutboxPublisher 实例。
        stop_event: 停止事件通知。
    """
    retry_seconds = INITIAL_RETRY_SECONDS
    while not stop_event.is_set():
        try:
            published = await publisher.publish_batch()
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception(
                "Outbox Publisher 循环异常，%.1f 秒后重试",
                retry_seconds,
            )
            if await _wait_or_stop(stop_event, retry_seconds):
                return
            retry_seconds = min(retry_seconds * 2, MAX_RETRY_SECONDS)
            continue

        retry_seconds = INITIAL_RETRY_SECONDS
        if published == 0 and await _wait_or_stop(
            stop_event,
            OUTBOX_IDLE_SECONDS,
        ):
            return


async def run_stream_loop(
    worker: RedisStreamWorker,
    *,
    stop_event: asyncio.Event,
) -> None:
    """Redis Stream 消费长循环：持续拉取并处理 Runtime 消息事件。

    失败消息不执行 ACK，保留在 Pending Entries List (PEL) 中供后续重试或由集群其他实例接管。

    参数:
        worker: RedisStreamWorker 消费工作者实例。
        stop_event: 停止事件通知。
    """
    retry_seconds = INITIAL_RETRY_SECONDS
    while not stop_event.is_set():
        try:
            await worker.run_once(
                block_milliseconds=STREAM_BLOCK_MILLISECONDS,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception(
                "Runtime Stream 循环异常，%.1f 秒后重试",
                retry_seconds,
            )
            if await _wait_or_stop(stop_event, retry_seconds):
                return
            retry_seconds = min(retry_seconds * 2, MAX_RETRY_SECONDS)
            continue

        retry_seconds = INITIAL_RETRY_SECONDS


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    """向当前事件循环注册 SIGINT (Ctrl+C) 与 SIGTERM 终止信号处理器。

    参数:
        stop_event: 触发停机的异步事件。
    """
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop_event.set)
        except NotImplementedError:
            # Windows 事件循环不支持 add_signal_handler；主线程转发停机信号。
            signal.signal(
                signum,
                lambda _signum, _frame: loop.call_soon_threadsafe(stop_event.set),
            )


async def run_worker() -> None:
    """装配全局依赖容器并并发启动 Outbox Publisher 与 Stream Consumer 任务。

    在接收到退出信号后排空并发任务并优雅释放应用连接池。
    """
    container = await build_container()
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)
    redis_worker = RedisStreamWorker(
        container.redis_client,
        dispatcher=container.runtime_event_dispatcher,
        delivery=container.message_delivery,
        consumer_name=build_consumer_name(),
    )
    # 并发启动 Outbox 发布与 Stream 消费两个异步任务
    tasks = [
        asyncio.create_task(
            run_outbox_loop(
                container.outbox_publisher,
                stop_event=stop_event,
            ),
            name="outbox-publisher-loop",
        ),
        asyncio.create_task(
            run_stream_loop(redis_worker, stop_event=stop_event),
            name="runtime-stream-loop",
        ),
    ]

    try:
        await stop_event.wait()
    finally:
        stop_event.set()
        try:
            await asyncio.gather(*tasks)
        finally:
            await container.aclose()


def main() -> None:
    """CLI 入口函数：配置根日志格式并启动异步 Worker 主循环。

    运行命令:
        `uv run --frozen python -m app.workers.runtime_main`
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
