"""可靠消息应用层数据传输对象（DTO）。

定义跨进程、跨 Worker 传输的统一运行时事件结构体。
"""

from dataclasses import dataclass
from datetime import datetime


class DeferredEvent(Exception):
    """当前执行租约尚有效；保留消息，到指定时间再检查，不计失败。"""

    def __init__(self, available_at: datetime) -> None:
        super().__init__("已有执行租约，等待恢复检查")
        self.available_at = available_at


class InvalidRuntimeEvent(ValueError):
    """消息格式或事件类型无法执行，必须隔离。"""


@dataclass(frozen=True)
class RuntimeEvent:
    """运行时事件数据传输对象。

    表示已从传输层（如 Redis Streams）解构成结构化对象的应用级事件。

    Attributes:
        event_id: 事件唯一标识符（UUID 或业务指定唯一 ID）。
        event_type: 事件类型（如 runtime.plan_wakeup, planning.replan_requested 等）。
        payload: 事件携带的结构化参数载荷字典。
    """

    event_id: str
    event_type: str
    payload: dict
