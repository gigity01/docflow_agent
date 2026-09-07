"""业务事务内使用的收件箱（Inbox）幂等辅助器。

用于在消费消息的业务事务内部校验并登记事件消费记录，防止同一消费者对同一事件进行重复消费处理。
"""

from datetime import datetime
from uuid import uuid4


def record_inbox_once(
    uow,
    *,
    inbox_event_factory,
    consumer_name: str,
    event_id: str,
) -> bool:
    """在调用方业务工作单元（Unit of Work）中原子登记收件箱消费记录。

    若该消费者（consumer_name）已处理过该事件（event_id），则直接返回 False；
    若未处理过，则向 Inbox 仓储中添加消费凭据并返回 True。由外部业务事务统一提交。

    Args:
        uow: 包含 inbox 仓储的业务工作单元实例。
        inbox_event_factory: InboxEvent 领域/持久化模型构造工厂方法。
        consumer_name: 消费者名称标识（如 "runtime.dispatcher"）。
        event_id: 待消费的事件唯一标识。

    Returns:
        True 表示首次消费登记成功，可继续执行业务；False 表示事件已处理过，应跳过幂等执行。
    """
    # 1. 检查当前消费者是否已记录该事件
    if uow.inbox.exists(consumer_name, event_id):
        return False

    # 2. 构造并登记收件箱记录
    uow.inbox.add(
        inbox_event_factory(
            inbox_id=f"inbox_{uuid4().hex}",
            consumer_name=consumer_name,
            event_id=event_id,
            processed_at=datetime.now(),
        )
    )
    return True
