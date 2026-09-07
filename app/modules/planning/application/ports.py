"""Planning Application 使用的持久化 Port 与运行接口定义。

定义 Planning 模块所需的外部依赖接口（Ports），包括 Planner 运行器接口
以及数据库工作单元（Unit of Work）与模型工厂。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol


class PlannerRunnerPort(Protocol):
    """Planner 规划器执行协议接口。

    封装 LangGraph / OpenAI Agents SDK 的规划器运行逻辑。
    """

    async def run(self, *, planner_input: Any, context: Any) -> Any:
        """运行 Planner 规划图并返回结果。

        Args:
            planner_input: 包含用户输入与关联上下文链的输入数据。
            context: 注入的 AgentToolContext 运行时上下文。

        Returns:
            规划器运行结果（最终状态及可能的澄清提问）。
        """


@dataclass(frozen=True)
class PlanningApplicationPorts:
    """由 Bootstrap 注入 Planning Use Case 的持久化与事件工厂能力集合。

    Attributes:
        uow_factory: 数据库 Unit of Work 工厂。
        plan_factory: Plan ORM 模型实体工厂。
        task_factory: Task ORM 模型实体工厂。
        task_dependency_factory: TaskDependency ORM 模型实体工厂。
        outbox_event_factory: OutboxEvent ORM 模型实体工厂。
        inbox_event_factory: InboxEvent ORM 模型实体工厂。
        clarification_request_factory: ClarificationRequest ORM 模型实体工厂。
        integrity_error_type: 底层数据库唯一性或完整性约束异常类型。
    """

    uow_factory: Callable[[], Any]
    plan_factory: Callable[..., Any]
    task_factory: Callable[..., Any]
    task_dependency_factory: Callable[..., Any]
    outbox_event_factory: Callable[..., Any]
    inbox_event_factory: Callable[..., Any]
    clarification_request_factory: Callable[..., Any]
    integrity_error_type: type[BaseException]

    def is_integrity_error(self, exc: BaseException) -> bool:
        """判断捕获的异常是否属于数据库完整性约束冲突异常。

        Args:
            exc: 捕获的异常对象。

        Returns:
            若为完整性约束冲突则返回 True，否则返回 False。
        """
        return isinstance(exc, self.integrity_error_type)
