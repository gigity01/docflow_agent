"""Task Runtime 端口与 Registry 注册表定义。

定义 Task 执行器（TaskExecutorPort）、副作用补偿器（OperationCompensatorPort）、
领域能力元数据定义（CapabilityDefinition）及其对应的注册表组件。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict

from app.modules.task_runtime.application.dto import (
    TaskExecutorResult,
    TaskRuntimeContext,
)


class TaskExecutorPort(Protocol):
    """Task 执行器协议接口。

    负责将领域能力输入 payload 转换为实际执行并返回 TaskExecutorResult。
    """

    async def execute(
        self,
        payload: BaseModel,
        context: TaskRuntimeContext,
    ) -> TaskExecutorResult:
        """执行指定能力的 Task。

        Args:
            payload: 能力输入参数模型。
            context: 运行时上下文信息。

        Returns:
            包含执行产出 output_json 与资源引用的结果。
        """
        ...


class OperationCompensatorPort(Protocol):
    """操作级副作用补偿器协议接口。

    当 Task 执行失败、超时或发生不可恢复错误时，负责确定性地清理该 operation_id
    创建的外部副作用（如 staging 文件、未提交分块或向量写入）。
    """

    async def compensate(
        self,
        *,
        operation_id: str,
        payload: BaseModel,
        context: TaskRuntimeContext,
    ) -> None:
        """补偿指定 operation_id 产生的持久化副作用并释放 ownership。

        Args:
            operation_id: 本次执行的操作标识 / ownership token。
            payload: 任务输入参数模型。
            context: 运行时上下文信息。
        """
        ...


class CapabilityDefinition(BaseModel):
    """领域能力执行与补偿元数据定义模型。

    Attributes:
        capability_code: 领域能力唯一编码（如 process_document）。
        input_model: 输入参数 Pydantic 模型类。
        output_model: 输出产物 Pydantic 模型类。
        executor_code: 绑定的 Executor 标识。
        compensator_code: 绑定的 Compensator 标识（若有副作用则必填）。
        max_attempts: 允许的最大尝试执行次数。
        timeout_seconds: 单次执行的超时时间（秒）。
        side_effect: 是否具有外部持久化副作用。
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    capability_code: str
    input_model: type[BaseModel]
    output_model: type[BaseModel]
    executor_code: str
    compensator_code: str | None
    max_attempts: int
    timeout_seconds: int
    side_effect: bool


class CapabilityRegistry:
    """系统支持的全部 Capability 元数据可信注册表。"""

    def __init__(self, definitions: list[CapabilityDefinition]) -> None:
        """初始化 CapabilityRegistry。

        Args:
            definitions: 能力定义列表。

        Raises:
            ValueError: capability_code 存在重复时。
        """
        self._definitions = {
            definition.capability_code: definition
            for definition in definitions
        }
        if len(self._definitions) != len(definitions):
            raise ValueError("CapabilityDefinition capability_code 不得重复")

    def require(self, capability_code: str) -> CapabilityDefinition:
        """根据 capability_code 获取能力元数据定义，若不存在则抛出异常。

        Args:
            capability_code: 目标能力编码。

        Returns:
            能力元数据。

        Raises:
            ValueError: 当能力未注册时。
        """
        try:
            return self._definitions[capability_code]
        except KeyError as exc:
            raise ValueError(f"未知 Capability: {capability_code}") from exc


class ExecutorRegistry:
    """Task Executor 实例注册表。"""

    def __init__(self, executors: dict[str, TaskExecutorPort]) -> None:
        """初始化 ExecutorRegistry。

        Args:
            executors: executor_code 到 TaskExecutorPort 实例的映射字典。
        """
        self._executors = dict(executors)

    def require(self, executor_code: str) -> TaskExecutorPort:
        """获取指定编码的 Executor 实例。

        Args:
            executor_code: Executor 编码标识。

        Returns:
            执行器实例。

        Raises:
            ValueError: 当 Executor 未注册时。
        """
        try:
            return self._executors[executor_code]
        except KeyError as exc:
            raise ValueError(f"未知 Executor: {executor_code}") from exc


class CompensatorRegistry:
    """Operation Compensator 实例注册表。"""

    def __init__(
        self,
        compensators: dict[str, OperationCompensatorPort],
    ) -> None:
        """初始化 CompensatorRegistry。

        Args:
            compensators: compensator_code 到 OperationCompensatorPort 实例的映射字典。
        """
        self._compensators = dict(compensators)

    def require(self, compensator_code: str) -> OperationCompensatorPort:
        """获取指定编码的 Compensator 实例。

        Args:
            compensator_code: 补偿器编码标识。

        Returns:
            补偿器实例。

        Raises:
            ValueError: 当 Compensator 未注册时。
        """
        try:
            return self._compensators[compensator_code]
        except KeyError as exc:
            raise ValueError(
                f"未知 Compensator: {compensator_code}"
            ) from exc


@dataclass(frozen=True)
class TaskRuntimePorts:
    """由 Bootstrap 注入 Task Runtime 的数据库模型工厂与 UoW。

    Attributes:
        uow_factory: Unit of Work 工厂。
        task_execution_factory: TaskExecution ORM 模型工厂。
        outbox_event_factory: OutboxEvent ORM 模型工厂。
        inbox_event_factory: InboxEvent ORM 模型工厂。
    """

    uow_factory: Callable[[], Any]
    task_execution_factory: Callable[..., Any]
    outbox_event_factory: Callable[..., Any]
    inbox_event_factory: Callable[..., Any]
