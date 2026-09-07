"""Task Runtime 命令、快照与执行结果 DTO 定义。

定义 Task Claim 领取快照、补偿恢复快照、Executor 执行上下文与最终执行结果模型。
"""

from typing import Any, Literal
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ClaimNextTaskInput(BaseModel):
    """Claim 下一个可执行任务的输入命令。"""

    plan_id: str = Field(min_length=1, max_length=100, description="目标 Plan ID")


class TaskSnapshot(BaseModel):
    """成功 Claim 到的任务不可变执行快照。"""

    task_id: str = Field(description="任务全局唯一 ID")
    plan_id: str = Field(description="所属 Plan ID")
    workflow_id: str = Field(description="所属工作流 ID")
    conversation_id: str = Field(description="会话 ID")
    turn_id: str = Field(description="所属 Turn ID")
    capability_code: str = Field(description="目标领域能力编码")
    input_json: dict[str, Any] = Field(description="任务输入参数字典")
    sequence: int = Field(description="任务执行序号")
    attempt: int = Field(description="本次执行尝试序号（从 1 开始）")
    max_attempts: int = Field(description="允许的最大尝试次数")
    execution_id: str = Field(description="本次执行尝试生成的 execution_id")
    operation_id: str = Field(description="本次执行操作生成的 operation_id（ownership token）")
    agent_run_id: str = Field(description="关联的 Agent Run ID")
    executor_code: str = Field(description="绑定的 Executor 编码")


class RecoverySnapshot(BaseModel):
    """需要执行补偿恢复的历史失败/超时任务快照。"""

    task_id: str = Field(description="任务全局唯一 ID")
    plan_id: str = Field(description="所属 Plan ID")
    workflow_id: str = Field(description="所属工作流 ID")
    conversation_id: str = Field(description="会话 ID")
    turn_id: str = Field(description="所属 Turn ID")
    capability_code: str = Field(description="目标领域能力编码")
    input_json: dict[str, Any] = Field(description="任务输入参数字典")
    attempt: int = Field(description="待补偿的尝试序号")
    max_attempts: int = Field(description="允许的最大尝试次数")
    execution_id: str = Field(description="待补偿的 execution_id")
    operation_id: str = Field(description="待补偿的 operation_id（ownership token）")
    agent_run_id: str = Field(description="待补偿关联的 Agent Run ID")


class ClaimNextTaskResult(BaseModel):
    """ClaimNextTask 操作的返回结果。"""

    outcome: Literal[
        "claimed",
        "compensation_required",
        "compensation_locked",
        "already_running",
        "no_task",
        "terminal",
    ] = Field(description="Claim 结果分类")
    task: TaskSnapshot | None = Field(default=None, description="新领取的任务快照（当 outcome=claimed 时有效）")
    defer_until: datetime | None = None
    recovery: RecoverySnapshot | None = Field(
        default=None,
        description="待补偿恢复的任务快照（当 outcome=compensation_required 或 compensation_locked 时有效）",
    )


class TaskRuntimeContext(BaseModel):
    """注入给 TaskExecutor 和 OperationCompensator 的执行上下文。"""

    model_config = ConfigDict(frozen=True)

    workflow_id: str = Field(description="工作流 ID")
    plan_id: str = Field(description="Plan ID")
    task_id: str = Field(description="Task ID")
    conversation_id: str = Field(description="会话 ID")
    turn_id: str = Field(description="Turn ID")
    execution_id: str = Field(description="本次执行尝试 ID")
    operation_id: str = Field(description="本次执行操作 ID / ownership token")
    agent_run_id: str = Field(description="Agent 运行追踪 ID")
    attempt: int = Field(description="当前尝试次数")


class TaskExecutorResult(BaseModel):
    """TaskExecutor 执行完成后的标准输出结果。"""

    output_json: dict[str, Any] = Field(description="业务产出 JSON 字典")
    resource_refs: list[str] = Field(
        default_factory=list,
        description="本次执行涉及或产生的资源引用（如 document:123）",
    )


class ExecutePlanResult(BaseModel):
    """执行单个 Plan 驱动步进（execute_next）后的最终结果。"""

    plan_id: str = Field(description="所属 Plan ID")
    defer_until: datetime | None = None
    outcome: Literal[
        "task_succeeded",
        "compensation_retry_scheduled",
        "compensation_locked",
        "retry_scheduled",
        "replan_requested",
        "already_running",
        "no_task",
        "terminal",
    ] = Field(description="执行步进结果状态")
    task_id: str | None = Field(default=None, description="本次处理的 Task ID（若适用）")
    execution_id: str | None = Field(default=None, description="本次处理的 Execution ID（若适用）")
