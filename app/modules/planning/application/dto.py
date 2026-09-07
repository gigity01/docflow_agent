"""Planning Use Case 的显式输入与输出 DTO 定义。

包含 Plan 创建、Task 创建、Plan 校验发布、澄清标记、Replan 触发及规划运行等
应用层输入输出数据传输对象。
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.modules.context.domain.models import ContextChain
from app.modules.planning.domain.enums import PlanStatus


class _PlanningInput(BaseModel):
    """Planning 所有输入 DTO 的基类，禁止未定义字段。"""

    model_config = ConfigDict(extra="forbid")


class CreatePlanInput(_PlanningInput):
    """创建 Plan 的用例输入参数。"""

    turn_id: str = Field(min_length=1, max_length=100, description="关联的 Conversation Turn ID")
    revision: int = Field(default=1, ge=1, description="Plan 修订版本号，从 1 开始")
    workflow_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
        description="关联的工作流 ID（未指定时自动生成）",
    )
    parent_plan_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
        description="上一版本 Plan ID（Replan 场景下传入）",
    )


class _CreateDocumentTaskInput(_PlanningInput):
    """创建文档领域 Task 的通用参数基类。"""

    plan_id: str = Field(min_length=1, max_length=100, description="所属 Plan ID")
    turn_id: str = Field(min_length=1, max_length=100, description="所属 Turn ID")
    document_id: int = Field(gt=0, description="目标文档 ID")
    sequence: int = Field(ge=1, description="Plan 内任务序号")
    task_ref: str = Field(min_length=1, max_length=100, description="任务在 Plan 内的唯一引用标识")
    depends_on_task_refs: list[str] = Field(
        default_factory=list,
        max_length=10,
        description="当前任务依赖的前置 task_ref 列表",
    )


class CreateProcessDocumentTaskInput(_CreateDocumentTaskInput):
    """创建文档处理（Process Document）Task 的输入参数。"""


class CreateBuildChunksTaskInput(_CreateDocumentTaskInput):
    """创建文档切块（Build Document Chunks）Task 的输入参数。"""


class CreateIndexVectorsTaskInput(_CreateDocumentTaskInput):
    """创建文档向量索引（Index Document Vectors）Task 的输入参数。"""


class FinalizePlanInput(_PlanningInput):
    """发布并确认 Plan 的用例输入参数。"""

    plan_id: str = Field(min_length=1, max_length=100, description="待发布的 Plan ID")
    turn_id: str = Field(min_length=1, max_length=100, description="所属 Turn ID")


class MarkPlanUnsupportedInput(_PlanningInput):
    """将 Plan 标记为不支持（Unsupported）的输入参数。"""

    plan_id: str = Field(min_length=1, max_length=100, description="目标 Plan ID")
    reason: str = Field(min_length=1, max_length=4000, description="不支持原因说明")


class MarkPlanRetryPendingInput(_PlanningInput):
    """将 Plan 标记为待重试（Retry Pending）的输入参数。"""

    plan_id: str = Field(min_length=1, max_length=100, description="目标 Plan ID")
    reason: str = Field(min_length=1, max_length=4000, description="触发重试的原因说明")


class MarkPlanNeedsClarificationInput(_PlanningInput):
    """将 Plan 标记为需要用户澄清（Needs Clarification）的输入参数。"""

    plan_id: str = Field(min_length=1, max_length=100, description="目标 Plan ID")
    conversation_id: str = Field(min_length=1, max_length=100, description="会话 ID")
    kind: str = Field(
        pattern=r"^(resource|intent|missing_parameter)$",
        description="澄清类型：资源歧义、意图不明或缺少必要参数",
    )
    reason: str = Field(min_length=1, max_length=4000, description="需要澄清的详细原因")
    required_information: list[str] = Field(
        min_length=1,
        max_length=10,
        description="需要用户补充的信息列表",
    )
    known_resource_refs: list[str] = Field(
        default_factory=list,
        max_length=50,
        description="当前已知的资源引用列表",
    )


class SetClarificationQuestionInput(_PlanningInput):
    """设置澄清问题的输入参数。"""

    plan_id: str = Field(min_length=1, max_length=100, description="目标 Plan ID")
    question: str = Field(min_length=1, max_length=4000, description="向用户提出的具体澄清问题文本")


class PlanResult(BaseModel):
    """Plan 实体对象的应用层输出模型。"""

    model_config = ConfigDict(from_attributes=True)

    plan_id: str
    workflow_id: str
    turn_id: str
    parent_plan_id: str | None
    current_task_id: str | None
    status: str
    revision: int
    failure_code: str | None
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime


class TaskResult(BaseModel):
    """Task 实体对象的应用层输出模型。"""

    model_config = ConfigDict(from_attributes=True)

    task_id: str
    plan_id: str
    turn_id: str
    task_ref: str
    capability_code: str
    input_json: dict
    sequence: int
    status: str
    attempt_count: int
    max_attempts: int
    created_at: datetime
    updated_at: datetime


class FinalizePlanResult(BaseModel):
    """FinalizePlan 用例的执行结果。"""

    plan_id: str
    turn_id: str
    plan_status: str
    task_ids: list[str]


class RunPlanningInput(_PlanningInput):
    """运行 Planner 规划用例的输入参数。"""

    conversation_id: str = Field(min_length=1, max_length=100, description="所属会话 ID")
    turn_id: str = Field(min_length=1, max_length=100, description="目标 Turn ID")
    revision: int = Field(default=1, ge=1, description="Plan 修订版本号")
    workflow_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
        description="关联工作流 ID",
    )
    parent_plan_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
        description="前置父 Plan ID",
    )


class PlannerContextInput(_PlanningInput):
    """Planner 主输入；当前请求与历史读取集合严格分离。"""

    current_user_input: str = Field(min_length=1, description="组合后的当前用户输入与澄清输入")
    context_chains: list[ContextChain] = Field(
        default_factory=list,
        description="关联上下文链及热资源队列快照",
    )


class RunPlanningResult(BaseModel):
    """Planner 规划执行完成后的最终结果。"""

    plan_id: str
    turn_id: str
    status: PlanStatus
    task_ids: list[str]
    failure_reason: str | None
    clarification_question: str | None = None
    clarification_id: str | None = None
    clarification_round: int | None = None
