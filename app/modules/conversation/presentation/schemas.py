"""Conversation HTTP API 请求与响应 Schema 定义。"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from app.modules.conversation.application.dto import ContextSelectionMetadata


class SendMessageRequest(BaseModel):
    """发送会话消息 HTTP 请求体 Schema。

    Attributes:
        message: 用户输入的原始文本（或对澄清问题的回复）。
        clarification_id: 若当前消息为澄清回复，则携带当前澄清请求 ID；普通消息为 None。
    """

    model_config = ConfigDict(extra="forbid")

    message: str = Field(min_length=1, description="用户消息文本")
    clarification_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
        description="当前待回答的澄清请求 ID",
    )


class SendMessageResponse(BaseModel):
    """发送会话消息 HTTP 响应体 Schema。

    Attributes:
        conversation_id: 会话唯一标识。
        turn_id: 本轮交互对应的 Turn ID。
        plan_id: 生成或关联的 Plan 唯一标识。
        status: 处理状态（processing / unsupported / needs_clarification / retry_pending / completed / failed）。
        assistant_message: 即时助手文本或错误/澄清提问内容。
        task_ids: 规划生成的 Task 标识列表。
        context_selection: 上下文路由选择元数据。
    """

    conversation_id: str = Field(description="会话唯一标识")
    turn_id: str = Field(description="Turn 唯一标识")
    plan_id: str | None = Field(default=None, description="Plan 唯一标识")
    status: Literal[
        "processing",
        "unsupported",
        "needs_clarification",
        "retry_pending",
        "completed",
        "failed",
    ] = Field(description="交互处理状态")
    assistant_message: str | None = Field(default=None, description="助手回复内容或澄清问题")
    task_ids: list[str] = Field(default_factory=list, description="包含的 Task ID 列表")
    clarification_id: str | None = None
    clarification_round: int | None = None
    context_selection: ContextSelectionMetadata | None = Field(
        default=None, description="上下文路由选择元数据"
    )


class TurnStatusResponse(BaseModel):
    """查询 Turn 状态 HTTP 响应体 Schema。

    Attributes:
        conversation_id: 会话唯一标识。
        turn_id: 查询的 Turn ID。
        turn_status: Turn 生命周期状态。
        plan_id: 关联的最新 Plan 唯一标识。
        plan_status: 关联的最新 Plan 状态。
        revision: 最新 Plan 修订版本号。
        task_ids: Plan 包含的 Task 标识列表。
        assistant_message: 最终完成或失败时的助手文本。
    """

    conversation_id: str = Field(description="会话唯一标识")
    turn_id: str = Field(description="Turn 唯一标识")
    turn_status: str = Field(description="Turn 生命周期状态")
    plan_id: str | None = Field(default=None, description="最新 Plan ID")
    plan_status: str | None = Field(default=None, description="最新 Plan 状态")
    revision: int | None = Field(default=None, description="最新 Plan 版本号")
    task_ids: list[str] = Field(default_factory=list, description="关联的任务 ID 列表")
    assistant_message: str | None = Field(default=None, description="最终助手回答文本")
    failure_code: str | None = None
    failure_reason: str | None = None
    next_action: str | None = None
    tasks: list[dict] = Field(default_factory=list)
    message_failures: list[dict] = Field(default_factory=list)
    clarifications: list[dict] = Field(default_factory=list)
