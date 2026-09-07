"""Conversation 消息命令与应用层数据传输对象（DTO）。"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from app.modules.context.domain.enums import ContextSelectionMode


class SendConversationMessageCommand(BaseModel):
    """发送会话消息的应用层输入命令。

    Attributes:
        conversation_id: 会话唯一标识。
        message: 用户输入的原始文本（或对澄清问题的回复内容）。
        clarification_id: 若当前消息是对先前澄清请求的回答，则携带当前澄清请求 ID；否则为 None。
    """

    model_config = ConfigDict(extra="forbid")

    conversation_id: str = Field(min_length=1, max_length=100)
    message: str = Field(min_length=1)
    clarification_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
    )


class ContextSelectionMetadata(BaseModel):
    """Planner 历史 Context Read Set 选择结果元数据。

    Attributes:
        selection_mode: 派生的选择模式（no_context / single_context / multi_context）。
        relevant_chain_ids: 本次选中的历史上下文链 ID 列表（Read Set）。
        reason_summary: Context Agent 给出的历史读取集合选择依据摘要。
    """

    selection_mode: ContextSelectionMode
    relevant_chain_ids: list[str]
    reason_summary: str


class SendConversationMessageResult(BaseModel):
    """发送会话消息的应用层执行结果。

    Attributes:
        conversation_id: 会话唯一标识。
        turn_id: 本轮交互对应的 Turn ID（若为澄清回答则复用其关联的源 Turn）。
        plan_id: 生成或关联的 Plan 唯一标识。
        status: 当前处理状态（processing / unsupported / needs_clarification / retry_pending / completed / failed）。
        assistant_message: 当处于 needs_clarification、unsupported 或 failed 时的即时助手回复或错误信息。
        task_ids: 规划生成的 Task 标识列表。
        context_selection: 历史 Context Read Set 选择元数据。
    """

    conversation_id: str
    turn_id: str
    plan_id: str | None
    status: Literal[
        "processing",
        "unsupported",
        "needs_clarification",
        "retry_pending",
        "completed",
        "failed",
    ]
    assistant_message: str | None = None
    task_ids: list[str] = Field(default_factory=list)
    clarification_id: str | None = None
    clarification_round: int | None = None
    context_selection: ContextSelectionMetadata | None = None


class TurnStatusResult(BaseModel):
    """Conversation Turn 状态查询结果。

    Attributes:
        conversation_id: 会话唯一标识。
        turn_id: 查询的 Turn 唯一标识。
        turn_status: Turn 生命周期状态（processing / needs_clarification / completed / failed 等）。
        plan_id: 关联的最新 Plan 唯一标识。
        plan_status: 关联的最新 Plan 生命周期状态。
        revision: 最新 Plan 的修订版本号。
        task_ids: Plan 包含的 Task 标识列表。
        assistant_message: 最终完成或失败时的助手文本回复。
    """

    conversation_id: str
    turn_id: str
    turn_status: str
    plan_id: str | None
    plan_status: str | None
    revision: int | None
    task_ids: list[str]
    assistant_message: str | None
    failure_code: str | None = None
    failure_reason: str | None = None
    next_action: str | None = None
    tasks: list[dict] = Field(default_factory=list)
    message_failures: list[dict] = Field(default_factory=list)
    clarifications: list[dict] = Field(default_factory=list)
