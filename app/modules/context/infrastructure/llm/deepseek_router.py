"""基于 DeepSeek strict Tool 的 Context Selector。"""

from __future__ import annotations

from time import monotonic_ns
from typing import Any

from pydantic import ValidationError

from app.agent_runtime.business_docs import load_service_map
from app.infrastructure.llm.deepseek.provider import DeepSeekModelProvider
from app.modules.context.application.dto import ContextAgentInput
from app.modules.context.domain.models import ContextSelectionDecision
from app.modules.context.infrastructure.llm.strict_schema_adapter import (
    ContextAgentOutputError,
    build_context_selection_tool_schema,
)


CONTEXT_SELECTION_TOOL_NAME = "submit_context_selection"
DEFAULT_CONTEXT_AGENT_OUTPUT_ATTEMPTS = 2
SERVICE_MAP = load_service_map()

CONTEXT_AGENT_INSTRUCTIONS = f"""
你是 Context Selection Agent。

你的唯一职责是选择 Planner 为正确理解并处理当前用户请求所需要读取的历史
ContextChain。选择的是 Planner 所需的历史信息，不是“当前消息最像哪条 Chain”。

以下 Service Map 只帮助理解请求所属业务语境，不得因此扩大 Context Selection 的
授权读取范围：

{SERVICE_MAP}

规则：

1. relevant_chain_ids 是 Context Read Set，可以选择 0、1 或多条已有 Chain。
2. 不需要已有历史信息时返回空列表。
3. 当前请求涉及多个历史背景时必须返回所有必要 Chain。
4. 不要因为存在一条“主要 Chain”就排除其他必要 Chain。
5. 不得拆分、改写或摘要当前用户输入。
6. ResourceQueue 是关联证据，但不是当前 Turn 最终归属的依据；资源只出现在某条
   Chain 中，也不能仅凭这一事实强制选择该 Chain。
7. 不得创建 Chain，也不得决定当前 Turn 最终归属。
8. 不得生成计划、任务、操作、权限或执行建议。
9. 不得修改链内容、资源、归档状态或时间戳。
10. 必须且只能调用 submit_context_selection 提交结构化选择结果，
   不得输出普通文本。
11. resource_queue 按从旧到新排列，越靠近队尾表示最近越活跃；
    队列只用于判断上下文关联，不得据此生成业务操作。

reason_summary 只能简要说明上下文关联依据，不得包含后续业务计划或执行建议。
""".strip()


def _empty_selection_without_model() -> ContextSelectionDecision:
    """当会话没有任何历史未归档链时，直接返回空读取集合，避免无意义的 LLM 网络调用。

    Returns:
        relevant_chain_ids 为空的决策对象。
    """
    return ContextSelectionDecision(
        relevant_chain_ids=[],
        reason_summary="当前 Conversation 没有历史上下文。",
    )


class DeepSeekContextRouter:
    """通过 DeepSeek strict tool call 返回结构化历史读取集合的路由器实现。

    设计约束：
    - 使用 strict tool call（submit_context_selection）强制结构化输出，禁用自然语言回复。
    - 禁用 reasoning/thinking，设置 temperature=0 确保判定确定性与低延迟。
    - 在输出格式不符合 strict 契约时最多重试 max_output_attempts 次。
    """

    def __init__(
        self,
        provider: DeepSeekModelProvider,
        *,
        max_output_attempts: int = DEFAULT_CONTEXT_AGENT_OUTPUT_ATTEMPTS,
        event_logger: Any | None = None,
    ) -> None:
        """初始化 DeepSeekContextRouter。

        Args:
            provider: DeepSeekModelProvider 实例。
            max_output_attempts: 最大输出重试尝试次数（默认 2 次）。
            event_logger: 可观测性事件日志记录器。

        Raises:
            ValueError: 当 max_output_attempts 小于 1 时抛出。
        """
        if max_output_attempts < 1:
            raise ValueError("max_output_attempts must be at least 1")
        self._client = provider.strict_tool_client
        self._model_name = provider.model_name
        self._tool_schema = build_context_selection_tool_schema()
        self._max_output_attempts = max_output_attempts
        self._event_logger = event_logger

    async def route(
        self,
        agent_input: ContextAgentInput,
    ) -> ContextSelectionDecision:
        """调用 DeepSeek 返回结构化的历史读取集合决策；若无历史 Chain 则直接短路返回。

        Args:
            agent_input: ContextAgentInput 包含当前用户输入与候选未归档链。

        Returns:
            结构化路由选择决策。

        Raises:
            ContextAgentOutputError: 多次尝试后依然未能获得合法 strict tool 调用结果。
        """
        # 1. 若当前会话无任何已有链，直接短路返回，无需调用模型
        if not agent_input.chains:
            self._observe(
                "context_selection_llm_skipped",
                conversation_id=agent_input.conversation_id,
                turn_id=agent_input.current_turn_id,
                context_selection_llm_duration=0.0,
                context_selection_retry_count=0,
                duration_unit="milliseconds",
            )
            return _empty_selection_without_model()

        started_at = monotonic_ns()
        last_error: ContextAgentOutputError | None = None
        # 2. 循环重试执行 strict tool 调用
        for attempt in range(self._max_output_attempts):
            try:
                response = await self._client.chat.completions.create(
                    model=self._model_name,
                    messages=self._build_messages(
                        agent_input,
                        retry=attempt > 0,
                    ),
                    tools=[
                        {
                            "type": "function",
                            "function": {
                                "name": CONTEXT_SELECTION_TOOL_NAME,
                                "description": (
                                    "提交唯一的 Context 历史读取集合。"
                                ),
                                "strict": True,
                                "parameters": self._tool_schema,
                            },
                        }
                    ],
                    tool_choice={
                        "type": "function",
                        "function": {
                            "name": CONTEXT_SELECTION_TOOL_NAME,
                        },
                    },
                    parallel_tool_calls=False,
                    temperature=0,
                    max_tokens=512,
                    extra_body={
                        "thinking": {
                            "type": "disabled",
                        }
                    },
                )
            except Exception as exc:
                self._observe(
                    "context_selection_llm_failed",
                    level="error",
                    conversation_id=agent_input.conversation_id,
                    turn_id=agent_input.current_turn_id,
                    context_selection_llm_duration=self._elapsed_ms(
                        started_at
                    ),
                    context_selection_retry_count=attempt,
                    error_type=type(exc).__name__,
                    duration_unit="milliseconds",
                )
                raise

            # 3. 解析并校验模型返回的 strict tool 调用
            try:
                decision = self._parse_response(response)
            except ContextAgentOutputError as exc:
                last_error = exc
                self._observe(
                    "context_selection_invalid_output",
                    level="warning",
                    conversation_id=agent_input.conversation_id,
                    turn_id=agent_input.current_turn_id,
                    attempt=attempt + 1,
                    context_selection_llm_duration=self._elapsed_ms(
                        started_at
                    ),
                    context_selection_invalid_output_count=1,
                    context_selection_will_retry=(
                        attempt + 1 < self._max_output_attempts
                    ),
                    duration_unit="milliseconds",
                )
                continue

            self._observe(
                "context_selection_llm_completed",
                conversation_id=agent_input.conversation_id,
                turn_id=agent_input.current_turn_id,
                context_selection_llm_duration=self._elapsed_ms(started_at),
                context_selection_retry_count=attempt,
                duration_unit="milliseconds",
            )
            return decision

        self._observe(
            "context_selection_llm_failed",
            level="error",
            conversation_id=agent_input.conversation_id,
            turn_id=agent_input.current_turn_id,
            context_selection_llm_duration=self._elapsed_ms(started_at),
            context_selection_retry_count=(
                self._max_output_attempts - 1
            ),
            error_type="ContextAgentOutputError",
            duration_unit="milliseconds",
        )
        raise ContextAgentOutputError(
            "Context Agent 连续返回非法 strict tool 响应"
        ) from last_error

    @staticmethod
    def _elapsed_ms(started_at: int) -> float:
        """计算自 started_at（纳秒）以来的毫秒数。"""
        return round((monotonic_ns() - started_at) / 1_000_000, 3)

    def _observe(self, event: str, **fields: Any) -> None:
        """记录结构化观察事件。"""
        if self._event_logger is None:
            return
        try:
            self._event_logger.write(event, **fields)
        except Exception:
            return

    @staticmethod
    def _build_messages(
        agent_input: ContextAgentInput,
        *,
        retry: bool,
    ) -> list[dict[str, str]]:
        """组装 LLM 提示词消息序列。

        Args:
            agent_input: ContextAgentInput 实例。
            retry: 是否为重试轮次。

        Returns:
            list[dict[str, str]]: 包含 system 和 user 角色的消息列表。
        """
        user_content = agent_input.model_dump_json(indent=2)
        if retry:
            user_content = (
                "上一次响应未形成唯一且合法的 submit_context_selection "
                "调用。请重新判断，并且只能调用该工具。\n\n"
                f"{user_content}"
            )
        return [
            {
                "role": "system",
                "content": CONTEXT_AGENT_INSTRUCTIONS,
            },
            {
                "role": "user",
                "content": user_content,
            },
        ]

    @staticmethod
    def _parse_response(response: Any) -> ContextSelectionDecision:
        """从 ChatCompletion 响应中解析并验证 submit_context_selection 工具调用参数。

        Args:
            response: OpenAI / DeepSeek 响应对象。

        Returns:
            ContextSelectionDecision: 解析出的领域决策模型。

        Raises:
            ContextAgentOutputError: 缺少 choice, message, tool_calls, arguments 或 JSON 校验失败。
        """
        choices = getattr(response, "choices", None)
        if not choices:
            raise ContextAgentOutputError("Context Agent 没有返回 choice")

        message = getattr(choices[0], "message", None)
        if message is None:
            raise ContextAgentOutputError("Context Agent choice 缺少 message")

        tool_calls = getattr(message, "tool_calls", None) or []
        if len(tool_calls) != 1:
            raise ContextAgentOutputError(
                "Context Agent 必须且只能返回一个 Tool Call"
            )

        tool_call = tool_calls[0]
        function = getattr(tool_call, "function", None)
        if function is None:
            raise ContextAgentOutputError(
                "Context Agent Tool Call 缺少 function"
            )
        if function.name != CONTEXT_SELECTION_TOOL_NAME:
            raise ContextAgentOutputError(
                "Context Agent 返回了非预期 Tool: "
                f"{function.name}"
            )

        arguments = getattr(function, "arguments", None)
        if not isinstance(arguments, str) or not arguments.strip():
            raise ContextAgentOutputError(
                "Context Agent Tool Call 缺少 arguments"
            )

        try:
            return ContextSelectionDecision.model_validate_json(arguments)
        except ValidationError as exc:
            raise ContextAgentOutputError(
                "Context Agent Tool 参数不符合 Selection 契约"
            ) from exc
