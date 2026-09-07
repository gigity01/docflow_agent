"""Conversation Message HTTP 路由端点定义。"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Response

from app.modules.clarification.application.errors import (
    ClarificationApplicationError,
)
from app.modules.context.application.errors import (
    ContextApplicationError,
    ContextConflictError,
    ContextRoutingError,
    ContextTurnNotFoundError,
    ContextValidationError,
    ConversationLockUnavailable,
)
from app.modules.conversation.application.dto import (
    SendConversationMessageCommand,
)
from app.modules.conversation.presentation.dependencies import (
    get_send_conversation_message,
    get_turn_status,
)
from app.modules.conversation.presentation.schemas import (
    SendMessageRequest,
    SendMessageResponse,
    TurnStatusResponse,
)
from app.modules.planning.application.errors import PlanningApplicationError


router = APIRouter(prefix="/conversations", tags=["conversations"])


def _context_status_code(exc: ContextApplicationError) -> int:
    """将 Context 模块领域/应用层异常映射为对应的 HTTP 状态码。

    Args:
        exc: ContextApplicationError 实例。

    Returns:
        int: 对应的 HTTP 状态码。
    """
    if isinstance(exc, ContextRoutingError):
        return 502
    if isinstance(exc, ContextTurnNotFoundError):
        return 404
    if isinstance(exc, (ContextConflictError, ConversationLockUnavailable)):
        return 409
    if isinstance(exc, ContextValidationError):
        return 400
    return 500


@router.post("/{conversation_id}/messages", response_model=SendMessageResponse)
async def send_conversation_message(
    conversation_id: Annotated[str, Path(min_length=1, max_length=100)],
    request: SendMessageRequest,
    response: Response,
    use_case=Depends(get_send_conversation_message),
) -> SendMessageResponse:
    """接收用户消息或针对澄清提问的回复，执行上下文路由与规划。

    响应状态码规则：
    - HTTP 202 Accepted: 规划成功并已发布异步 Plan（status 为 processing 或 retry_pending），客户端需通过 Turn 查询接口轮询结果。
    - HTTP 200 OK: 即时返回非异步终态（needs_clarification、unsupported 或 failed）。
    - HTTP 400/404/409/502: 对应的参数校验、资源不存在、并发锁冲突或外部路由失败异常。

    Args:
        conversation_id: 路径参数，会话唯一标识。
        request: 请求体，包含 message 与可选的 clarification_id。
        response: FastAPI 响应对象，用于动态设置 202 状态码。
        use_case: 依赖注入的 SendConversationMessageUseCase 实例。

    Returns:
        SendMessageResponse: 会话交互响应结果。

    Raises:
        HTTPException: 对应各种应用层业务异常的 HTTP 映射。
    """
    try:
        result = await use_case.execute(
            SendConversationMessageCommand(
                conversation_id=conversation_id,
                message=request.message,
                clarification_id=request.clarification_id,
            )
        )
    except ContextApplicationError as exc:
        raise HTTPException(
            status_code=_context_status_code(exc),
            detail=str(exc),
        ) from exc
    except PlanningApplicationError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    except ClarificationApplicationError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    # 对于异步执行中的状态，返回 HTTP 202 Accepted
    if result.status in {"processing", "retry_pending"}:
        response.status_code = 202
    return SendMessageResponse.model_validate(result.model_dump())


@router.get(
    "/{conversation_id}/turns/{turn_id}",
    response_model=TurnStatusResponse,
)
def get_conversation_turn_status(
    conversation_id: Annotated[str, Path(min_length=1, max_length=100)],
    turn_id: Annotated[str, Path(min_length=1, max_length=100)],
    use_case=Depends(get_turn_status),
) -> TurnStatusResponse:
    """查询指定 Turn 的当前状态、关联的最新 Plan Revision 及任务执行结果。

    Args:
        conversation_id: 路径参数，会话唯一标识。
        turn_id: 路径参数，轮次唯一标识。
        use_case: 依赖注入的 GetTurnStatusUseCase 实例。

    Returns:
        TurnStatusResponse: Turn 执行状态响应模型。

    Raises:
        HTTPException: 当 Turn 不存在时抛出 404。
    """
    try:
        result = use_case.execute(conversation_id, turn_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return TurnStatusResponse.model_validate(result.model_dump())
