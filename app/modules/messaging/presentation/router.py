"""运行时失败查询及有状态恢复，接口限定在本机开发服务范围。"""

from fastapi import APIRouter, Depends, HTTPException, Query

from app.bootstrap.dependencies import get_container
from app.modules.messaging.application.delivery import FailureView, RecoveryError

router = APIRouter(prefix="/admin/runtime", tags=["runtime-recovery"])


@router.get("/failures", response_model=list[FailureView])
def list_failures(plan_id: str | None = None, limit: int = Query(50, ge=1, le=100),
                  offset: int = Query(0, ge=0), container=Depends(get_container)):
    return container.message_delivery.list_failures(plan_id=plan_id, limit=limit, offset=offset)


@router.post("/failures/{failure_id}/retry")
def retry_message(failure_id: str, container=Depends(get_container)):
    try:
        return container.message_delivery.retry(failure_id)
    except RecoveryError as exc:
        raise HTTPException(exc.status_code, detail=str(exc)) from exc


@router.post("/plans/{plan_id}/recover")
def recover_plan(plan_id: str, container=Depends(get_container)):
    try:
        return container.recover_plan.execute(plan_id)
    except RecoveryError as exc:
        raise HTTPException(exc.status_code, detail=str(exc)) from exc
