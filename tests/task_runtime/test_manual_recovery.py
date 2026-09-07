"""人工恢复保留成功结果和归属，只重新开放有限重试。"""

from datetime import timedelta
import unittest
from types import SimpleNamespace
import httpx
from fastapi import FastAPI

from task_runtime import test_task_runtime as fixture
from app.modules.clarification.infrastructure.persistence.models import ClarificationRequest
from app.modules.task_runtime.application.recover_plan import RecoverPlanUseCase
from app.modules.conversation.application.get_turn_status import GetTurnStatusUseCase
from app.modules.messaging.application.delivery import RecoveryError
from app.shared.time import utc_now
from app.bootstrap.dependencies import get_container
from app.modules.messaging.application.delivery import MessageDeliveryService
from app.modules.messaging.presentation.router import router as recovery_router
from app.modules.conversation.presentation.router import router as conversation_router


class ManualRecoveryTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = fixture.TaskRuntimeTest()
        await self.fixture.asyncSetUp()
        self.sessions = self.fixture.session_factory
        ClarificationRequest.__table__.create(self.fixture.engine)
        self.recover = RecoverPlanUseCase(uow_factory=lambda: fixture.SQLAlchemyUnitOfWork(self.sessions),
            outbox_factory=fixture.OutboxEvent, capabilities=fixture.build_capability_registry())

    async def asyncTearDown(self):
        ClarificationRequest.__table__.drop(self.fixture.engine)
        await self.fixture.asyncTearDown()

    async def test_locked_compensation_keeps_operation_and_resumes_compensation_first(self):
        claimed = self.fixture.runtime.claim_next_task(fixture.ClaimNextTaskInput(plan_id="plan-runtime"))
        with self.sessions() as session:
            execution = session.get(fixture.TaskExecution, claimed.task.execution_id)
            execution.status = "compensation_locked"
            execution.compensation_attempt_count = 5
            execution.error_code = "external_unavailable"
            execution.error_message = "外部服务不可用"
            execution.retryable = True
            execution.compensation_last_error = "清理失败"
            document = session.get(fixture.Document, 1)
            document.active_operation_id = claimed.task.operation_id
            session.commit()
        result = self.recover.execute("plan-runtime")
        self.assertEqual(result["status"], "compensation_queued")
        with self.sessions() as session:
            execution = session.get(fixture.TaskExecution, claimed.task.execution_id)
            self.assertEqual(execution.status, "compensation_required")
            self.assertEqual(execution.operation_id, claimed.task.operation_id)
            self.assertEqual(session.get(fixture.Document, 1).active_operation_id, claimed.task.operation_id)
        resumed = await self.fixture.runtime.execute_next("plan-runtime")
        self.assertEqual(resumed.outcome, "retry_scheduled")
        self.assertEqual(len(self.fixture.compensator.calls), 1)
        with self.sessions() as session:
            self.assertEqual(session.query(fixture.TaskExecution).count(), 1)
            self.assertEqual(session.get(fixture.Task, "task-1").attempt_count, 1)

    async def test_completed_plan_returns_existing_result_without_new_wakeup(self):
        await self.fixture.runtime.execute_next("plan-runtime")
        await self.fixture.runtime.execute_next("plan-runtime")
        with self.sessions() as session:
            before = session.query(fixture.OutboxEvent).count()
        self.assertEqual(self.recover.execute("plan-runtime")["status"], "completed")
        with self.sessions() as session:
            self.assertEqual(session.query(fixture.OutboxEvent).count(), before)
        result = GetTurnStatusUseCase(lambda: fixture.SQLAlchemyUnitOfWork(self.sessions)).execute(
            "conversation-runtime", "turn-runtime")
        self.assertEqual(len(result.tasks), 2)
        self.assertTrue(all(task["result"] for task in result.tasks))

    async def test_failed_plan_retries_only_unfinished_task_with_new_bounded_budget(self):
        await self.fixture.runtime.execute_next("plan-runtime")
        with self.sessions() as session:
            plan = session.get(fixture.Plan, "plan-runtime")
            plan.status = "failed"
            task = session.get(fixture.Task, "task-2")
            task.status, task.attempt_count = "failed", 3
            session.commit()
        self.recover.execute("plan-runtime")
        self.recover.execute("plan-runtime")
        with self.sessions() as session:
            self.assertEqual(session.get(fixture.Task, "task-1").status, "succeeded")
            task = session.get(fixture.Task, "task-2")
            self.assertEqual((task.attempt_count, task.max_attempts), (3, 6))
        result = await self.fixture.runtime.execute_next("plan-runtime")
        self.assertEqual(result.task_id, "task-2")
        with self.sessions() as session:
            self.assertEqual(session.get(fixture.Task, "task-1").attempt_count, 1)
            self.assertEqual(session.get(fixture.Task, "task-2").attempt_count, 4)

    async def test_running_plan_recovery_waits_until_lease(self):
        claimed = self.fixture.runtime.claim_next_task(fixture.ClaimNextTaskInput(plan_id="plan-runtime"))
        result = self.recover.execute("plan-runtime")
        self.assertEqual(result["status"], "waiting_for_lease")
        with self.sessions() as session:
            self.assertEqual(session.get(fixture.Task, claimed.task.task_id).attempt_count, 1)
            self.assertEqual(session.get(fixture.TaskExecution, claimed.task.execution_id).status, "running")

    async def test_http_query_and_recovery_expose_actionable_state(self):
        app = FastAPI()
        app.include_router(recovery_router, prefix="/api")
        app.include_router(conversation_router, prefix="/api")
        uow = lambda: fixture.SQLAlchemyUnitOfWork(self.sessions)
        container = SimpleNamespace(recover_plan=self.recover,
            get_turn_status=GetTurnStatusUseCase(uow),
            message_delivery=MessageDeliveryService(uow_factory=uow,
                inbox_factory=fixture.InboxEvent, outbox_factory=fixture.OutboxEvent))
        app.dependency_overrides[get_container] = lambda: container
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/conversations/conversation-runtime/turns/turn-runtime")
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(len(response.json()["tasks"]), 2)
            self.assertTrue(response.json()["next_action"])
            response = await client.get("/api/admin/runtime/failures")
            self.assertEqual(response.json(), [])
            response = await client.post("/api/admin/runtime/plans/missing/recover")
            self.assertEqual(response.status_code, 404)
            response = await client.post("/api/admin/runtime/plans/plan-runtime/recover")
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["status"], "queued")
