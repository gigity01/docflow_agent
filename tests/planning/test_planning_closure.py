"""澄清循环与 Planner 中断恢复必须有限且可继续。"""

from datetime import timedelta
import unittest

from planning import test_replan_clarification as fixture
from app.modules.messaging.application.dto import DeferredEvent
from app.modules.planning.application.dto import MarkPlanNeedsClarificationInput
from app.modules.planning.application.use_cases import MarkPlanNeedsClarificationUseCase
from app.modules.planning.application.recovery_policy import PLANNER_RUN_LEASE_SECONDS
from app.modules.conversation.application.get_turn_status import GetTurnStatusUseCase
from app.shared.time import utc_now


class PlanningClosureTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = fixture.ReplanClarificationTest()
        await self.fixture.asyncSetUp()
        self.sessions = self.fixture.session_factory
        self.uow = lambda: fixture.SQLAlchemyUnitOfWork(self.sessions)
        self.ports = fixture.PlanningApplicationPorts(uow_factory=self.uow,
            plan_factory=fixture.Plan, task_factory=fixture.Task,
            task_dependency_factory=fixture.TaskDependency, outbox_event_factory=fixture.OutboxEvent,
            inbox_event_factory=fixture.InboxEvent, clarification_request_factory=fixture.ClarificationRequest,
            integrity_error_type=fixture.IntegrityError)
        self.runner = fixture._RunPlanning()
        self.replan = fixture.ReplanUseCase(ports=self.ports, run_planning=self.runner)
        self.answer = fixture.AnswerClarificationUseCase(uow_factory=self.uow, outbox_event_factory=fixture.OutboxEvent)
        self.mark = MarkPlanNeedsClarificationUseCase(ports=self.ports)

    async def asyncTearDown(self):
        await self.fixture.asyncTearDown()

    async def answer_and_replan(self, clarification_id, answer):
        self.answer.execute(conversation_id="conversation-1", clarification_id=clarification_id, answer=answer)
        with self.sessions() as session:
            events = session.query(fixture.OutboxEvent).filter_by(event_type="planning.replan_requested").all()
            record = [item for item in events if item.payload_json["trigger_type"] == "clarification_answered"][-1]
            event = fixture.ReplanRequested(event_id=record.event_id, **record.payload_json)
        result = await self.replan.execute(event)
        return result, event

    def ask(self, plan_id):
        return self.mark.execute(MarkPlanNeedsClarificationInput(
            plan_id=plan_id, conversation_id="conversation-1", kind="missing_parameter",
            reason="目标仍不明确", required_information=["target"], known_resource_refs=[]))

    async def test_three_clarifications_preserve_answers_and_do_not_spend_system_retries(self):
        clarification_id = "clarification-1"
        for number in range(1, 4):
            result, event = await self.answer_and_replan(clarification_id, f"answer-{number}")
            with self.sessions() as session:
                plan = session.get(fixture.Plan, result.plan_id)
                self.assertEqual(plan.system_retry_count, 0)
                self.assertEqual(plan.revision, number + 1)
                turn = session.get(fixture.ConversationTurn, "turn-question")
                for previous in range(1, number + 1):
                    self.assertIn(f"answer-{previous}", turn.clarification_input)
            self.ask(result.plan_id)
            with self.sessions() as session:
                if number < 3:
                    request = session.query(fixture.ClarificationRequest).filter_by(status="open").one()
                    clarification_id = request.clarification_id
                    self.assertEqual(request.round, number + 1)
                else:
                    self.assertEqual(session.query(fixture.ClarificationRequest).filter_by(status="open").count(), 0)
                    self.assertEqual(session.get(fixture.Plan, result.plan_id).status, "failed")
                    turn = session.get(fixture.ConversationTurn, "turn-question")
                    self.assertEqual(turn.status, "failed")
                    self.assertIn("target", turn.assistant_content)
        with self.assertRaises(fixture.ClarificationApplicationError):
            self.answer.execute(conversation_id="conversation-1", clarification_id="clarification-1", answer="old duplicate")

    async def test_committed_revision_without_completed_planner_defers_then_recovers_once(self):
        first, event = await self.answer_and_replan("clarification-1", "Document 7")
        with self.assertRaises(DeferredEvent):
            await self.replan.execute(event)
        self.assertEqual(len(self.runner.calls), 1)
        with self.sessions() as session:
            plan = session.get(fixture.Plan, first.plan_id)
            plan.updated_at = utc_now() - timedelta(seconds=PLANNER_RUN_LEASE_SECONDS + 1)
            session.commit()
        resumed = await self.replan.execute(event)
        self.assertEqual(self.runner.calls[-1][0].parent_plan_id, first.plan_id)
        with self.sessions() as session:
            old, new = session.get(fixture.Plan, first.plan_id), session.get(fixture.Plan, resumed.plan_id)
            self.assertEqual(old.status, "superseded")
            self.assertEqual(new.revision, 3)
            self.assertEqual(new.system_retry_count, 1)
            new.status = "ready"
            session.commit()
            watchdogs = [row for row in session.query(fixture.OutboxEvent).all()
                         if row.payload_json.get("trigger_type") == "planner_timeout"]
            self.assertEqual(len(watchdogs), 2)
        self.assertIsNone(await self.replan.execute(event))
        self.assertEqual(len(self.runner.calls), 2)

    async def test_wrong_event_revision_does_not_create_a_plan(self):
        event = fixture.ReplanRequested(event_id="wrong-revision", workflow_id="workflow-1",
            conversation_id="conversation-1", root_turn_id="turn-question",
            previous_plan_id="plan-question", next_revision=99, trigger_type="manual_retry")
        with self.assertRaises(ValueError):
            await self.replan.execute(event)
        with self.sessions() as session:
            self.assertEqual(session.query(fixture.Plan).count(), 1)

    async def test_system_retries_stop_with_reason_without_another_open_plan(self):
        result, _ = await self.answer_and_replan("clarification-1", "Document 7")
        with self.sessions() as session:
            plan = session.get(fixture.Plan, result.plan_id)
            plan.status = "retry_pending"
            plan.system_retry_count = 2
            session.commit()
        event = fixture.ReplanRequested(event_id="system-retry-limit", workflow_id="workflow-1",
            conversation_id="conversation-1", root_turn_id="turn-question", previous_plan_id=result.plan_id,
            next_revision=3, trigger_type="planner_failed")
        self.assertIsNone(await self.replan.execute(event))
        with self.sessions() as session:
            self.assertEqual(session.query(fixture.Plan).count(), 2)
            plan = session.get(fixture.Plan, result.plan_id)
            self.assertEqual(plan.status, "failed")
            self.assertEqual(plan.failure_code, "system_retry_limit_exceeded")
