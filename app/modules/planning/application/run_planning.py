"""连接用户输入、上下文快照、Planner 执行与计划状态持久化。

应用层准备规划输入和受限工具上下文，调用 PlannerRunner 后读取业务状态，
并处理澄清、重试与失败。任务创建和计划发布由 Planning 用例负责。
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from uuid import uuid4

from app.agent_runtime.audit import AgentToolAuditLogger
from app.agent_runtime.context import (
    AgentToolContext,
    ContextToolServices,
    DocumentToolServices,
    OperationsToolServices,
    PlanningToolServices,
)
from app.modules.context.application.ports import ContextChainMapperPort
from app.modules.context.application.resource_service import (
    ContextResourceService,
)
from app.modules.context.domain.enums import ContextTurnStatus
from app.modules.context.domain.models import ContextChain
from app.modules.planning.application.dto import (
    CreatePlanInput,
    MarkPlanRetryPendingInput,
    PlannerContextInput,
    RunPlanningInput,
    RunPlanningResult,
    SetClarificationQuestionInput,
)
from app.modules.planning.application.errors import (
    PlanningApplicationError,
    PlanningRetryRequested,
)
from app.modules.planning.application.ports import (
    PlannerRunnerPort,
    PlanningApplicationPorts,
)
from app.modules.planning.application.use_cases import PlanningUseCases
from app.modules.planning.domain.enums import PlanStatus, TaskStatus


PLANNER_PERMISSIONS = frozenset(
    {
        "document:read",
        "context:read",
        "operations:read",
        "planning:write",
    }
)
PLANNER_ACTOR_CODE = "planner_agent"
PLANNER_AGENT_NAME = "planner"


@dataclass(frozen=True)
class _PlanningChainSnapshot:
    """规划所需的单条上下文链快照及资源版本。"""

    chain: ContextChain
    resource_version: int


@dataclass(frozen=True)
class _PlannableSnapshot:
    """规划前加载的用户输入与上下文链快照集合。"""

    current_user_input: str
    chains: list[_PlanningChainSnapshot]


def _compose_current_user_input(
    user_input: str,
    clarification_input: str | None,
) -> str:
    """组合原始用户请求与用户对澄清问题的补充回答。

    Args:
        user_input: 原始用户输入。
        clarification_input: 用户对澄清问题的回答内容（若有）。

    Returns:
        拼接后的完整输入文本。
    """
    if clarification_input is None:
        return user_input
    return (
        f"原始用户请求：\n{user_input}\n\n"
        f"用户对澄清问题的补充：\n{clarification_input}"
    )


class RunPlanningUseCase:
    """执行一轮规划，并将模型执行结果收敛为持久化的计划状态。"""

    def __init__(
        self,
        *,
        ports: PlanningApplicationPorts,
        planning_use_cases: PlanningUseCases,
        planner_runner: PlannerRunnerPort,
        document_services: DocumentToolServices,
        context_services: ContextToolServices,
        context_resource_service: ContextResourceService,
        context_chain_mapper: ContextChainMapperPort,
        operations_services: OperationsToolServices,
        audit_logger_factory: Callable[[], AgentToolAuditLogger] = (
            AgentToolAuditLogger
        ),
    ) -> None:
        """初始化 RunPlanningUseCase。

        Args:
            ports: 数据库能力集合。
            planning_use_cases: Planning 领域用例集合。
            planner_runner: LangGraph 编排的 Planner 运行器。
            document_services: 文档领域只读与操作服务。
            context_services: 上下文领域服务。
            context_resource_service: 上下文链热资源缓存与队列服务。
            context_chain_mapper: 上下文链 ORM 到领域模型映射器。
            operations_services: 运维日志查询服务。
            audit_logger_factory: 工具审计日志记录器工厂。
        """
        self._ports = ports
        self._planning_use_cases = planning_use_cases
        self._planner_runner = planner_runner
        self._document_services = document_services
        self._context_services = context_services
        self._context_resource_service = context_resource_service
        self._context_chain_mapper = context_chain_mapper
        self._operations_services = operations_services
        self._audit_logger_factory = audit_logger_factory

    async def execute(self, command: RunPlanningInput) -> RunPlanningResult:
        """执行完整规划用例（新建 Plan revision 并运行 Planner）。

        Args:
            command: 规划输入参数。

        Returns:
            规划结果。
        """
        # 加载规划所需的上下文与用户输入
        planner_input = await self._load_plannable_input(
            command,
            {ContextTurnStatus.CONTEXT_READY.value},
        )
        # 短事务创建 Plan 实体（初始状态为 planning）
        plan = await asyncio.to_thread(
            self._planning_use_cases.create_plan.execute,
            CreatePlanInput(
                turn_id=command.turn_id,
                revision=command.revision,
                workflow_id=command.workflow_id,
                parent_plan_id=command.parent_plan_id,
            ),
        )
        # 运行 Planner 规划图并处理结果
        return await self._run_existing_plan(
            command,
            plan.plan_id,
            planner_input,
        )

    async def execute_existing(
        self,
        command: RunPlanningInput,
        plan_id: str,
    ) -> RunPlanningResult:
        """运行已由外层事务创建的新 revision Plan（用于 Replan 或澄清后重规划）。

        Args:
            command: 规划输入参数。
            plan_id: 已存在的 Plan ID。

        Returns:
            规划结果。
        """
        planner_input = await self._load_plannable_input(
            command,
            {
                ContextTurnStatus.CONTEXT_READY.value,
                ContextTurnStatus.PROCESSING.value,
                ContextTurnStatus.COMPLETED.value,
            },
        )
        await asyncio.to_thread(self._validate_existing_plan, command, plan_id)
        return await self._run_existing_plan(command, plan_id, planner_input)

    async def _run_existing_plan(
        self,
        command: RunPlanningInput,
        plan_id: str,
        planner_input: PlannerContextInput,
    ) -> RunPlanningResult:
        """构建 Agent 工具上下文，执行 PlannerRunner 并安全收尾。"""
        context = self._build_agent_context(
            command,
            plan_id,
            planner_input,
        )

        try:
            # 运行 LangGraph 编排的 Planner（Evidence -> Gap -> Commit）
            runner_result = await self._planner_runner.run(
                planner_input=planner_input,
                context=context,
            )
        except PlanningRetryRequested as exc:
            # Planner 内部主动请求重试
            return await asyncio.to_thread(
                self._finish_from_database,
                plan_id,
                command.turn_id,
                exc.reason,
            )
        except Exception:
            # 规划过程发生未捕获异常，标记 retry_pending
            return await asyncio.to_thread(
                self._finish_from_database,
                plan_id,
                command.turn_id,
                "Planner Runner 或 Tool 执行发生系统异常",
            )

        # 若模型产生了具体的澄清提问，回写到 ClarificationRequest 与 Turn
        question = getattr(
            getattr(runner_result, "final_output", None),
            "question",
            None,
        )
        if isinstance(question, str) and question.strip():
            await asyncio.to_thread(
                self._planning_use_cases.set_clarification_question.execute,
                SetClarificationQuestionInput(
                    plan_id=plan_id,
                    question=question,
                ),
            )

        # 校验最终数据库状态，若未终结则标记 retry_pending
        return await asyncio.to_thread(
            self._finish_from_database,
            plan_id,
            command.turn_id,
            "Planner 未调用 finalize_plan 或 mark_plan_unsupported",
        )

    async def _load_plannable_input(
        self,
        command: RunPlanningInput,
        allowed_statuses: set[str],
    ) -> PlannerContextInput:
        """从数据库读取并组装包含热资源队列的完整规划输入。"""
        snapshot = await asyncio.to_thread(
            self._load_plannable_snapshot,
            command,
            allowed_statuses,
        )
        context_chains: list[ContextChain] = []
        for loaded in snapshot.chains:
            queue = await self._context_resource_service.get_queue(
                conversation_id=command.conversation_id,
                chain_id=loaded.chain.chain_id,
                resource_version=loaded.resource_version,
            )
            context_chains.append(
                loaded.chain.model_copy(update={"resource_queue": queue})
            )
        return PlannerContextInput(
            current_user_input=snapshot.current_user_input,
            context_chains=context_chains,
        )

    def _load_plannable_snapshot(
        self,
        command: RunPlanningInput,
        allowed_statuses: set[str],
    ) -> _PlannableSnapshot:
        """短事务读取 Turn、ContextSelection 以及关联 Chain 实体。"""
        with self._ports.uow_factory() as uow:
            turn = uow.conversation_turns.get_by_id(command.turn_id)
            if turn is None:
                raise PlanningApplicationError(
                    404,
                    "Conversation Turn 不存在",
                    result_code="turn_not_found",
                )
            if turn.conversation_id != command.conversation_id:
                raise PlanningApplicationError(
                    409,
                    "Conversation Turn 会话归属不一致",
                    result_code="turn_conversation_conflict",
                )
            if turn.status not in allowed_statuses:
                raise PlanningApplicationError(
                    409,
                    "Conversation Turn 尚未完成 Context Selection",
                    result_code="turn_state_conflict",
                )
            selection = uow.context.get_selection_record(command.turn_id)
            if selection is None:
                raise PlanningApplicationError(
                    409,
                    "Conversation Turn 缺少 Context Selection",
                    result_code="context_selection_missing",
                )
            if selection.conversation_id != command.conversation_id:
                raise PlanningApplicationError(
                    409,
                    "Context Selection 会话归属不一致",
                    result_code="context_selection_conflict",
                )

            chain_ids = list(selection.relevant_chain_ids or [])
            chain_models = uow.context.get_chains_by_ids(chain_ids)
            chain_map = {chain.chain_id: chain for chain in chain_models}
            chains: list[_PlanningChainSnapshot] = []
            for chain_id in chain_ids:
                chain = chain_map.get(chain_id)
                if chain is None:
                    raise PlanningApplicationError(
                        409,
                        f"Context Selection Chain 不存在: {chain_id}",
                        result_code="context_selection_chain_missing",
                    )
                if chain.conversation_id != command.conversation_id:
                    raise PlanningApplicationError(
                        409,
                        f"Context Selection Chain 会话归属不一致: {chain_id}",
                        result_code="context_selection_conflict",
                    )
                chains.append(
                    _PlanningChainSnapshot(
                        chain=self._context_chain_mapper(
                            chain,
                            resource_queue=(
                                self._context_resource_service.empty_queue()
                            ),
                        ),
                        resource_version=chain.resource_version,
                    )
                )
            return _PlannableSnapshot(
                current_user_input=_compose_current_user_input(
                    turn.user_input,
                    turn.clarification_input,
                ),
                chains=chains,
            )

    def _validate_existing_plan(
        self,
        command: RunPlanningInput,
        plan_id: str,
    ) -> None:
        """校验已存在的 Plan revision 是否合法且处于 planning 状态。"""
        with self._ports.uow_factory() as uow:
            plan = uow.plans.get_by_id(plan_id)
            if plan is None:
                raise PlanningApplicationError(
                    404, "Plan 不存在", result_code="plan_not_found"
                )
            if (
                plan.turn_id != command.turn_id
                or plan.revision != command.revision
                or plan.status != PlanStatus.PLANNING.value
            ):
                raise PlanningApplicationError(
                    409,
                    "Plan revision 当前不可运行",
                    result_code="plan_state_conflict",
                )

    def _build_agent_context(
        self,
        command: RunPlanningInput,
        plan_id: str,
        planner_input: PlannerContextInput,
    ) -> AgentToolContext:
        """装配用于注入 Agents SDK / LangGraph 的 AgentToolContext。"""
        allowed_chain_ids = frozenset(
            chain.chain_id for chain in planner_input.context_chains
        )
        allowed_turn_ids = frozenset(
            {
                command.turn_id,
                *(
                    node.turn_id
                    for chain in planner_input.context_chains
                    for node in chain.nodes
                ),
            }
        )
        return AgentToolContext(
            trace_id=f"trace_{uuid4().hex}",
            agent_run_id=f"agent_run_{uuid4().hex}",
            agent_name=PLANNER_AGENT_NAME,
            conversation_id=command.conversation_id,
            turn_id=command.turn_id,
            task_id=None,
            actor_code=PLANNER_ACTOR_CODE,
            permissions=PLANNER_PERMISSIONS,
            document_services=self._document_services,
            context_services=self._context_services,
            operations_services=self._operations_services,
            allowed_context_chain_ids=allowed_chain_ids,
            allowed_context_turn_ids=allowed_turn_ids,
            planning_services=PlanningToolServices(
                create_process_document_task=(
                    self._planning_use_cases.create_process_document_task
                ),
                create_build_chunks_task=(
                    self._planning_use_cases.create_build_chunks_task
                ),
                create_index_vectors_task=(
                    self._planning_use_cases.create_index_vectors_task
                ),
                finalize_plan=self._planning_use_cases.finalize_plan,
                mark_plan_unsupported=(
                    self._planning_use_cases.mark_plan_unsupported
                ),
                mark_plan_needs_clarification=(
                    self._planning_use_cases.mark_plan_needs_clarification
                ),
            ),
            plan_id=plan_id,
            audit_logger=self._audit_logger_factory(),
        )

    def _finish_from_database(
        self,
        plan_id: str,
        turn_id: str,
        retry_reason: str,
    ) -> RunPlanningResult:
        """从数据库读取最终 Plan 结果；若状态未流转则兜底触发 retry_pending。"""
        result = self._read_result(plan_id, turn_id)
        needs_retry = result.status == PlanStatus.PLANNING or (
            result.status == PlanStatus.NEEDS_CLARIFICATION
            and result.clarification_question is None
        )
        if not needs_retry:
            return result

        self._planning_use_cases.mark_plan_retry_pending.execute(
            MarkPlanRetryPendingInput(
                plan_id=plan_id,
                reason=retry_reason,
            )
        )
        return self._read_result(plan_id, turn_id)

    def _read_result(
        self,
        plan_id: str,
        turn_id: str,
    ) -> RunPlanningResult:
        """短事务从数据库重新读取权威的 Plan、Tasks 与 Clarification 状态。"""
        with self._ports.uow_factory() as uow:
            plan = uow.plans.get_by_id(plan_id)
            if plan is None:
                raise PlanningApplicationError(
                    404,
                    "Plan 不存在",
                    result_code="plan_not_found",
                )
            if plan.turn_id != turn_id:
                raise PlanningApplicationError(
                    409,
                    "Plan 与 Conversation Turn 归属不一致",
                    result_code="plan_turn_conflict",
                )
            turn = uow.conversation_turns.get_by_id(turn_id)
            if turn is None:
                raise PlanningApplicationError(
                    404,
                    "Conversation Turn 不存在",
                    result_code="turn_not_found",
                )
            tasks = uow.tasks.list_by_plan_id(plan_id)
            clarification = uow.clarifications.get_by_plan_id(plan_id)
            status = PlanStatus(plan.status)
            task_ids: list[str] = []
            if status == PlanStatus.READY:
                pending_task_ids = [
                    task.task_id
                    for task in tasks
                    if task.status == TaskStatus.PENDING.value
                ]
                if list(turn.task_ids or []) != pending_task_ids:
                    raise PlanningApplicationError(
                        500,
                        "Plan、Task 与 Turn 的发布状态不一致",
                        result_code="planning_state_inconsistent",
                    )
                task_ids = pending_task_ids
            return RunPlanningResult(
                plan_id=plan.plan_id,
                turn_id=plan.turn_id,
                status=status,
                task_ids=task_ids,
                failure_reason=plan.failure_reason,
                clarification_question=(
                    clarification.question
                    if clarification is not None
                    else None
                ),
            )
