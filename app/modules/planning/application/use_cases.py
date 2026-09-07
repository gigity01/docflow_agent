"""Plan 与 Task 的事务型核心 Use Cases。

本模块实现了规划系统核心用例：
1. CreatePlanUseCase: 创建 Plan 实体与初始 revision。
2. CreateProcessDocumentTaskUseCase / CreateBuildChunksTaskUseCase / CreateIndexVectorsTaskUseCase: 创建草稿 Task。
3. FinalizePlanUseCase: 校验 Task 连续性与 DAG 依赖拓扑（最大深度 3、无环），并在单一事务中原子发布 Plan、更新 Turn 状态及写入 Outbox 事件。
4. MarkPlanUnsupportedUseCase / MarkPlanRetryPendingUseCase / MarkPlanNeedsClarificationUseCase: 处理规划异常、不支持请求及用户澄清流程。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4

from app.modules.clarification.domain.enums import ClarificationStatus
from app.modules.context.domain.enums import ContextTurnStatus
from app.modules.messaging.domain.enums import (
    OutboxEventStatus,
    RuntimeEventType,
)
from app.modules.planning.application.dto import (
    CreateBuildChunksTaskInput,
    CreateIndexVectorsTaskInput,
    CreatePlanInput,
    CreateProcessDocumentTaskInput,
    FinalizePlanInput,
    FinalizePlanResult,
    MarkPlanNeedsClarificationInput,
    MarkPlanRetryPendingInput,
    MarkPlanUnsupportedInput,
    PlanResult,
    SetClarificationQuestionInput,
    TaskResult,
)
from app.modules.planning.application.errors import PlanningApplicationError
from app.modules.planning.application.ports import PlanningApplicationPorts
from app.modules.planning.domain.enums import (
    PlanningCapabilityCode,
    PlanStatus,
    TaskStatus,
)


MAX_TASKS_PER_PLAN = 10
MAX_PLAN_DAG_DEPTH = 3


def _new_id(prefix: str) -> str:
    """生成带前缀的唯一十六进制 ID。"""
    return f"{prefix}_{uuid4().hex}"


def _new_outbox_event(
    ports: PlanningApplicationPorts,
    *,
    event_type: RuntimeEventType,
    aggregate_id: str,
    payload: dict,
):
    """构建标准初始状态的 OutboxEvent 实体。"""
    return ports.outbox_event_factory(
        event_id=_new_id("event"),
        event_type=event_type.value,
        aggregate_type="plan",
        aggregate_id=aggregate_id,
        payload_json=payload,
        status=OutboxEventStatus.PENDING.value,
        attempts=0,
        available_at=datetime.now(),
        published_at=None,
    )


def _plan_not_found() -> PlanningApplicationError:
    """生成 Plan 不存在的标准业务异常。"""
    return PlanningApplicationError(
        404,
        "Plan 不存在",
        result_code="plan_not_found",
    )


def _turn_not_found() -> PlanningApplicationError:
    """生成 Conversation Turn 不存在的标准业务异常。"""
    return PlanningApplicationError(
        404,
        "Conversation Turn 不存在",
        result_code="turn_not_found",
    )


def _require_planning(plan: Any) -> None:
    """校验 Plan 状态必须为 PLANNING，否则抛出状态冲突异常。"""
    if plan.status != PlanStatus.PLANNING.value:
        raise PlanningApplicationError(
            409,
            f"Plan 当前状态不允许继续规划: {plan.status}",
            result_code="plan_state_conflict",
        )


def _require_turn_ownership(plan: Any, turn_id: str) -> None:
    """校验 Plan 归属的 Turn ID 与当前上下文是否一致。"""
    if plan.turn_id != turn_id:
        raise PlanningApplicationError(
            409,
            "Plan 与 Conversation Turn 归属不一致",
            result_code="plan_turn_conflict",
        )


class CreatePlanUseCase:
    """为已持久化 Turn 创建 planning 状态的 revision。"""

    def __init__(self, *, ports: PlanningApplicationPorts) -> None:
        """初始化 CreatePlanUseCase。

        Args:
            ports: 数据库能力与模型工厂集合。
        """
        self._ports = ports

    def execute(self, command: CreatePlanInput) -> PlanResult:
        """执行创建 Plan 流程。

        Args:
            command: 包含 turn_id、revision、workflow_id 等参数的输入模型。

        Returns:
            创建完成的 Plan 视图对象。

        Raises:
            PlanningApplicationError: Turn 不存在或该 revision 已存在时。
        """
        with self._ports.uow_factory() as uow:
            # 校验所属 Turn 是否存在
            turn = uow.conversation_turns.get_by_id(command.turn_id)
            if turn is None:
                raise _turn_not_found()
            # 校验 workflow 或 turn 下该 revision 是否发生冲突
            existing = (
                uow.plans.get_by_workflow_and_revision(
                    command.workflow_id,
                    command.revision,
                )
                if command.workflow_id is not None
                else uow.plans.get_by_turn_and_revision(
                    command.turn_id,
                    command.revision,
                )
            )
            if existing is not None:
                raise PlanningApplicationError(
                    409,
                    "该 Turn 的 Plan revision 已存在",
                    result_code="plan_revision_conflict",
                )
            # 持久化新 Plan 实体
            plan = uow.plans.create(
                self._ports.plan_factory(
                    plan_id=_new_id("plan"),
                    workflow_id=(command.workflow_id or _new_id("workflow")),
                    turn_id=command.turn_id,
                    parent_plan_id=command.parent_plan_id,
                    current_task_id=None,
                    status=PlanStatus.PLANNING.value,
                    revision=command.revision,
                    failure_code=None,
                    failure_reason=None,
                )
            )
            uow.commit()
            return PlanResult.model_validate(plan)


class _CreateDocumentTaskUseCase:
    """创建文档领域 Task 的抽象基类。"""

    def __init__(
        self,
        *,
        ports: PlanningApplicationPorts,
        capability_code: PlanningCapabilityCode,
    ) -> None:
        """初始化 _CreateDocumentTaskUseCase。

        Args:
            ports: 数据库能力集合。
            capability_code: 当前用例对应的领域能力编码。
        """
        self._ports = ports
        self._capability_code = capability_code

    def _execute(self, command: Any) -> TaskResult:
        """执行 Task 创建的核心校验与持久化。

        Args:
            command: 任务创建命令对象。

        Returns:
            创建完成的 Task 实体对象。

        Raises:
            PlanningApplicationError: 任务数超限、序号重复、标识冲突或依赖非法时。
        """
        with self._ports.uow_factory() as uow:
            # 行锁锁定 Plan，确保当前处于 planning 状态
            plan = uow.plans.get_by_id_for_update(command.plan_id)
            if plan is None:
                raise _plan_not_found()
            _require_planning(plan)
            _require_turn_ownership(plan, command.turn_id)

            # 查询并锁定当前已有的 draft 任务列表
            draft_tasks = (
                uow.tasks.list_by_plan_id_and_status_for_update(
                    plan.plan_id,
                    TaskStatus.DRAFT.value,
                )
            )
            # 校验任务数量上限（最多 10 个）
            if len(draft_tasks) >= MAX_TASKS_PER_PLAN:
                raise PlanningApplicationError(
                    409,
                    f"Plan 的 draft Task 数量不能超过 {MAX_TASKS_PER_PLAN}",
                    result_code="plan_task_limit_exceeded",
                )
            # 校验 sequence 序号唯一性
            if any(
                task.sequence == command.sequence
                for task in draft_tasks
            ):
                raise PlanningApplicationError(
                    409,
                    "同一 Plan 的 Task sequence 不得重复",
                    result_code="plan_task_sequence_conflict",
                )
            # 校验 task_ref 引用唯一性
            if any(task.task_ref == command.task_ref for task in draft_tasks):
                raise PlanningApplicationError(
                    409,
                    "同一 Plan 的 task_ref 不得重复",
                    result_code="plan_task_ref_conflict",
                )
            # 校验前置依赖合法性：去重并禁止自身依赖
            dependency_refs = list(dict.fromkeys(command.depends_on_task_refs))
            if command.task_ref in dependency_refs:
                raise PlanningApplicationError(
                    409,
                    "Task 不能依赖自身 task_ref",
                    result_code="plan_task_dependency_invalid",
                )
            # 写入 draft 任务实体
            try:
                task = uow.tasks.create(
                    self._ports.task_factory(
                        task_id=_new_id("task"),
                        plan_id=plan.plan_id,
                        turn_id=plan.turn_id,
                        task_ref=command.task_ref,
                        capability_code=self._capability_code.value,
                        input_json={
                            "document_id": command.document_id,
                            "_depends_on_task_refs": dependency_refs,
                        },
                        sequence=command.sequence,
                        status=TaskStatus.DRAFT.value,
                        attempt_count=0,
                        max_attempts=3,
                    )
                )
            except Exception as exc:
                if self._ports.is_integrity_error(exc):
                    raise PlanningApplicationError(
                        409,
                        "同一 Plan 的 Task sequence 不得重复",
                        result_code="plan_task_sequence_conflict",
                    ) from exc
                raise
            uow.commit()
            return TaskResult.model_validate(task)


class CreateProcessDocumentTaskUseCase(_CreateDocumentTaskUseCase):
    """为指定 Plan 创建文档处理（Process）Task 的用例。"""

    def __init__(self, *, ports: PlanningApplicationPorts) -> None:
        super().__init__(
            ports=ports,
            capability_code=PlanningCapabilityCode.PROCESS_DOCUMENT,
        )

    def execute(self, command: CreateProcessDocumentTaskInput) -> TaskResult:
        """执行创建文档处理任务。"""
        return self._execute(command)


class CreateBuildChunksTaskUseCase(_CreateDocumentTaskUseCase):
    """为指定 Plan 创建文档父子切块（Build Chunks）Task 的用例。"""

    def __init__(self, *, ports: PlanningApplicationPorts) -> None:
        super().__init__(
            ports=ports,
            capability_code=PlanningCapabilityCode.BUILD_DOCUMENT_CHUNKS,
        )

    def execute(self, command: CreateBuildChunksTaskInput) -> TaskResult:
        """执行创建文档切块任务。"""
        return self._execute(command)


class CreateIndexVectorsTaskUseCase(_CreateDocumentTaskUseCase):
    """为指定 Plan 创建文档向量索引（Index Vectors）Task 的用例。"""

    def __init__(self, *, ports: PlanningApplicationPorts) -> None:
        super().__init__(
            ports=ports,
            capability_code=PlanningCapabilityCode.INDEX_DOCUMENT_VECTORS,
        )

    def execute(self, command: CreateIndexVectorsTaskInput) -> TaskResult:
        """执行创建文档向量索引任务。"""
        return self._execute(command)


class FinalizePlanUseCase:
    """原子发布 draft Tasks，校验 DAG 约束，并将状态与 Outbox 唤醒事件落盘。

    发布约束：
    1. Task 数量必须为 1 ~ 10 个。
    2. sequence 必须从 1 开始严格单调递增且连续。
    3. DAG 校验：依赖边必须合法、无环、最大深度不超过 3。
    4. 单一事务内原子将 Plan 推进至 READY、全部 Task 转为 PENDING、写入依赖边表、更新 Turn 状态及追加首个 `runtime.plan_wakeup` Outbox 事件。
    """

    def __init__(self, *, ports: PlanningApplicationPorts) -> None:
        """初始化 FinalizePlanUseCase。

        Args:
            ports: 数据库能力集合。
        """
        self._ports = ports

    def execute(self, command: FinalizePlanInput) -> FinalizePlanResult:
        """执行 Plan 发布与校验流程。

        Args:
            command: 包含 plan_id 与 turn_id 的输入。

        Returns:
            发布结果。

        Raises:
            PlanningApplicationError: 当 Plan 状态冲突、任务序号不连续或 DAG 校验失败时。
        """
        with self._ports.uow_factory() as uow:
            # 行锁锁定 Plan，校验状态
            plan = uow.plans.get_by_id_for_update(command.plan_id)
            if plan is None:
                raise _plan_not_found()
            _require_planning(plan)
            _require_turn_ownership(plan, command.turn_id)

            # 查询并锁定所有 draft 状态的 Task
            tasks = uow.tasks.list_by_plan_id_and_status_for_update(
                plan.plan_id,
                TaskStatus.DRAFT.value,
            )
            # 约束 1：任务数量为 1 ~ 10 个
            if not tasks:
                raise PlanningApplicationError(
                    409,
                    "Plan 至少需要一个 draft Task 才能发布",
                    result_code="plan_task_count_invalid",
                )
            if len(tasks) > MAX_TASKS_PER_PLAN:
                raise PlanningApplicationError(
                    409,
                    f"Plan 的 Task 数量不能超过 {MAX_TASKS_PER_PLAN}",
                    result_code="plan_task_limit_exceeded",
                )
            # 约束 2：sequence 必须从 1 开始严格单调连续递增
            sequences = [task.sequence for task in tasks]
            expected_sequences = list(range(1, len(tasks) + 1))
            if sequences != expected_sequences:
                raise PlanningApplicationError(
                    409,
                    "Task sequence 必须唯一且从 1 开始连续",
                    result_code="plan_task_sequence_invalid",
                )

            # 约束 3：校验 Task DAG（无环、无无效引用、深度 <= 3）
            dependency_pairs = self._validate_dag(tasks)

            # 校验并锁定关联的 ConversationTurn
            turn = uow.conversation_turns.get_by_id_for_update(plan.turn_id)
            if turn is None:
                raise _turn_not_found()
            if turn.status not in {
                ContextTurnStatus.CONTEXT_READY.value,
                ContextTurnStatus.PROCESSING.value,
            }:
                raise PlanningApplicationError(
                    409,
                    "Conversation Turn 当前状态不允许发布 Plan",
                    result_code="turn_state_conflict",
                )
            task_ids = [task.task_id for task in tasks]

            # 约束 4：原子落盘 - 写入持久化依赖边记录
            uow.task_dependencies.add_all(
                self._ports.task_dependency_factory(
                    dependency_id=_new_id("dependency"),
                    plan_id=plan.plan_id,
                    task_id=task_id,
                    depends_on_task_id=depends_on_task_id,
                )
                for task_id, depends_on_task_id in dependency_pairs
            )
            # 清理 input_json 中的临时依赖字段
            for task in tasks:
                task.input_json = {
                    key: value
                    for key, value in task.input_json.items()
                    if key != "_depends_on_task_refs"
                }
            # 原子流转状态：Tasks -> pending, Plan -> ready, Turn -> processing
            uow.tasks.set_status(tasks, TaskStatus.PENDING.value)
            uow.plans.set_status(
                plan,
                status=PlanStatus.READY.value,
                failure_reason=None,
            )
            uow.conversation_turns.set_task_ids(turn, task_ids)
            uow.conversation_turns.set_status(
                turn,
                ContextTurnStatus.PROCESSING.value,
            )
            # 追加首个 plan_wakeup Outbox 事件以唤醒 Task Runtime Worker
            uow.outbox.add(
                _new_outbox_event(
                    self._ports,
                    event_type=RuntimeEventType.PLAN_WAKEUP,
                    aggregate_id=plan.plan_id,
                    payload={
                        "workflow_id": plan.workflow_id,
                        "plan_id": plan.plan_id,
                    },
                )
            )
            uow.commit()
            return FinalizePlanResult(
                plan_id=plan.plan_id,
                turn_id=plan.turn_id,
                plan_status=plan.status,
                task_ids=task_ids,
            )

    @staticmethod
    def _validate_dag(tasks: list[Any]) -> list[tuple[str, str]]:
        """校验任务拓扑图（DAG）的合法性。

        检查项：
        - task_ref 唯一性。
        - 依赖引用的存在性与非自依赖性。
        - 无循环依赖（环检测）。
        - DAG 最大拓扑深度不超过 MAX_PLAN_DAG_DEPTH (3)。

        Args:
            tasks: draft 任务列表。

        Returns:
            list[tuple[str, str]]: (task_id, depends_on_task_id) 依赖对列表。

        Raises:
            PlanningApplicationError: 拓扑非法或深度超限时。
        """
        by_ref = {task.task_ref: task for task in tasks}
        if len(by_ref) != len(tasks):
            raise PlanningApplicationError(
                409,
                "Task task_ref 必须唯一",
                result_code="plan_task_ref_conflict",
            )
        graph: dict[str, list[str]] = {}
        for task in tasks:
            refs = list(task.input_json.get("_depends_on_task_refs", []))
            if len(refs) != len(set(refs)):
                raise PlanningApplicationError(
                    409,
                    "Task 依赖引用不得重复",
                    result_code="plan_task_dependency_invalid",
                )
            unknown = set(refs) - set(by_ref)
            if unknown:
                raise PlanningApplicationError(
                    409,
                    "Task 依赖引用不存在: " + ", ".join(sorted(unknown)),
                    result_code="plan_task_dependency_missing",
                )
            if task.task_ref in refs:
                raise PlanningApplicationError(
                    409,
                    "Task 不能依赖自身",
                    result_code="plan_task_dependency_invalid",
                )
            graph[task.task_ref] = refs

        depths: dict[str, int] = {}
        visiting: set[str] = set()

        def depth(task_ref: str) -> int:
            if task_ref in depths:
                return depths[task_ref]
            if task_ref in visiting:
                raise PlanningApplicationError(
                    409,
                    "Task DAG 不能包含循环依赖",
                    result_code="plan_task_dependency_cycle",
                )
            visiting.add(task_ref)
            predecessors = graph[task_ref]
            result = 1 + max((depth(ref) for ref in predecessors), default=0)
            visiting.remove(task_ref)
            depths[task_ref] = result
            return result

        if max((depth(task_ref) for task_ref in graph), default=0) > MAX_PLAN_DAG_DEPTH:
            raise PlanningApplicationError(
                409,
                f"Plan DAG 深度不能超过 {MAX_PLAN_DAG_DEPTH}",
                result_code="plan_task_dependency_depth_exceeded",
            )
        return [
            (by_ref[task_ref].task_id, by_ref[dependency_ref].task_id)
            for task_ref, dependency_refs in graph.items()
            for dependency_ref in dependency_refs
        ]


class _MarkPlanUseCase:
    """将 Plan 标记为目标状态的抽象基类。"""

    def __init__(
        self,
        *,
        ports: PlanningApplicationPorts,
        target_status: PlanStatus,
    ) -> None:
        self._ports = ports
        self._target_status = target_status

    def _execute(self, plan_id: str, reason: str) -> PlanResult:
        normalized_reason = reason.strip()
        if not normalized_reason:
            raise PlanningApplicationError(
                400,
                "失败原因不能为空",
                result_code="invalid_plan_failure_reason",
            )
        with self._ports.uow_factory() as uow:
            plan = uow.plans.get_by_id_for_update(plan_id)
            if plan is None:
                raise _plan_not_found()
            if not (
                self._target_status == PlanStatus.RETRY_PENDING
                and plan.status == PlanStatus.NEEDS_CLARIFICATION.value
            ):
                _require_planning(plan)
            uow.plans.set_status(
                plan,
                status=self._target_status.value,
                failure_code=(
                    "planner_system_failure"
                    if self._target_status == PlanStatus.RETRY_PENDING
                    else "unsupported"
                ),
                failure_reason=normalized_reason,
            )
            # 若重试挂起，处理澄清请求过期并发布 REPLAN_REQUESTED 事件
            if self._target_status == PlanStatus.RETRY_PENDING:
                clarification = uow.clarifications.get_by_plan_id_for_update(
                    plan.plan_id
                )
                if clarification is not None:
                    clarification.status = ClarificationStatus.EXPIRED.value
                    clarification.resolved_at = datetime.now()
                turn = uow.conversation_turns.get_by_id_for_update(
                    plan.turn_id
                )
                if turn is None:
                    raise _turn_not_found()
                uow.conversation_turns.set_status(
                    turn,
                    ContextTurnStatus.PROCESSING.value,
                )
                uow.outbox.add(
                    _new_outbox_event(
                        self._ports,
                        event_type=RuntimeEventType.REPLAN_REQUESTED,
                        aggregate_id=plan.plan_id,
                        payload={
                            "workflow_id": plan.workflow_id,
                            "conversation_id": turn.conversation_id,
                            "root_turn_id": plan.turn_id,
                            "previous_plan_id": plan.plan_id,
                            "next_revision": plan.revision + 1,
                            "trigger_type": "planner_system_failure",
                            "source_task_id": None,
                            "error_code": "planner_system_failure",
                            "error_message": normalized_reason,
                        },
                    )
                )
            uow.commit()
            return PlanResult.model_validate(plan)


class MarkPlanUnsupportedUseCase(_MarkPlanUseCase):
    """将 Plan 标记为 unsupported（能力不支持）的用例。"""

    def __init__(self, *, ports: PlanningApplicationPorts) -> None:
        super().__init__(ports=ports, target_status=PlanStatus.UNSUPPORTED)

    def execute(self, command: MarkPlanUnsupportedInput) -> PlanResult:
        """执行标记不支持。"""
        return self._execute(command.plan_id, command.reason)


class MarkPlanRetryPendingUseCase(_MarkPlanUseCase):
    """将 Plan 标记为 retry_pending（系统错误待重试）的用例。"""

    def __init__(self, *, ports: PlanningApplicationPorts) -> None:
        super().__init__(ports=ports, target_status=PlanStatus.RETRY_PENDING)

    def execute(self, command: MarkPlanRetryPendingInput) -> PlanResult:
        """执行标记待重试。"""
        return self._execute(command.plan_id, command.reason)


class MarkPlanNeedsClarificationUseCase:
    """将 Plan 与 Turn 标记为 needs_clarification 并创建 ClarificationRequest 的用例。"""

    def __init__(self, *, ports: PlanningApplicationPorts) -> None:
        self._ports = ports

    def execute(self, command: MarkPlanNeedsClarificationInput) -> PlanResult:
        """执行标记需要澄清。

        Args:
            command: 澄清请求输入。

        Returns:
            更新后的 Plan 实体。

        Raises:
            PlanningApplicationError: 会话归属冲突、状态冲突或已存在 open 澄清时。
        """
        with self._ports.uow_factory() as uow:
            plan = uow.plans.get_by_id_for_update(command.plan_id)
            if plan is None:
                raise _plan_not_found()
            _require_planning(plan)
            turn = uow.conversation_turns.get_by_id_for_update(plan.turn_id)
            if turn is None:
                raise _turn_not_found()
            if turn.conversation_id != command.conversation_id:
                raise PlanningApplicationError(
                    409,
                    "Clarification 会话归属不一致",
                    result_code="clarification_conversation_conflict",
                )
            if turn.status not in {
                ContextTurnStatus.CONTEXT_READY.value,
                ContextTurnStatus.PROCESSING.value,
            }:
                raise PlanningApplicationError(
                    409,
                    "Conversation Turn 当前状态不允许标记需要澄清",
                    result_code="turn_state_conflict",
                )
            if (
                uow.clarifications.get_by_source_turn_id_for_update(
                    plan.turn_id
                )
                is not None
            ):
                raise PlanningApplicationError(
                    409,
                    "Conversation Turn 已存在 ClarificationRequest",
                    result_code="clarification_conflict",
                )
            # 更新 Plan 与 Turn 状态
            uow.plans.set_status(
                plan,
                status=PlanStatus.NEEDS_CLARIFICATION.value,
                failure_code="clarification_required",
                failure_reason=command.reason,
            )
            uow.conversation_turns.set_status(
                turn,
                ContextTurnStatus.NEEDS_CLARIFICATION.value,
            )
            # 持久化 ClarificationRequest 实体
            uow.clarifications.add(
                self._ports.clarification_request_factory(
                    clarification_id=_new_id("clarification"),
                    conversation_id=command.conversation_id,
                    source_turn_id=plan.turn_id,
                    source_plan_id=plan.plan_id,
                    kind=command.kind,
                    reason=command.reason,
                    question=None,
                    required_information_json=list(
                        command.required_information
                    ),
                    known_resource_refs_json=list(
                        command.known_resource_refs
                    ),
                    status=ClarificationStatus.OPEN.value,
                    answer_turn_id=None,
                    resolved_at=None,
                )
            )
            uow.commit()
            return PlanResult.model_validate(plan)


class SetClarificationQuestionUseCase:
    """为已标记需要澄清的 Plan 设置具体向用户提问文本的用例。"""

    def __init__(self, *, ports: PlanningApplicationPorts) -> None:
        self._ports = ports

    def execute(self, command: SetClarificationQuestionInput) -> str:
        """执行设置澄清提问文本并同步回写 Turn。

        Args:
            command: 提问文本输入。

        Returns:
            成功持久化的提问文本。
        """
        with self._ports.uow_factory() as uow:
            plan = uow.plans.get_by_id_for_update(command.plan_id)
            request = uow.clarifications.get_by_plan_id_for_update(
                command.plan_id
            )
            if plan is None or request is None:
                raise PlanningApplicationError(
                    404,
                    "ClarificationRequest 不存在",
                    result_code="clarification_not_found",
                )
            turn = uow.conversation_turns.get_by_id_for_update(plan.turn_id)
            if turn is None:
                raise _turn_not_found()
            if plan.status != PlanStatus.NEEDS_CLARIFICATION.value:
                raise PlanningApplicationError(
                    409,
                    "Plan 当前不等待澄清",
                    result_code="plan_state_conflict",
                )
            if turn.status != ContextTurnStatus.NEEDS_CLARIFICATION.value:
                raise PlanningApplicationError(
                    409,
                    "Conversation Turn 当前不等待澄清",
                    result_code="turn_state_conflict",
                )
            # 同步更新 ClarificationRequest 与 Turn 助手内容
            request.question = command.question
            turn.assistant_content = command.question
            turn.assistant_compact = command.question
            turn.task_result_summary = "等待用户补充信息"
            uow.commit()
            return request.question


@dataclass(frozen=True)
class PlanningUseCases:
    """包含全部 Planning Use Cases 实例的容器类。"""

    create_plan: CreatePlanUseCase
    create_process_document_task: CreateProcessDocumentTaskUseCase
    create_build_chunks_task: CreateBuildChunksTaskUseCase
    create_index_vectors_task: CreateIndexVectorsTaskUseCase
    finalize_plan: FinalizePlanUseCase
    mark_plan_unsupported: MarkPlanUnsupportedUseCase
    mark_plan_retry_pending: MarkPlanRetryPendingUseCase
    mark_plan_needs_clarification: MarkPlanNeedsClarificationUseCase
    set_clarification_question: SetClarificationQuestionUseCase


def build_planning_use_cases(
    ports: PlanningApplicationPorts,
) -> PlanningUseCases:
    """以同一组显式 Ports 装配全部 Planning Use Cases。

    Args:
        ports: PlanningApplicationPorts 实例。

    Returns:
        装配完成的用例集合对象。
    """
    return PlanningUseCases(
        create_plan=CreatePlanUseCase(ports=ports),
        create_process_document_task=CreateProcessDocumentTaskUseCase(
            ports=ports
        ),
        create_build_chunks_task=CreateBuildChunksTaskUseCase(ports=ports),
        create_index_vectors_task=CreateIndexVectorsTaskUseCase(ports=ports),
        finalize_plan=FinalizePlanUseCase(ports=ports),
        mark_plan_unsupported=MarkPlanUnsupportedUseCase(ports=ports),
        mark_plan_retry_pending=MarkPlanRetryPendingUseCase(ports=ports),
        mark_plan_needs_clarification=MarkPlanNeedsClarificationUseCase(
            ports=ports
        ),
        set_clarification_question=SetClarificationQuestionUseCase(
            ports=ports
        ),
    )
