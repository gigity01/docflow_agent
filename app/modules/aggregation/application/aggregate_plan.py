"""读取已完成 Plan 的事实，生成确定性回答并完成 Context Turn。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

from app.modules.context.application.attribution_policy import (
    build_read_set_fallback_attribution,
)
from app.modules.context.application.dto import (
    ChainTurnUpdate,
    CompleteTurnCommand,
    ContextResourceInput,
)
from app.modules.planning.domain.enums import PlanStatus, TaskStatus
from app.modules.task_runtime.domain.enums import TaskExecutionStatus


@dataclass(frozen=True)
class _AggregationSnapshot:
    """Plan 聚合所需的不可变快照数据结构。

    Attributes:
        turn_id: 关联的 ConversationTurn 标识。
        task_ids: Plan 下包含的所有 Task 标识列表。
        relevant_chain_ids: Context Selection 为该 Turn 选中的历史链 ID 列表（Read Set）。
        summaries: 各 Task 的执行摘要字符串列表（格式为 `{capability_code}: {output_json}`）。
        resource_refs: 各 Task 执行过程中产生或引用的资源唯一引用列表（稳定去重）。
    """

    turn_id: str
    task_ids: list[str]
    relevant_chain_ids: list[str]
    summaries: list[str]
    resource_refs: list[str]


class AggregatePlanUseCase:
    """聚合已完成 Plan 执行结果的用例。

    职责与主流程：
    1. 从数据库读取所有成功 Task 的产出摘要与涉及的资源引用（保证稳定去重）。
    2. 生成确定性执行事实摘要字符串（本项目非 RAG 问答，仅生成执行事实汇报）。
    3. 若当前 Plan 是在用户回答澄清问题后派生的新 revision，将源 Plan 对应的 ClarificationRequest 状态推进至 resolved。
    4. 调用 ContextService.complete_turn 完成 Turn，在关联的上下文链上创建节点、追加资源事件并刷新 Redis 资源队列。
    """

    def __init__(self, *, uow_factory, context_service) -> None:
        """初始化 AggregatePlanUseCase。

        Args:
            uow_factory: UnitOfWork 工厂，用于提供数据库事务上下文。
            context_service: ContextService 实例，用于驱动上下文 Turn 的完成和归因写入。
        """
        self._uow_factory = uow_factory
        self._context_service = context_service

    async def execute(self, plan_id: str):
        """执行 Plan 结果聚合并完成 Context 回写。

        Args:
            plan_id: 已完成（COMPLETED）的 Plan 唯一标识。

        Returns:
            ContextService.complete_turn 的执行结果（包含更新后的 Turn 与链节点等信息）。

        Raises:
            ValueError: 当 Plan 未完成、Task 未全部成功、缺少成功执行记录或缺少 Context Selection 记录时抛出。
        """
        # 在工作线程中加载聚合所需的执行快照数据（避免同步 DB 操作阻塞事件循环）
        snapshot = await asyncio.to_thread(self._load_snapshot, plan_id)

        # 组装确定性的事实摘要文本
        summary = "；".join(snapshot.summaries)
        assistant_content = f"已完成 {len(snapshot.task_ids)} 项任务。{summary}"

        # 解析资源引用并构造 ContextResourceInput 列表
        resources = [
            self._resource(resource_ref)
            for resource_ref in snapshot.resource_refs
        ]

        # 若无命中已有链，则预分配新链 ID
        new_chain_id = (
            None
            if snapshot.relevant_chain_ids
            else f"chain_{uuid4().hex}"
        )
        attribution = build_read_set_fallback_attribution(
            snapshot.relevant_chain_ids,
            new_chain_id=new_chain_id,
        )

        # 收集目标上下文链 ID（包含已有链及新创建的链）
        target_chain_ids = list(attribution.existing_chain_ids)
        if attribution.create_new_chain:
            assert attribution.new_chain_id is not None
            target_chain_ids.append(attribution.new_chain_id)

        # 驱动 ContextService 完成当前 Turn，并在数据库中建立节点与更新资源
        result = await self._context_service.complete_turn(
            snapshot.turn_id,
            CompleteTurnCommand(
                assistant_content=assistant_content,
                assistant_compact=assistant_content,
                task_ids=snapshot.task_ids,
                task_result_summary=summary,
                attribution=attribution,
                chain_updates=[
                    ChainTurnUpdate(
                        chain_id=chain_id,
                        related_task_ids=snapshot.task_ids,
                        related_resources=resources,
                    )
                    for chain_id in target_chain_ids
                ],
            ),
        )

        # 若存在关联的 ClarificationRequest（来自澄清重规划），将其状态推进至 resolved
        await asyncio.to_thread(self._resolve_clarification, plan_id)
        return result

    def _load_snapshot(self, plan_id: str) -> _AggregationSnapshot:
        """从数据库中读取并校验 Plan 及其 Task 的最终执行事实。

        Args:
            plan_id: Plan 唯一标识。

        Returns:
            校验通过后的聚合快照对象。

        Raises:
            ValueError: Plan 状态非 COMPLETED、Task 存在非 SUCCEEDED、执行记录不完整或缺少 Context Selection。
        """
        with self._uow_factory() as uow:
            # 1. 校验 Plan 是否存在且处于 COMPLETED 状态
            plan = uow.plans.get_by_id(plan_id)
            if plan is None or plan.status != PlanStatus.COMPLETED.value:
                raise ValueError("Plan 尚未完成，不能聚合")

            # 2. 校验 Plan 下所有 Task 是否均已达到 SUCCEEDED 终态
            tasks = uow.tasks.list_by_plan_id(plan_id)
            if not tasks or any(
                task.status != TaskStatus.SUCCEEDED.value for task in tasks
            ):
                raise ValueError("Plan Task 尚未全部成功")

            # 3. 读取各 Task 最新的成功执行记录（TaskExecution）
            executions = [
                execution
                for execution in uow.task_executions.list_by_plan_id(plan_id)
                if execution.status == TaskExecutionStatus.SUCCEEDED.value
            ]
            latest_by_task = {}
            for execution in executions:
                latest_by_task[execution.task_id] = execution
            if set(latest_by_task) != {task.task_id for task in tasks}:
                raise ValueError("Task 成功执行记录不完整")

            # 4. 读取该 Turn 初始规划时锁定的 ContextSelectionRecord
            selection = uow.context.get_selection_record_for_update(
                plan.turn_id
            )
            if selection is None:
                raise ValueError("Turn 缺少 Context Selection 记录")

            # 5. 提取并稳定去重所有产出/涉及的资源引用
            resource_refs = list(
                dict.fromkeys(
                    resource_ref
                    for execution in latest_by_task.values()
                    for resource_ref in (execution.resource_refs_json or [])
                )
            )

            # 6. 生成每个 Task 的能力调用输出摘要
            summaries = [
                f"{task.capability_code}: {task.output_json}"
                for task in tasks
            ]
            return _AggregationSnapshot(
                turn_id=plan.turn_id,
                task_ids=[task.task_id for task in tasks],
                relevant_chain_ids=list(
                    selection.relevant_chain_ids or []
                ),
                summaries=summaries,
                resource_refs=resource_refs,
            )

    @staticmethod
    def _resource(resource_ref: str) -> ContextResourceInput:
        """将格式为 `resource_type:resource_id` 的字符串解析为 ContextResourceInput。

        Args:
            resource_ref: 资源引用字符串（例如 "document:doc_123"）。

        Returns:
            ContextResourceInput: 构造的上下文资源输入 DTO。

        Raises:
            ValueError: 资源引用格式不合法时抛出。
        """
        resource_type, separator, resource_id = resource_ref.partition(":")
        if not separator or not resource_type or not resource_id:
            raise ValueError(f"非法资源引用: {resource_ref}")
        return ContextResourceInput(
            resource_type=resource_type,
            resource_id=resource_id,
            relation="task_output",
            summary="Task Runtime 产生的资源",
        )

    def _resolve_clarification(self, plan_id: str) -> None:
        """若当前 Plan 来源于澄清回答的重规划，则将对应的 ClarificationRequest 状态推进为 resolved。

        根据架构规范，ClarificationRequest 在用户回答后处于 answered 状态，
        只有在基于该回答生成的新 Plan 全部成功聚合后，才原子推进为 resolved。

        Args:
            plan_id: 当前成功聚合的 Plan 标识。
        """
        with self._uow_factory() as uow:
            plan = uow.plans.get_by_id(plan_id)
            if plan is None or plan.parent_plan_id is None:
                return
            request = uow.clarifications.get_by_plan_id_for_update(
                plan.parent_plan_id
            )
            if request is None or request.status != "answered":
                return
            request.status = "resolved"
            request.resolved_at = datetime.now()
            uow.commit()
