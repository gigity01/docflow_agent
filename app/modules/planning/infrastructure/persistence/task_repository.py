"""Task ORM 仓储实现。

提供 Task 实体的持久化、查询、行级锁定、依赖就绪检查及状态批量更新等操作。
注意：Repository 只负责实体操作与 flush，不自行提交数据库事务。
"""

from collections.abc import Iterable

from sqlalchemy.orm import Session

from app.modules.planning.infrastructure.persistence.models.task import Task
from app.modules.planning.infrastructure.persistence.models.task_dependency import (
    TaskDependency,
)


class TaskRepository:
    """Task 数据访问仓储，只负责 Task 查询、锁定和 flush，不提交事务。"""

    def __init__(self, db: Session) -> None:
        """初始化 TaskRepository。

        Args:
            db: SQLAlchemy Session 数据库会话。
        """
        self.db = db

    def create(self, task: Task) -> Task:
        """添加并持久化新的 Task 实体。

        Args:
            task: Task 实体对象。

        Returns:
            刷新后的 Task 实体。
        """
        self.db.add(task)
        self.db.flush()
        self.db.refresh(task)
        return task

    def list_by_plan_id(self, plan_id: str) -> list[Task]:
        """查询指定 Plan 下的全部 Task，按 sequence 和 task_id 升序排列。

        Args:
            plan_id: 规划 ID。

        Returns:
            任务列表。
        """
        return (
            self.db.query(Task)
            .filter(Task.plan_id == plan_id)
            .order_by(Task.sequence.asc(), Task.task_id.asc())
            .all()
        )

    def list_by_plan_id_and_status_for_update(
        self,
        plan_id: str,
        status: str,
    ) -> list[Task]:
        """以悲观写锁（FOR UPDATE）查询指定 Plan 和状态的 Task 列表。

        Args:
            plan_id: 规划 ID。
            status: 目标状态字符串（TaskStatus）。

        Returns:
            锁定的任务列表。
        """
        return (
            self.db.query(Task)
            .filter(
                Task.plan_id == plan_id,
                Task.status == status,
            )
            .order_by(Task.sequence.asc(), Task.task_id.asc())
            .with_for_update()
            .all()
        )

    def get_by_id_for_update(self, task_id: str) -> Task | None:
        """以悲观写锁（FOR UPDATE）根据 task_id 查询并锁定 Task。

        Args:
            task_id: 任务全局唯一标识。

        Returns:
            锁定的 Task 实体或 None。
        """
        return (
            self.db.query(Task)
            .filter(Task.task_id == task_id)
            .with_for_update()
            .first()
        )

    def get_next_runnable_for_update(
        self,
        plan_id: str,
        pending_status: str,
        succeeded_status: str,
    ) -> Task | None:
        """查询并锁定下一个可执行的任务。

        判断条件：
        1. 任务属于指定 Plan 且状态为 pending。
        2. 该任务在 TaskDependency 中的所有前置依赖任务均已处于 succeeded 状态。
        3. 按 sequence 升序与 task_id 升序选取首个满足依赖的任务。

        Args:
            plan_id: 规划 ID。
            pending_status: 待执行状态标识（TaskStatus.PENDING）。
            succeeded_status: 执行成功状态标识（TaskStatus.SUCCEEDED）。

        Returns:
            下一个就绪可执行的 Task 实体，或 None。
        """
        candidates = (
            self.db.query(Task)
            .filter(Task.plan_id == plan_id, Task.status == pending_status)
            .order_by(Task.sequence.asc(), Task.task_id.asc())
            .with_for_update()
            .all()
        )
        for task in candidates:
            predecessor_ids = [
                row.depends_on_task_id
                for row in self.db.query(TaskDependency)
                .filter(TaskDependency.task_id == task.task_id)
                .all()
            ]
            # 无前置依赖任务直接可执行
            if not predecessor_ids:
                return task
            # 校验所有前置任务是否都已成功
            succeeded_count = (
                self.db.query(Task)
                .filter(
                    Task.task_id.in_(predecessor_ids),
                    Task.status == succeeded_status,
                )
                .count()
            )
            if succeeded_count == len(predecessor_ids):
                return task
        return None

    def count_by_plan_and_status(self, plan_id: str, status: str) -> int:
        """统计指定 Plan 下处于特定状态的 Task 数量。

        Args:
            plan_id: 规划 ID。
            status: 任务状态。

        Returns:
            匹配的任务数量。
        """
        return (
            self.db.query(Task)
            .filter(Task.plan_id == plan_id, Task.status == status)
            .count()
        )

    def set_status(self, tasks: Iterable[Task], status: str) -> None:
        """批量设置 Task 列表的状态并 flush。

        Args:
            tasks: Task 实体迭代器。
            status: 目标状态字符串。
        """
        for task in tasks:
            task.status = status
        self.db.flush()

    def set_unfinished_status(self, plan_id: str, status: str) -> None:
        """将指定 Plan 下所有非终态的任务批量更新为指定状态并 flush。

        终态集合包括：succeeded, failed, cancelled, superseded。

        Args:
            plan_id: 规划 ID。
            status: 目标状态（如 failed 或 superseded）。
        """
        terminal = {"succeeded", "failed", "cancelled", "superseded"}
        tasks = self.db.query(Task).filter(Task.plan_id == plan_id).all()
        for task in tasks:
            if task.status not in terminal:
                task.status = status
        self.db.flush()
