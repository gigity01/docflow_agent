"""TaskExecution 仓储实现。

提供 TaskExecution 实体的添加、查询、行级锁定以及按任务/尝试次数检索等操作。
注意：Repository 只负责实体操作与 flush，不自行提交数据库事务。
"""

from sqlalchemy.orm import Session

from app.modules.task_runtime.infrastructure.persistence.models import (
    TaskExecution,
)


class TaskExecutionRepository:
    """TaskExecution 数据访问仓储。"""

    def __init__(self, db: Session) -> None:
        """初始化 TaskExecutionRepository。

        Args:
            db: SQLAlchemy Session 会话。
        """
        self.db = db

    def add(self, execution: TaskExecution) -> TaskExecution:
        """添加 TaskExecution 实体并 flush。

        Args:
            execution: TaskExecution 实体对象。

        Returns:
            刷新后的实体。
        """
        self.db.add(execution)
        self.db.flush()
        return execution

    def get_by_id_for_update(self, execution_id: str) -> TaskExecution | None:
        """以悲观写锁（FOR UPDATE）根据 execution_id 查询并锁定 TaskExecution。

        Args:
            execution_id: 执行唯一标识。

        Returns:
            锁定的实体或 None。
        """
        return (
            self.db.query(TaskExecution)
            .filter(TaskExecution.execution_id == execution_id)
            .with_for_update()
            .first()
        )

    def list_by_plan_id(self, plan_id: str) -> list[TaskExecution]:
        """查询指定 Plan 下的全部 TaskExecution 记录，按 started_at 升序排列。

        Args:
            plan_id: 规划 ID。

        Returns:
            执行记录列表。
        """
        return (
            self.db.query(TaskExecution)
            .filter(TaskExecution.plan_id == plan_id)
            .order_by(TaskExecution.started_at.asc())
            .all()
        )

    def get_latest_running_by_task_for_update(
        self,
        task_id: str,
    ) -> TaskExecution | None:
        """以悲观写锁查询指定 Task 当前最新的 running 状态 TaskExecution。

        Args:
            task_id: 任务 ID。

        Returns:
            锁定的实体或 None。
        """
        return (
            self.db.query(TaskExecution)
            .filter(
                TaskExecution.task_id == task_id,
                TaskExecution.status == "running",
            )
            .order_by(TaskExecution.attempt.desc())
            .with_for_update()
            .first()
        )

    def get_latest_by_task_for_update(
        self,
        task_id: str,
    ) -> TaskExecution | None:
        """以悲观写锁查询指定 Task 最新 attempt 的 TaskExecution 记录。

        Args:
            task_id: 任务 ID。

        Returns:
            锁定的实体或 None。
        """
        return (
            self.db.query(TaskExecution)
            .filter(TaskExecution.task_id == task_id)
            .order_by(TaskExecution.attempt.desc())
            .with_for_update()
            .first()
        )
