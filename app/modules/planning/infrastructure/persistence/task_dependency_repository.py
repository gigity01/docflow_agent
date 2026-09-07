"""TaskDependency 仓储实现。

提供 Task DAG 依赖边数据的批量持久化与查询接口。
"""

from collections.abc import Iterable

from sqlalchemy.orm import Session

from app.modules.planning.infrastructure.persistence.models.task_dependency import (
    TaskDependency,
)


class TaskDependencyRepository:
    """Task 依赖边仓储类。"""

    def __init__(self, db: Session) -> None:
        """初始化 TaskDependencyRepository。

        Args:
            db: SQLAlchemy Session 会话。
        """
        self.db = db

    def add_all(self, dependencies: Iterable[TaskDependency]) -> None:
        """批量添加 Task 依赖边并 flush 到数据库。

        Args:
            dependencies: TaskDependency 实体集合。
        """
        self.db.add_all(list(dependencies))
        self.db.flush()

    def list_by_plan_id(self, plan_id: str) -> list[TaskDependency]:
        """查询指定 Plan 下的全部 Task 依赖边记录。

        Args:
            plan_id: 规划 ID。

        Returns:
            依赖边列表。
        """
        return (
            self.db.query(TaskDependency)
            .filter(TaskDependency.plan_id == plan_id)
            .all()
        )
