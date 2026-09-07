"""Plan ORM 仓储实现。

提供 Plan 实体的数据查询、行级悲观锁锁定与状态更新接口。
注意：Repository 只负责实体操作与 flush，不自行提交数据库事务。
"""

from sqlalchemy.orm import Session

from app.modules.planning.infrastructure.persistence.models.plan import Plan


class PlanRepository:
    """Plan 数据访问仓储，只负责实体查询、锁定和 flush，不提交事务。"""

    def __init__(self, db: Session) -> None:
        """初始化 PlanRepository。

        Args:
            db: SQLAlchemy Session 数据库会话。
        """
        self.db = db

    def create(self, plan: Plan) -> Plan:
        """添加并持久化新的 Plan 实体。

        Args:
            plan: Plan 实体对象。

        Returns:
            刷新后的 Plan 实体。
        """
        self.db.add(plan)
        self.db.flush()
        self.db.refresh(plan)
        return plan

    def get_by_id(self, plan_id: str) -> Plan | None:
        """根据 plan_id 查询 Plan 记录。

        Args:
            plan_id: 规划唯一标识。

        Returns:
            查询到的 Plan 实体或 None。
        """
        return (
            self.db.query(Plan)
            .filter(Plan.plan_id == plan_id)
            .first()
        )

    def get_by_turn_and_revision(
        self,
        turn_id: str,
        revision: int,
    ) -> Plan | None:
        """根据 turn_id 和 revision 查询特定版本的 Plan。

        Args:
            turn_id: 关联的 Turn ID。
            revision: 修订版本号。

        Returns:
            查询到的 Plan 实体或 None。
        """
        return (
            self.db.query(Plan)
            .filter(
                Plan.turn_id == turn_id,
                Plan.revision == revision,
            )
            .first()
        )

    def get_latest_by_turn(self, turn_id: str) -> Plan | None:
        """获取指定 Turn 最新版本号的 Plan 记录。

        Args:
            turn_id: 关联的 Turn ID。

        Returns:
            最新版本的 Plan 实体或 None。
        """
        return (
            self.db.query(Plan)
            .filter(Plan.turn_id == turn_id)
            .order_by(Plan.revision.desc())
            .first()
        )

    def get_by_workflow_and_revision(
        self,
        workflow_id: str,
        revision: int,
    ) -> Plan | None:
        """根据 workflow_id 和 revision 查询特定版本的 Plan。

        Args:
            workflow_id: 工作流 ID。
            revision: 修订版本号。

        Returns:
            查询到的 Plan 实体或 None。
        """
        return (
            self.db.query(Plan)
            .filter(
                Plan.workflow_id == workflow_id,
                Plan.revision == revision,
            )
            .first()
        )

    def get_by_workflow_and_revision_for_update(
        self,
        workflow_id: str,
        revision: int,
    ) -> Plan | None:
        """以悲观写锁（FOR UPDATE）查询指定 workflow 与 revision 的 Plan。

        Args:
            workflow_id: 工作流 ID。
            revision: 修订版本号。

        Returns:
            锁定的 Plan 实体或 None。
        """
        return (
            self.db.query(Plan)
            .filter(
                Plan.workflow_id == workflow_id,
                Plan.revision == revision,
            )
            .with_for_update()
            .first()
        )

    def get_by_id_for_update(self, plan_id: str) -> Plan | None:
        """以悲观写锁（FOR UPDATE）根据 plan_id 查询并锁定 Plan。

        Args:
            plan_id: 规划唯一标识。

        Returns:
            锁定的 Plan 实体或 None。
        """
        return (
            self.db.query(Plan)
            .filter(Plan.plan_id == plan_id)
            .with_for_update()
            .first()
        )

    def set_status(
        self,
        plan: Plan,
        *,
        status: str,
        failure_reason: str | None,
        failure_code: str | None = None,
    ) -> Plan:
        """更新 Plan 的状态、错误码与失败原因并 flush。

        Args:
            plan: 目标 Plan 实体。
            status: 新状态字符串（PlanStatus）。
            failure_reason: 失败原因说明。
            failure_code: 错误分类码。

        Returns:
            更新后的 Plan 实体。
        """
        plan.status = status
        plan.failure_code = failure_code
        plan.failure_reason = failure_reason
        self.db.flush()
        return plan

    def set_current_task(self, plan: Plan, task_id: str | None) -> Plan:
        """更新 Plan 当前正在执行的 current_task_id 并 flush。

        Args:
            plan: 目标 Plan 实体。
            task_id: 当前领取的 Task ID（或 None 释放）。

        Returns:
            更新后的 Plan 实体。
        """
        plan.current_task_id = task_id
        self.db.flush()
        return plan
