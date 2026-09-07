"""Plan 的 SQLAlchemy ORM 定义。

映射 `plans` 数据表，记录针对 Conversation Turn 的任务规划及其版本修订历史。
"""

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.infrastructure.database.base import Base
from app.modules.planning.domain.enums import PlanStatus


class Plan(Base):
    """保存一次 Turn 的一个规划 revision 记录。

    Attributes:
        plan_id: 规划全局唯一标识，主键。
        workflow_id: 关联的工作流全局唯一标识。
        turn_id: 关联的 ConversationTurn 外键。
        parent_plan_id: 父 Plan ID（前一版本 revision 的外键引用）。
        current_task_id: 当前正在执行的任务 ID 外键引用。
        status: 规划生命周期状态（PlanStatus）。
        revision: 修订版本序号，从 1 开始。
        failure_code: 失败或重试错误分类码。
        failure_reason: 详细失败原因文本。
        started_at: 规划开始执行时间。
        completed_at: 规划最终完成或终止时间。
        created_at: 创建时间。
        updated_at: 更新时间。
        tasks: 与关联 Task 实体的 1 对多关系。
    """

    __tablename__ = "plans"
    __table_args__ = (
        UniqueConstraint(
            "turn_id",
            "revision",
            name="uq_plans_turn_revision",
        ),
        UniqueConstraint(
            "workflow_id",
            "revision",
            name="uq_plans_workflow_revision",
        ),
    )

    plan_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(100), nullable=False)
    turn_id: Mapped[str] = mapped_column(
        String(100),
        ForeignKey("conversation_turns.turn_id"),
        nullable=False,
    )
    parent_plan_id: Mapped[str | None] = mapped_column(
        String(100),
        ForeignKey("plans.plan_id"),
        nullable=True,
    )
    current_task_id: Mapped[str | None] = mapped_column(
        String(100),
        ForeignKey(
            "tasks.task_id",
            name="fk_plans_current_task_id",
            use_alter=True,
        ),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        default=PlanStatus.PLANNING.value,
    )
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    system_retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    failure_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    tasks = relationship(
        "Task",
        back_populates="plan",
        foreign_keys="Task.plan_id",
    )


Index("idx_plans_turn_status", Plan.turn_id, Plan.status)
Index("idx_plans_workflow_revision", Plan.workflow_id, Plan.revision)
