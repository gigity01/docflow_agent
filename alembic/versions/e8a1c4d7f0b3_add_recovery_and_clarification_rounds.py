"""消息失败恢复记录与多轮澄清。"""

from alembic import op
import sqlalchemy as sa

revision = "e8a1c4d7f0b3"
down_revision = "9a7c5e3d1b2f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if inspector.has_table("plans"):
        with op.batch_alter_table("plans") as batch:
            batch.add_column(sa.Column("system_retry_count", sa.Integer(), nullable=False, server_default="0"))
    if inspector.has_table("clarification_requests"):
        with op.batch_alter_table("clarification_requests") as batch:
            batch.drop_constraint("uq_clarification_requests_source_turn", type_="unique")
            batch.add_column(sa.Column("round", sa.Integer(), nullable=False, server_default="1"))
            batch.add_column(sa.Column("answer_text", sa.Text(), nullable=True))
            batch.create_unique_constraint("uq_clarification_requests_turn_round", ["source_turn_id", "round"])
    if inspector.has_table("outbox_events"):
        op.add_column("outbox_events", sa.Column("origin_event_id", sa.String(100), nullable=True))
    if inspector.has_table("inbox_events"):
        with op.batch_alter_table("inbox_events") as batch:
            batch.alter_column("processed_at", existing_type=sa.DateTime(), nullable=True)
            for column in (
                sa.Column("status", sa.String(30), nullable=False, server_default="processed"),
                sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
                sa.Column("available_at", sa.DateTime(), nullable=True),
                sa.Column("event_type", sa.String(100), nullable=True),
                sa.Column("payload_json", sa.JSON(), nullable=True),
                sa.Column("plan_id", sa.String(100), nullable=True),
                sa.Column("error_code", sa.String(100), nullable=True),
                sa.Column("error_message", sa.String(1000), nullable=True),
                sa.Column("recovery_count", sa.Integer(), nullable=False, server_default="0"),
            ):
                batch.add_column(column)
            batch.create_index("ix_inbox_events_plan_id", ["plan_id"])


def downgrade() -> None:
    raise RuntimeError("多轮澄清不能无损退回单轮结构；测试环境请重建空库")
