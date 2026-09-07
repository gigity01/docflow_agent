"""Clarification source_turn_id 唯一约束 Alembic 迁移（9a7c5e3d1b2f）测试。

核心业务不变量（遵循 AGENTS.md 规范）：
1. 一轮对话仅存在一个 Clarification 实体：
   - 保证同一个 `source_turn_id` 在 `clarification_requests` 表中仅能存在唯一记录，创建 `uq_clarification_requests_source_turn` 唯一约束。
2. 降级回退：
   - 验证降级操作能够安全删除该唯一约束。
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest
from unittest import mock

import sqlalchemy as sa

from app.modules.clarification.infrastructure.persistence.models import (
    ClarificationRequest,
)


ROOT_DIR = Path(__file__).resolve().parents[2]
MIGRATION_PATH = (
    ROOT_DIR
    / "alembic"
    / "versions"
    / "9a7c5e3d1b2f_add_clarification_source_turn_unique.py"
)


def _load_migration():
    """动态加载 Clarification source_turn 唯一约束迁移脚本。"""
    spec = importlib.util.spec_from_file_location(
        "clarification_source_turn_migration_under_test",
        MIGRATION_PATH,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("无法加载 Clarification source Turn migration")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ClarificationSourceTurnMigrationTest(unittest.TestCase):
    """验证 uq_clarification_requests_source_turn 约束的创建、幂等降级与 ORM 映射。"""
    def test_upgrade_and_downgrade_manage_unique_constraint(self) -> None:
        migration = _load_migration()
        operations = mock.Mock()
        operations.get_bind.return_value.execute.return_value.first.return_value = (
            None
        )
        migration.op = operations

        migration.upgrade()
        migration.downgrade()

        operations.create_unique_constraint.assert_called_once_with(
            "uq_clarification_requests_source_turn",
            "clarification_requests",
            ["source_turn_id"],
        )
        operations.drop_constraint.assert_called_once_with(
            "uq_clarification_requests_source_turn",
            "clarification_requests",
            type_="unique",
        )
        self.assertEqual(migration.down_revision, "4ce8fd45dde4")

    def test_upgrade_fails_closed_when_existing_rows_are_duplicated(
        self,
    ) -> None:
        migration = _load_migration()
        engine = sa.create_engine("sqlite://")
        self.addCleanup(engine.dispose)
        metadata = sa.MetaData()
        table = sa.Table(
            "clarification_requests",
            metadata,
            sa.Column("clarification_id", sa.String(100), primary_key=True),
            sa.Column("source_turn_id", sa.String(100), nullable=False),
        )
        metadata.create_all(engine)

        with engine.begin() as connection:
            connection.execute(
                table.insert(),
                [
                    {
                        "clarification_id": "clarification-1",
                        "source_turn_id": "turn-1",
                    },
                    {
                        "clarification_id": "clarification-2",
                        "source_turn_id": "turn-1",
                    },
                ],
            )
            operations = mock.Mock()
            operations.get_bind.return_value = connection
            migration.op = operations

            with self.assertRaisesRegex(
                RuntimeError,
                "存在重复 source_turn_id",
            ):
                migration.upgrade()

            operations.create_unique_constraint.assert_not_called()

    def test_orm_declares_turn_round_unique_constraint(self) -> None:
        constraints = {constraint.name: list(constraint.columns.keys())
                       for constraint in ClarificationRequest.__table__.constraints
                       if isinstance(constraint, sa.UniqueConstraint)}
        self.assertNotIn("uq_clarification_requests_source_turn", constraints)
        self.assertEqual(constraints["uq_clarification_requests_turn_round"], ["source_turn_id", "round"])
