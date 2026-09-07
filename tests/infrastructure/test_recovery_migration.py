"""新恢复字段和澄清轮次约束可从上一版本升级。"""

import importlib.util
from pathlib import Path
import unittest

from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa


class RecoveryMigrationTest(unittest.TestCase):
    def test_upgrade_keeps_old_records_and_permits_distinct_clarification_rounds(self):
        engine = sa.create_engine("sqlite://")
        source = Path(__file__).resolve().parents[2] / "alembic/versions/e8a1c4d7f0b3_add_recovery_and_clarification_rounds.py"
        spec = importlib.util.spec_from_file_location("closure_migration", source)
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        try:
            with engine.begin() as connection:
                connection.exec_driver_sql("CREATE TABLE plans (plan_id VARCHAR(100) PRIMARY KEY)")
                connection.exec_driver_sql("CREATE TABLE outbox_events (event_id VARCHAR(100) PRIMARY KEY)")
                connection.exec_driver_sql("CREATE TABLE inbox_events (inbox_id VARCHAR(100) PRIMARY KEY, processed_at DATETIME NOT NULL)")
                connection.exec_driver_sql("CREATE TABLE clarification_requests (clarification_id VARCHAR(100) PRIMARY KEY, source_turn_id VARCHAR(100), CONSTRAINT uq_clarification_requests_source_turn UNIQUE (source_turn_id))")
                connection.exec_driver_sql("INSERT INTO clarification_requests VALUES ('q1', 't1')")
                connection.exec_driver_sql("INSERT INTO inbox_events VALUES ('i1', '2026-09-07 00:00:00')")
                migration.op = Operations(MigrationContext.configure(connection))
                migration.upgrade()
                inspector = sa.inspect(connection)
                self.assertIn("system_retry_count", {column["name"] for column in inspector.get_columns("plans")})
                self.assertEqual(connection.exec_driver_sql("SELECT status FROM inbox_events").scalar_one(), "processed")
                connection.exec_driver_sql("INSERT INTO clarification_requests (clarification_id, source_turn_id, round) VALUES ('q2', 't1', 2)")
                with self.assertRaises(sa.exc.IntegrityError):
                    connection.exec_driver_sql("INSERT INTO clarification_requests (clarification_id, source_turn_id, round) VALUES ('q3', 't1', 2)")
        finally:
            engine.dispose()
