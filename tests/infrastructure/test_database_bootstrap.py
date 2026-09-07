"""空库初始化必须登记完整结构，并拒绝再次操作已有数据库。"""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from sqlalchemy import create_engine, inspect, text

ROOT = Path(__file__).resolve().parents[2]


class DatabaseBootstrapTest(unittest.TestCase):
    def test_empty_database_seed_revision_and_existing_database_refusal(self):
        with tempfile.TemporaryDirectory() as directory:
            url = "sqlite:///" + (Path(directory) / "bootstrap.db").as_posix()
            env = {**os.environ, "SQLALCHEMY_DATABASE_URL": url,
                   "DASHSCOPE_API_KEY": "bootstrap-test-placeholder", "PYTHONUTF8": "1"}
            # 仅在子进程测试中适配 MySQL 类型；生产初始化仍以 MySQL 为目标。
            harness = """
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.ext.compiler import compiles
compiles(MEDIUMTEXT, 'sqlite')(lambda *args, **kwargs: 'TEXT')
from scripts.bootstrap_database import bootstrap_database
bootstrap_database(seed_demo=True)
"""
            command = [sys.executable, "-c", harness]
            first = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True,
                                   encoding="utf-8", timeout=60)
            self.assertEqual(first.returncode, 0, first.stderr)
            engine = create_engine(url)
            try:
                tables = set(inspect(engine).get_table_names())
                self.assertTrue({"documents", "knowledge_bases", "tasks", "plans",
                                 "outbox_events", "inbox_events", "alembic_version"} <= tables)
                with engine.connect() as connection:
                    self.assertEqual(connection.execute(text("SELECT kb_code FROM knowledge_bases")).scalar_one(),
                                     "docflow_demo")
                    revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                    self.assertTrue(revision)
                second = subprocess.run(command, cwd=ROOT, env=env, capture_output=True,
                                        text=True, encoding="utf-8", timeout=60)
                self.assertNotEqual(second.returncode, 0)
                self.assertIn("初始化要求空库", second.stderr)
                with engine.connect() as connection:
                    self.assertEqual(connection.execute(text("SELECT COUNT(*) FROM knowledge_bases")).scalar_one(), 1)
                    self.assertEqual(connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one(), revision)
            finally:
                engine.dispose()
