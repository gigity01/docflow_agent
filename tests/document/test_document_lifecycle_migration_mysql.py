"""真实 MySQL 环境下的文档生命周期 Alembic 迁移（e7b3c2d4a9f1）集成测试。

核心业务不变量：
1. 真实 MySQL DDL 执行：
   - 验证升级过程中成功添加列、填充存量 active_content_hash、创建复合唯一键 `(kb_id, active_content_hash)`。
2. MySQL NULL 唯一性语义验证：
   - 验证在同一知识库下，多个 `active_content_hash=NULL` 的失效文档可以共存，而相同非 NULL 哈希的文档在插入时触发 IntegrityError。
3. 降级回滚：
   - 验证降级操作安全删除唯一索引与新增的三状态列。
"""

from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
MYSQL_TEST_URL_ENV = "TEST_MYSQL_DATABASE_URL"
PREVIOUS_REVISION = "c5f12a3e9b71"
MIGRATION_COLUMNS = {
    "active_content_hash",
    "lifecycle_status",
    "storage_status",
}


class DocumentLifecycleMigrationMySQLTest(unittest.TestCase):
    """验证实际 MySQL DDL、唯一索引 NULL 语义和 downgrade。"""

    @classmethod
    def setUpClass(cls) -> None:
        database_url = os.getenv(MYSQL_TEST_URL_ENV)
        if not database_url:
            raise unittest.SkipTest(
                f"未设置 {MYSQL_TEST_URL_ENV}，跳过 MySQL migration 集成测试"
            )

        from sqlalchemy import create_engine, inspect
        from sqlalchemy.engine import make_url
        from sqlalchemy.exc import IntegrityError

        url = make_url(database_url)
        if url.drivername != "mysql+pymysql":
            raise RuntimeError(
                f"{MYSQL_TEST_URL_ENV} 必须使用 mysql+pymysql 驱动"
            )
        if not url.database or not url.database.lower().endswith("_test"):
            raise RuntimeError(
                f"{MYSQL_TEST_URL_ENV} 必须指向名称以 _test 结尾的专用测试库"
            )

        cls.database_url = database_url
        cls.inspect = inspect
        cls.integrity_error = IntegrityError
        cls.engine = create_engine(database_url, pool_pre_ping=True)

        existing_tables = inspect(cls.engine).get_table_names()
        if existing_tables:
            cls.engine.dispose()
            raise RuntimeError(
                "MySQL migration 集成测试要求空测试库；"
                f"当前存在表: {', '.join(sorted(existing_tables))}"
            )

    @classmethod
    def tearDownClass(cls) -> None:
        if hasattr(cls, "engine"):
            cls.engine.dispose()

    def setUp(self) -> None:
        self.addCleanup(self._drop_test_tables)
        with self.engine.begin() as connection:
            connection.exec_driver_sql(
                """
                CREATE TABLE documents (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    kb_id BIGINT NOT NULL,
                    doc_code VARCHAR(100) NOT NULL,
                    content_hash VARCHAR(128) NOT NULL,
                    status VARCHAR(30) NOT NULL,
                    PRIMARY KEY (id),
                    UNIQUE KEY uk_documents_doc_code (doc_code)
                ) ENGINE=InnoDB
                """
            )
            connection.exec_driver_sql(
                """
                CREATE TABLE parent_blocks (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    doc_id BIGINT NOT NULL,
                    block_index INT NOT NULL,
                    PRIMARY KEY (id)
                ) ENGINE=InnoDB
                """
            )
            connection.exec_driver_sql(
                """
                CREATE TABLE child_chunks (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    parent_id BIGINT NOT NULL,
                    doc_id BIGINT NOT NULL,
                    chunk_index INT NOT NULL,
                    vector_status VARCHAR(30) NOT NULL,
                    status VARCHAR(30) NOT NULL,
                    PRIMARY KEY (id)
                ) ENGINE=InnoDB
                """
            )

        result = self._run_alembic("upgrade", PREVIOUS_REVISION)
        if result.returncode != 0:
            self.fail(f"无法准备 migration 前置版本:\n{result.stderr}")

    def _drop_test_tables(self) -> None:
        with self.engine.begin() as connection:
            for table_name in (
                "document_artifacts",
                "child_chunks",
                "parent_blocks",
                "documents",
                "alembic_version",
            ):
                connection.exec_driver_sql(f"DROP TABLE IF EXISTS {table_name}")

    def _run_alembic(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment["SQLALCHEMY_DATABASE_URL"] = self.database_url
        environment["DASHSCOPE_API_KEY"] = "migration-test-placeholder"
        environment["PYTHONUTF8"] = "1"
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "alembic",
                "-c",
                str(ROOT_DIR / "alembic.ini"),
                *arguments,
            ],
            cwd=ROOT_DIR,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )

    def _column_names(self) -> set[str]:
        return {
            column["name"]
            for column in self.inspect(self.engine).get_columns("documents")
        }

    def _current_revision(self) -> str:
        with self.engine.connect() as connection:
            return connection.exec_driver_sql(
                "SELECT version_num FROM alembic_version"
            ).scalar_one()

    def test_upgrade_unique_null_semantics_and_downgrade(self) -> None:
        upgrade_result = self._run_alembic("upgrade", "e7b3c2d4a9f1")
        self.assertEqual(upgrade_result.returncode, 0, upgrade_result.stderr)
        self.assertTrue(MIGRATION_COLUMNS.issubset(self._column_names()))

        with self.engine.begin() as connection:
            connection.exec_driver_sql(
                """
                INSERT INTO documents
                    (kb_id, doc_code, content_hash, status, active_content_hash)
                VALUES
                    (7, 'DOC_ACTIVE', 'hash-a', 'uploaded', 'hash-a'),
                    (7, 'DOC_INACTIVE_1', 'inactive-1', 'uploaded', NULL),
                    (7, 'DOC_INACTIVE_2', 'inactive-2', 'uploaded', NULL)
                """
            )
            defaults = connection.exec_driver_sql(
                """
                SELECT lifecycle_status, storage_status
                FROM documents
                WHERE doc_code = 'DOC_ACTIVE'
                """
            ).mappings().one()
        self.assertEqual(defaults["lifecycle_status"], "active")
        self.assertEqual(defaults["storage_status"], "active")

        with self.assertRaises(self.integrity_error):
            with self.engine.begin() as connection:
                connection.exec_driver_sql(
                    """
                    INSERT INTO documents
                        (kb_id, doc_code, content_hash, status,
                         active_content_hash)
                    VALUES
                        (7, 'DOC_DUPLICATE', 'hash-a', 'uploaded', 'hash-a')
                    """
                )

        unique_constraints = {
            constraint["name"]
            for constraint in self.inspect(self.engine).get_unique_constraints(
                "documents"
            )
        }
        self.assertIn("uq_documents_kb_active_content_hash", unique_constraints)

        downgrade_result = self._run_alembic("downgrade", PREVIOUS_REVISION)
        self.assertEqual(downgrade_result.returncode, 0, downgrade_result.stderr)
        self.assertTrue(MIGRATION_COLUMNS.isdisjoint(self._column_names()))
        self.assertEqual(self._current_revision(), PREVIOUS_REVISION)


if __name__ == "__main__":
    unittest.main()
