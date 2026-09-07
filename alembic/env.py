"""Alembic 迁移运行环境配置模块。

本模块负责在执行数据库迁移（Alembic upgrade / downgrade）时：
1. 显式调用 `load_all_models()` 导入全部 SQLAlchemy ORM 实体，确保 `Base.metadata` 包含完整的数据库模式定义。
2. 从应用核心配置 `app.config.settings.SQLALCHEMY_DATABASE_URL` 动态覆盖 Alembic 配置中的数据库连接串，
   保证迁移工具与 FastAPI / Worker 运行时连接同一数据库实例，避免环境配置漂移。
3. 提供离线（offline）SQL 脚本生成与在线（online）事务性连接执行两种迁移执行模式。
"""

from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

from app.config.settings import SQLALCHEMY_DATABASE_URL
from app.infrastructure.database.base import Base
from app.infrastructure.database.connection import connection_options
from app.infrastructure.database.model_registry import load_all_models


# 加载所有 ORM 模型类，确保 Base.metadata 注册了完整的表结构定义
load_all_models()

# Alembic 配置对象来自 alembic.ini；运行时 URL 由应用统一配置覆盖，避免迁移与服务连接到不同数据库
config = context.config
config.set_main_option("sqlalchemy.url", SQLALCHEMY_DATABASE_URL.replace("%", "%%"))

# 仅在存在 ini 配置文件时初始化 Alembic 自身日志格式
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# 自动生成迁移（autogenerate）时以应用 ORM 的 Base.metadata 为唯一事实来源
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """以离线模式运行迁移：仅输出 SQL DDL 语句，不与数据库建立真实连接。

    用于审查即将执行的 SQL 变更或在受限生产环境中导出迁移脚本。
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """以在线模式运行迁移：创建临时连接池并在受控事务中执行 DDL 迁移。

    使用 NullPool 避免连接池驻留，迁移完成后立即释放数据库连接。
    """
    connection = config.attributes.get("connection")
    if connection is not None:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
        return

    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args=connection_options(SQLALCHEMY_DATABASE_URL),
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
