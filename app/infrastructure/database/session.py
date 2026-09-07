"""SQLAlchemy 数据库引擎、会话工厂与 FastAPI 会话依赖注入模块。

职责说明：
- 创建全局共享的 SQLAlchemy `engine`，配置连接池大小、健康探测 (pool_pre_ping) 与超时重连。
- 提供全局 `sessionmaker` 工厂 `session_local`。
- 提供面向 FastAPI HTTP 请求生命周期的数据库会话生成器 `get_db`。
"""

from collections.abc import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config.settings import SQLALCHEMY_DATABASE_URL
from app.infrastructure.database.base import Base
from app.infrastructure.database.connection import connection_options

# 初始化全局 SQLAlchemy 数据库引擎
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args=connection_options(SQLALCHEMY_DATABASE_URL),
    pool_size=10,
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False,
)

# 初始化全局会话工厂，关闭自动提交/自动刷新，保持事务显式可控
session_local = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


def get_db() -> Generator[Session, None, None]:
    """FastAPI 依赖注入生成器：按 HTTP 请求提供数据库 Session 实例并在请求结束时确保关闭。

    返回:
        Generator[Session, None, None]: 活跃的数据库 Session 对象。
    """
    db = session_local()
    try:
        yield db
    finally:
        db.close()
