"""为全新空库创建当前 ORM 结构并登记 Alembic head；拒绝修改已有库。"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def bootstrap_database(*, seed_demo: bool = False) -> None:
    from alembic import command
    from alembic.config import Config
    from alembic.script import ScriptDirectory
    from sqlalchemy import create_engine, inspect

    from app.config.settings import (
        EMBEDDING_MODEL_NAME,
        QDRANT_COLLECTION_NAME,
        SQLALCHEMY_DATABASE_URL,
    )
    from app.infrastructure.database.base import Base
    from app.infrastructure.database.model_registry import load_all_models
    from app.modules.document.infrastructure.persistence.models.knowledge_base import KnowledgeBase

    load_all_models()
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    # 在建表前检查迁移头唯一性，避免部分初始化。
    if ScriptDirectory.from_config(config).get_current_head() is None:
        raise RuntimeError("未找到 Alembic head")
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    try:
        with engine.begin() as connection:
            inspector = inspect(connection)
            if inspector.get_table_names() or inspector.get_view_names():
                raise RuntimeError("初始化要求空库；已有库请按当前版本执行 Alembic 迁移")
            Base.metadata.create_all(connection)
            if seed_demo:
                connection.execute(KnowledgeBase.__table__.insert().values(
                    id=1,
                    kb_code="docflow_demo",
                    name="DocFlow 演示知识库",
                    domain_code="demo",
                    status="active",
                    visibility="private",
                    embedding_model=EMBEDDING_MODEL_NAME,
                    vector_collection=QDRANT_COLLECTION_NAME,
                ))
            config.attributes["connection"] = connection
            command.stamp(config, "head")
    finally:
        engine.dispose()
    print("数据库初始化完成，Alembic 已登记为 head。")
    if seed_demo:
        print("演示知识库：kb_id=1，domain_code=demo")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed-demo", action="store_true", help="创建演示知识库")
    args = parser.parse_args()
    bootstrap_database(seed_demo=args.seed_demo)


if __name__ == "__main__":
    main()
