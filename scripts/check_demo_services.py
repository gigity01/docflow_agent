"""校验演示库完整结构、Redis 通信和 Qdrant 点写入；不调用模型。"""

from pathlib import Path
import sys
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    from alembic.config import Config
    from alembic.migration import MigrationContext
    from alembic.script import ScriptDirectory
    from qdrant_client import QdrantClient, models
    from redis import Redis
    from sqlalchemy import create_engine, inspect, text

    from app.config.settings import SQLALCHEMY_DATABASE_URL, REDIS_URL, QDRANT_URL
    from app.infrastructure.database.base import Base
    from app.infrastructure.database.model_registry import load_all_models

    load_all_models()
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            expected_tables = set(Base.metadata.tables)
            if not expected_tables <= set(inspector.get_table_names()):
                raise RuntimeError("数据库缺少 ORM 表")
            for table in Base.metadata.sorted_tables:
                actual_columns = {column["name"] for column in inspector.get_columns(table.name)}
                if set(table.columns.keys()) != actual_columns:
                    raise RuntimeError(f"表结构与 ORM 不一致：{table.name}")
            config = Config(str(ROOT / "alembic.ini"))
            config.set_main_option("script_location", str(ROOT / "alembic"))
            expected = ScriptDirectory.from_config(config).get_current_head()
            if MigrationContext.configure(connection).get_current_revision() != expected:
                raise RuntimeError("数据库版本不是 Alembic head")
            if connection.execute(text("SELECT kb_code FROM knowledge_bases WHERE id=1")).scalar_one() != "docflow_demo":
                raise RuntimeError("演示种子数据不正确")
    finally:
        engine.dispose()
    print("MySQL schema, revision and demo seed: OK")

    redis = Redis.from_url(REDIS_URL, socket_timeout=5, socket_connect_timeout=5)
    stream = f"docflow-demo-check:{uuid4().hex}"
    try:
        redis.ping()
        redis.xadd(stream, {"event": "demo-check"})
        redis.expire(stream, 60)
        if len(redis.xrange(stream)) != 1:
            raise RuntimeError("Redis Streams 读写失败")
    finally:
        redis.delete(stream)
        redis.close()
    print("Redis Streams round-trip: OK")

    client = QdrantClient(url=QDRANT_URL, timeout=5)
    collection = f"docflow_demo_check_{uuid4().hex}"
    created = False
    try:
        for attempt in range(30):
            try:
                client.get_collections()
                break
            except Exception:
                if attempt == 29:
                    raise
                time.sleep(1)
        client.create_collection(collection, vectors_config=models.VectorParams(size=2, distance=models.Distance.COSINE))
        created = True
        client.upsert(collection, points=[models.PointStruct(id=1, vector=[1.0, 0.0])], wait=True)
        if len(client.retrieve(collection, ids=[1])) != 1:
            raise RuntimeError("Qdrant 点写入未读回")
    finally:
        if created:
            client.delete_collection(collection)
        client.close()
    print("Qdrant point round-trip: OK (synthetic vector; no embedding model)")


if __name__ == "__main__":
    main()
