# 本地启动与演示

从仓库根目录执行以下命令。需要 Python 3.11+（推荐 3.12）、uv 和 Docker Compose；依赖按 `uv.lock` 安装。
Compose 只启动 MySQL、Redis、Qdrant，API 和 Worker 在主机运行。

## 1. 安装与配置

```sh
uv sync --locked
docker compose up -d --wait
```

复制 `.env.example` 为 `.env`（PowerShell：`Copy-Item .env.example .env`；Linux：`cp .env.example .env`），修改以下值：

```dotenv
SQLALCHEMY_DATABASE_URL=mysql+pymysql://docflow:docflow-local@127.0.0.1:3307/docflow_demo
REDIS_URL=redis://127.0.0.1:6380/0
QDRANT_URL=http://127.0.0.1:6333
DASHSCOPE_API_KEY=local-demo-no-model-calls
DEEPSEEK_API_KEY=
```

以上公开演示凭据仅用于本机 Compose。`local-demo-no-model-calls` 只用于不执行向量生成的演示，不能调用真实 Embedding。
完整索引需要在 `.env` 配置真实 DashScope 密钥；Agent 模式还需要 DeepSeek 密钥，并按账号可用模型设置 `DEEPSEEK_MODEL_NAME`。
修改配置后重启 API 和 Worker。不要提交 `.env`。

示例使用 Markdown，本地即可解析。PDF、Word、PowerPoint 等复杂格式另外需要配置 Docling 服务。

## 2. 初始化全新数据库

```sh
uv run --locked python scripts/bootstrap_database.py --seed-demo
uv run --locked alembic current
uv run --locked python scripts/check_demo_services.py
```

初始化要求空库：注册全部 ORM 模型、创建当前结构、创建 `kb_id=1` 的演示知识库，最后登记 Alembic head。
已有库会被拒绝，不删除、不覆盖任何表。首次初始化期间只运行一个初始化进程。
MySQL DDL 不保证事务回滚；如果初始化中途失败，保留现场检查，改用另一全新演示库重试，不要对未知表结构执行 `stamp head`。

已有项目数据库后续升级使用 `uv run --locked alembic upgrade head`。
历史迁移基线依赖原有表，不能单独用于空库建表；新入口采用 [Alembic 官方建库方案](https://alembic.sqlalchemy.org/en/latest/cookbook.html#building-an-up-to-date-database-from-scratch)。
已有库的历史降级仍受早期基线限制，不把新入口当成历史 schema 重建工具。

服务检查会比对表、列和版本，检查种子数据，并使用随机命名的临时 Redis Stream / Qdrant Collection 验证读写后清理；Qdrant 使用合成向量，不代表验证了模型效果。

## 3. 启动 API

```sh
uv run --locked uvicorn app.main:app --host 127.0.0.1 --port 8000
```

打开 <http://127.0.0.1:8000/docs> 查看接口。另开终端运行无需模型密钥的演示：

```sh
uv run --locked python scripts/demo_document.py --mode direct --skip-index
```

脚本上传 `examples/onboarding.md`，调用清洗和切块接口，再查询流水线与切块统计。
父块和子块数量必须大于 0，成功打印 `DEMO_OK`，同时明确提示未验证模型与向量索引。
每次追加新的演示批次标识，避免重复内容冲突；演示数据会保留在本地。

配置真实 DashScope 密钥并重启 API 后，运行完整文档流水线：

```sh
uv run --locked python scripts/demo_document.py --mode direct
```

脚本要求索引无失败、文档状态为 `indexed`，且所有子块都有向量 ID。该模式会调用收费的 Embedding API。

## 4. 运行完整 Agent 链路

配置真实 DeepSeek 和 DashScope 密钥，重启 API；在同一目录另开终端启动 Worker：

```sh
uv run --locked python -m app.workers.runtime_main
```

然后运行：

```sh
uv run --locked python scripts/demo_document.py --mode agent --timeout 300
```

链路：上传 → 自然语言消息 → Context / Planner → Plan / Task / Outbox → Worker → 聚合 → Turn 查询 → 文档索引结果。
脚本打印实际请求路径、HTTP 状态码、文档 ID、Turn / Plan ID 和结果，不预设模型一定成功。
澄清、失败或超时以非零状态退出；超时不等于取消后台任务，可用输出的 ID 继续查询。
API 与 Worker 必须共享同一个工作目录、数据库、Redis、存储和日志配置。

如遇澄清，在 Swagger 中向同一 `conversation_id` 的 `messages` 接口发送：

```json
{"message": "补充问题所需的信息", "source_turn_id": "上一次响应的 turn_id"}
```

按新响应中的 `turn_id` 继续查询，不能把澄清状态当成执行成功。

## 测试与验证范围

```sh
uv run --locked python scripts/run_tests.py
```

GitHub Actions 自动执行 Python 3.11 / 3.12 测试，以及真实 MySQL 空库初始化、Redis / Qdrant 读写和 HTTP 上传清洗切块演示。
测试入口会覆盖当前进程的数据库、DashScope 和 DeepSeek 配置，不要求本地 `.env` 或真实模型密钥。
现有 `tests/integration/test_runtime_worker_end_to_end.py` 使用模拟模型、内存 Redis 和 SQLite 验证任务依赖、补偿及重规划。
CI 不持有模型密钥，不验证真实 Agent 决策、Embedding 质量，也不宣称证明跨进程故障恢复。
真实 MySQL 历史迁移测试需要独立空测试库 `TEST_MYSQL_DATABASE_URL`（库名以 `_test` 结尾）；普通测试中会明确显示跳过。

停止本地基础服务：`docker compose stop`；容器卷和演示数据保留。
当前 API 尚未提供 HTTP 鉴权，只绑定本机回环地址；公开仓库不代表可以直接作为公网演示服务部署。
