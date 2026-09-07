# DocFlow Agent

**文档任务智能编排后端**：通过自然语言发起文档处理任务，由多个职责受限的 Agent 协作完成取证、规划与执行，并由持久化任务运行时管理状态和失败恢复。

例如，对于“把这份文档处理并建立向量索引”的请求，系统先确认文档及其处理状态，再规划所需步骤；信息不明确时发起澄清，任务发布后按依赖顺序异步执行。

## 核心能力

- **文档处理**：文档处理与转换、父子分块、向量生成及 Qdrant 索引写入。
- **上下文管理**：管理 Conversation、Turn 和 Context Chain，通过 Context Selection 选择相关历史上下文。
- **协作规划**：Evidence 收集可验证事实，Gap Handler 判断补查、澄清或失败分支，Commit 创建并发布 Plan / Task。
- **受限执行**：按任务能力分配 Executor 工具，区分只读查询与有副作用的命令。
- **失败恢复**：任务抢占与执行归属校验、补偿、重试、重新规划，以及补偿耗尽后的锁定。
- **状态查询与审计**：通过 HTTP API 查询轮次和任务结果，并记录工具调用及文档流水线事件。

## 工作流程

```text
用户请求
  → Context Selection：选择相关上下文
  → Evidence：查询文档、上下文及运行状态
  → Gap Handler：存在缺口时补查、澄清或进入失败分支
  → Commit：创建任务及依赖，发布计划
  → Task Runtime：调度受限 Executor 执行
  → 持久化结果；失败时进入补偿、重试或重新规划
```

证据充分时可直接进入 Commit。规划阶段决定做什么，执行阶段在已注册能力内完成操作；业务结果以工具输出和数据库状态为准。

## 技术栈

Python · FastAPI · OpenAI Agents SDK · LangGraph · SQLAlchemy / Alembic · MySQL · Redis Streams · Qdrant

模型接入包含 DeepSeek，向量索引能力使用 Qwen Embedding。

## 启动与演示

见 [本地启动指南](docs/quickstart.md)：包含空库初始化、基础服务、API / Worker 启动和可执行演示。

- 无模型密钥：上传 Markdown → 清洗 → 切块 → 查询结果。
- 配置模型后：自然语言发起任务 → 异步执行 → 查看文档索引结果。
- [Backend tests](https://github.com/gigity01/docflow_agent/actions/workflows/tests.yml)：自动测试与真实基础服务检查；验证范围见启动指南。

## 代码导航

- [Agent 实现](app/agents)：取证、缺口处理、规划与受限执行器。
- [业务模块](app/modules)：文档、会话、上下文、规划及任务运行时。
- [运行时与工具注册](app/agent_runtime)：工具描述、调用上下文及审计。
- [业务能力说明](business_docs/service_map.md)：模块职责与规划边界。
- [测试](tests)：业务逻辑、API 契约、任务依赖和失败恢复测试。

## 当前范围

当前版本以**后端 API 和 Agent 执行链路**为主，提供命令行演示，尚未提供前端页面。

目前可执行任务为文档处理、切块和向量索引，不是通用自主执行平台，也尚未提供完整的检索问答产品界面。仓库包含使用模拟组件的集成测试；真实模型及外部服务的运行效果需在配置对应环境后验证。
