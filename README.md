# Agent Runtime

Agent 执行引擎与任务中心是数据面。它接收一个不可变 Release Manifest 和任务输入，可靠、安全地完成一次 Agent Run，并产生可追踪、可恢复、可评测的事件、Evidence 和最终结果。

## 不是什么

- 不是 Agent 配置后台；配置和发布属于 Lifecycle Platform。
- 不是 Coding Agent 产品；Coding、Ops、客服等只是不同上层应用。
- 不是模型代理转发层；它管理完整 Run 生命周期和工具副作用。
- 第一版不是微服务集群，先做模块化单体和数据库任务队列。

## 核心能力

1. Run/Attempt 状态机和幂等创建。
2. Model Adapter、Streaming 和 Context 管理。
3. Tool Registry、MCP、并发调度和副作用控制。
4. Policy/HITL、预算、Deadline、取消和熔断。
5. Checkpoint、Lease/Fencing、恢复和 Outbox。
6. Trace Event、Evidence、Usage 和最终结构化输出。

## 文档

- [系统架构](docs/architecture.md)
- [状态机与失败窗口](docs/state-machine.md)
- [开发计划](docs/development-plan.md)

## 计划代码结构

```text
agent-runtime/
├── cmd/api/                 # Run API、SSE、审批 API
├── cmd/worker/              # 获取 Lease 并执行 Run
├── internal/
│   ├── run/                 # Run/Attempt 状态机
│   ├── engine/              # Agent Loop
│   ├── httpapi/             # Run API 与 SSE
│   ├── worker/              # Lease、续租和持久化编排
│   ├── outbox/              # 可靠事件发布与重试
│   ├── model/               # Model Adapter
│   ├── context/             # Context 构建和压缩
│   ├── tool/                # Registry、Scheduler、MCP
│   ├── policy/              # Guard、HITL、Grant
│   ├── evidence/            # Evidence 归一化和引用
│   ├── trace/               # Event Bus、OTel
│   └── platform/            # DB、Outbox、Clock、ID、Auth
├── api/                     # OpenAPI / protobuf
├── migrations/
├── tests/
└── docs/
```

## 当前状态

`PHASE-2 PARTIAL`：已完成可替换 Model Adapter、Agent Loop、Tool Registry、预算与取消传播，以及 PostgreSQL 幂等任务、Lease/Fencing、Event/Checkpoint 和终态事务。Engine Checkpoint v1 可恢复消息、turn、预算与 Tool 进度；新 Worker 能从安全边界继续，且不会自动重放状态未知的写 Tool。HTTP API 支持创建/查询 Run 和 SSE 续传。Outbox Publisher 已实现租约、token fencing、发布超时和指数退避。下一步是写 Tool 的幂等查询恢复、真实认证和生产消息 Sink。

```bash
GOCACHE=/tmp/safemarket-agent-go-cache \
GOTMPDIR=/tmp/safemarket-agent-go-tmp \
go test ./...

GOCACHE=/tmp/safemarket-agent-go-cache \
GOTMPDIR=/tmp/safemarket-agent-go-tmp \
go run ./cmd/demo
```

运行真实 PostgreSQL 集成测试：

```bash
docker compose up -d --wait

TEST_DATABASE_URL='postgres://agent_runtime:agent_runtime_dev@127.0.0.1:54329/agent_runtime?sslmode=disable' \
GOCACHE=/tmp/safemarket-agent-go-cache \
GOTMPDIR=/tmp/safemarket-agent-go-tmp \
go test -v ./internal/store/postgres

docker compose stop
```

真实 DeepSeek 联调时仅通过环境变量传入密钥：

```bash
export DEEPSEEK_API_KEY='...'
export DEEPSEEK_BASE_URL='https://api.deepseek.com'
export DEEPSEEK_MODEL='deepseek-chat'
go run ./cmd/deepseek-demo
```

运行带 PostgreSQL、Lease/Fencing 和事件持久化的完整演示：

```bash
docker compose up -d --wait
docker compose exec -T postgres \
  psql -U agent_runtime -d agent_runtime \
  -f /migrations/001_runtime.up.sql
docker compose exec -T postgres \
  psql -U agent_runtime -d agent_runtime \
  -f /migrations/002_outbox_leasing.up.sql

export DEEPSEEK_API_KEY='...'
go run ./cmd/persistent-demo
```

演示会创建唯一 Run，由 Worker 获取 Lease 后执行一次 Tool Calling，并从数据库按 seq 输出完整事件。重复执行 migration 会因表已存在而失败；本地需要重建时再显式执行 down migration。

也可以分别启动 API 和 Worker：

```bash
# terminal 1
go run ./cmd/api

# terminal 2
export DEEPSEEK_API_KEY='...'
go run ./cmd/worker
```

创建任务并订阅事件：

```bash
curl -X POST http://127.0.0.1:8080/v1/runs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: local-demo' \
  -H 'Idempotency-Key: demo-request-1' \
  -d '{"input":"Check order-service health","mode":"EVAL","manifest":{"release_id":"demo-v1","checksum":"sha256:demo-v1"}}'

curl -N 'http://127.0.0.1:8080/v1/runs/<run_id>/events?after_seq=0' \
  -H 'X-Tenant-ID: local-demo'
```

`X-Tenant-ID` 目前仅用于本地开发；生产环境必须由认证中间件提供可信租户身份。

不要提交 `.env` 或在日志、Trace、Release Manifest 中记录 API Key。模型名和价格可能变化，以 DeepSeek 官方文档和账户控制台为准。
