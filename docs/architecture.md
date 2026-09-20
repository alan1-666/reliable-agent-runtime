# Agent Runtime 架构

## 1. 总体架构

```text
Lifecycle Platform / Business App / CLI
                    │ CreateRun
                    ▼
              Run API + Auth
                    │
          PostgreSQL Run Queue
                    │ lease + fence
                    ▼
                Run Worker
                    │
        ┌───────────┼────────────┐
        ▼           ▼            ▼
 Context Builder  Model Adapter  Policy Engine
        │           │            │
        └────── Agent Loop ──────┘
                    │ Tool Call
                    ▼
        Tool Registry / Scheduler
          ├── In-process Tool
          ├── MCP stdio
          └── MCP HTTP
                    │
       Evidence + Trace + Checkpoint
                    │
           PostgreSQL + Outbox
                    │
              SSE / gRPC Events
```

## 2. 请求生命周期

1. `CreateRun` 校验身份、租户、Manifest checksum 和 `Idempotency-Key`。
2. 同一事务写入 Run、初始 Event 和 Outbox。
3. Worker 通过 `FOR UPDATE SKIP LOCKED` 获取任务，写 `lease_owner、lease_until、fence_token`。
4. Context Builder 加载输入、Manifest、Checkpoint 和允许的上下文引用。
5. Agent Loop 调用 Model Adapter，解析文本增量和完整 Tool Call。
6. Policy Engine 对完整参数做 Schema、权限、环境和风险校验。
7. Tool Scheduler 执行工具并把结果归一化为 Evidence。
8. 每轮保存 Event、Usage 和必要 Checkpoint，再进入下一轮。
9. 达成目标、无足够证据、预算耗尽、取消或错误后写入终态。
10. SSE/gRPC 消费者按 `(run_id, seq)` 获取事件，断线后从最后 seq 续传。

## 3. Agent Loop

逻辑阶段：

```text
BUILD_CONTEXT
  → CALL_MODEL
  → PARSE_OUTPUT
  ├── FINALIZE
  └── PLAN_TOOLS
        → POLICY_CHECK
        ├── WAIT_APPROVAL
        └── EXECUTE_TOOLS
              → SAVE_EVIDENCE
              → CHECKPOINT
              → BUILD_CONTEXT
```

所有循环都受 `max_turns、max_tokens、max_tool_calls、deadline、cost_budget` 限制。

## 4. Model Adapter

统一接口负责：

- 流式文本和 Tool Call 增量组装；
- Structured Output 校验；
- Provider 错误归一化；
- Token/成本统计；
- 超时、取消和有限重试；
- 能力声明：tools、JSON schema、vision、context window。

第一版通过 OpenAI-compatible Chat Completions Adapter 接 DeepSeek；API Key 仅从环境变量或 Secret Manager 注入。Fallback 必须确保候选模型满足原模型所需能力，且每个 Run 最多降级一次，避免递归重试。

## 5. Tool Registry 与 Scheduler

每个工具声明：

```text
name + version
input/output schema
risk level
side-effect class
resource key extractor
timeout/retry policy
required scopes
idempotency capability
```

调度规则：

- 无依赖且只读的工具允许有界并行。
- 写工具按资源键串行；资源键不能确定时默认全局串行。
- 工具完整参数组装完成后才进入 Policy，禁止校验流式半成品。
- 第一版在 Runtime 内校验 `type/required/properties/additionalProperties/items/enum` 子集；正式接入外部 Tool Schema 前替换或补齐完整 JSON Schema 实现。
- 写工具必须具有 scoped grant 和业务幂等键。
- 未声明幂等能力的写工具超时后状态标记 `UNKNOWN`，先查询结果，不能盲目重试。

## 6. Context 与 Evidence

Context 分为 system policy、Manifest assets、conversation、checkpoint summary、tool evidence 五层。接近窗口上限时，优先丢弃可重取的大块原文，保留约束、ID、结论、未决项和 Evidence 引用。

工具结果不能直接等同事实。Evidence 记录来源、环境、时间窗、工具版本、摘要、内容哈希、权限级别和原文引用。最终关键断言必须引用 Evidence ID。

## 7. 持久化模型

| 表 | 用途 |
|---|---|
| `runs` | 任务、状态、Manifest 快照、预算、Deadline |
| `run_attempts` | 每次 Worker 执行、Lease、Fence、错误 |
| `run_events` | 单调 seq 的领域事件 |
| `checkpoints` | 可恢复状态和上下文摘要 |
| `tool_calls` | 工具参数摘要、状态、业务幂等键 |
| `approvals` / `grants` | HITL 请求和一次性授权 |
| `evidence` | 归一化证据及原文引用 |
| `usage_records` | Token、模型、工具、延迟和成本 |
| `outbox_events` | 可靠发布事件 |

关键唯一约束：

- `(tenant_id, idempotency_key)`；
- `(run_id, seq)`；
- `(run_id, tool_call_id)`；
- `(tool_name, business_idempotency_key)`；
- 每个 Run 只允许一个有效 Lease/Fence。

当前持久化写入遵循三条硬约束：

1. `CreateRun` 同事务写入 Run、`RUN_CREATED` 与 Outbox；相同幂等键和相同请求返回原 Run，不同请求报冲突。
2. Worker 写 Event、Checkpoint 和终态时必须同时匹配 `lease_owner + fence_token + lease_until`，旧 Worker 即使恢复也无法提交。
3. `last_event_seq` 在锁定 Run 行后递增，Event 与 Outbox 同事务落库；Checkpoint 只能引用已提交的 Event seq，不能倒退或超前。

对应实现见 [`internal/store`](../internal/store) 和 [`migrations`](../migrations)。

## 8. API

- `POST /v1/runs`：幂等创建 Run。
- `GET /v1/runs/{id}`：状态与最终结果。
- `GET /v1/runs/{id}/events?after_seq=N`：SSE 续传。
- `POST /v1/runs/{id}/cancel`：协作取消。
- `POST /v1/runs/{id}/approvals/{approval_id}`：批准或拒绝。
- `GET /v1/runs/{id}/trace`：脱敏 Trace。
- `POST /v1/runs/{id}/replay`：基于固定快照重放，仅允许 EVAL/REPLAY。

## 9. 安全

- 租户、用户、环境和 scopes 从认证上下文传入。
- Manifest 工具白名单与调用者权限取交集，不允许模型扩大权限。
- Prompt Injection 只能影响模型建议，不能绕过 Policy Engine。
- Secret 由执行时注入工具适配器，永不进入模型 Context 和 Trace。
- 生产写操作默认 HITL，并记录审批人、范围、有效期和消费状态。

## 10. 可观测

OpenTelemetry Span 至少覆盖 API、排队、context、model、policy、tool、checkpoint 和 finalize。Prometheus 指标包括队列深度、Run 终态、恢复次数、模型/工具错误、TTFT、p95、Token、成本、重复调用、审批等待和无 Evidence 终态。
