# Agent Runtime 开发计划

## Phase 0：契约和工程骨架

- [x] 初始化 Go module 和测试骨架。
- [x] 定义 Run、Manifest、Event 和 Model Adapter 接口。
- [x] 建立 Fake Model 和可控 Clock，保证基础测试不依赖真实 API。
- [ ] 增加 Tool 接口、配置、结构化日志、迁移和本地 Docker Compose。

验收：一个内存模式 Run 可以用 Fake Model 返回固定 Final Result。**已通过。**

## Phase 1：单进程 Agent Loop

- [x] 四阶段循环的最小状态机和 typed event。
- [x] 完整 Tool Call 契约、Tool Registry 与 Model → Tool → Model 循环。
- [x] 一个只读演示工具与 Scripted Fake Model。
- [x] Tool 参数的 JSON Schema MVP 校验：`type/required/properties/additionalProperties/items/enum`。
- [x] turn/tool-call/token/cost/deadline 预算和 context 取消。
- [x] OpenAI-compatible/DeepSeek 流式 Adapter：文本、Tool Call 拼装、Usage、HTTP 错误和取消。
- [ ] 使用真实 DeepSeek Key 完成端到端联调并保存脱敏证据。
- [ ] Structured Output 校验及完整 JSON Schema 实现或成熟库替换。

验收：CLI/API 能完成“模型 → 工具 → 模型 → Final”的确定性演示；所有事件顺序可断言。**Fake Model 场景已通过，真实模型场景待完成。**

## Phase 2：持久任务

- [x] PostgreSQL Run/Attempt/Event/Checkpoint/Outbox 表结构与迁移。
- [x] 幂等 CreateRun；Run、`RUN_CREATED` 和 Outbox 同事务提交。
- [x] 数据库任务队列、`FOR UPDATE SKIP LOCKED`、Lease/Fencing 和 Attempt 接管。
- [x] Event 单调序号、Checkpoint 单调推进和终态事务。
- [x] 内存并发测试与真实 PostgreSQL 生命周期集成测试。
- [ ] Worker 将持久化事件和 Checkpoint 接入 Agent Loop。
- [ ] SSE `after_seq` 断线续传。
- [ ] Outbox Publisher 与崩溃注入恢复测试。

验收：在模型前、工具前、工具后和终态前杀死 Worker，任务均能按设计恢复且不产生双终态。**存储原语和真实数据库集成测试已通过，Worker 恢复链路待接入。**

## Phase 3：工具治理

- Tool Registry、JSON Schema、风险等级和 scopes。
- 只读工具有界并行、写工具资源锁串行。
- MCP stdio/HTTP Adapter。
- Tool timeout、业务幂等键和 `UNKNOWN` 处理。

验收：并发上限、串行资源锁、越权拒绝、超时未知和重复请求都有自动化测试。

## Phase 4：HITL 与 Evidence

- Approval/Grant 状态机和一次性消费。
- Evidence 归一化、哈希、来源和引用。
- 关键断言与 Evidence refs 的结构化输出。

验收：Run 可以暂停、重启后等待、批准后继续；重复审批不会重复执行写工具。

## Phase 5：Trace、预算与熔断

- OpenTelemetry 和 Usage Record。
- 重复调用、无新 Evidence、连续失败熔断。
- Context 压缩和一次 Model Fallback。
- Prometheus 指标与最小 Dashboard。

验收：能从 Trace 解释每次模型/工具调用、版本、延迟、成本和终止原因。

## Phase 6：Lifecycle Platform 联调

- 校验 Release Manifest checksum。
- EVAL/SHADOW/LIVE/REPLAY 执行模式。
- Eval 批量并发限制和固定 Snapshot。
- 前端 SSE 事件契约联调。

验收：同一个 Dataset 和 Manifest 可重复运行并生成可比较结果；管理端能查看进度、Trace 和终态。

## 必测故障

- API 成功落库但响应丢失；
- Worker Lease 过期和旧 Worker 恢复；
- 模型 429、5xx、超时和流中断；
- MCP 进程崩溃、工具超时和半成功；
- 写工具回包未知；
- 审批重复提交和 Grant 重放；
- SSE 断线重连；
- Context 超限和压缩失败；
- 数据库短暂不可用和 Outbox 重发。

## 第一版明确不做

- 自研向量数据库；
- 多区域容灾；
- 自动无限 Model Fallback；
- 任意工作流 DSL；
- 无人工批准的生产高风险写操作；
- 为了简历数字提前做没有基线的性能优化。
