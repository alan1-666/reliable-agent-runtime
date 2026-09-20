# Run 状态机与失败窗口

## 1. Run 状态

```text
QUEUED
  → RUNNING
  → WAITING_APPROVAL ──approve──▶ QUEUED
  → RETRY_WAIT ─────────timer───▶ QUEUED
  → SUCCEEDED
  → INCONCLUSIVE
  → FAILED
  → TIMED_OUT
  → CANCELLED
```

终态不可逆。恢复不会把终态改回 RUNNING，而是显式创建 Replay 或新 Run。

## 2. Attempt 与 Lease

Run 是用户可见任务，Attempt 是一次 Worker 执行。Worker 获取任务时：

1. 锁定可执行 Run；
2. 增加 `fence_token`；
3. 创建 Attempt；
4. 写 Lease 到期时间；
5. 周期续租。

任何写入都带当前 fence token。旧 Worker 即使恢复网络，也会因 token 过期而拒绝写入，防止双执行。

## 3. Checkpoint 边界

Checkpoint 至少包含：

- 当前 turn 和阶段；
- 已消费预算；
- Context 摘要与未决项；
- 已完成 Tool Call ID；
- Evidence refs；
- 待审批请求；
- Manifest checksum。

保存原则：模型调用前、工具调度前、工具结果落库后、进入等待审批前、终态前。

## 4. 失败窗口

### 创建 Run 后 API 丢回包

客户端使用同一个 Idempotency-Key 重试，服务返回已有 Run。

### Worker 获取任务后崩溃

Lease 到期后新 Worker 创建新 Attempt，从最近 Checkpoint 恢复；旧 Worker 被 fence。

### 模型返回成功但结果未落库

模型调用通常不具备业务副作用。新 Attempt 可以重调，但要记录重复成本；若供应商支持 request id 查询则优先查询。

### 只读工具成功但结果未落库

可以使用同一 Tool Call ID 重试，结果作为新 Evidence 版本保存。

### 写工具成功但回包丢失

状态进入 `UNKNOWN`。必须用业务幂等键查询外部系统；确认未执行才能重试。禁止仅凭超时判断失败。

### 审批通过后服务崩溃

审批决定与一次性 Grant 在同一事务落库；Grant 使用唯一消费记录，恢复后最多消费一次。

### 最终结果落库但通知失败

Run 终态和 Outbox 同事务提交。Publisher 重试发送；消费者按事件 ID 去重。

## 5. 取消和 Deadline

- API 写入 `cancel_requested_at`，Worker 周期检查并取消根 context。
- Model/Tool Adapter 必须接收 context；不支持取消的调用返回后也不能继续提交副作用。
- 超过 Deadline 进入 `TIMED_OUT`，与用户主动 `CANCELLED` 区分。
- 取消写工具时仍需查询最终业务结果，不能假设请求没有执行。

## 6. 熔断规则

首期实现三种确定性规则：

1. 规范化后的同工具同参数连续出现 3 次；
2. 连续 3 个 turn 没有新增 Evidence；
3. 同类可重试错误连续达到阈值。

触发后停止工具循环，输出 `INCONCLUSIVE` 或 `FAILED`，并写明触发规则和已有 Evidence。
