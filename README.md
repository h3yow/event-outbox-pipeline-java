# Event Outbox Pipeline (Java / Spring Boot)

一套**本地事件投递与异步消费流程**：用**进程内状态**模拟消息代理（分区日志）、
消费位点与死信区，使事件在**重复投递、处理失败与并发消费**下的最终结果可预期。
全部能力无需外部中间件即可在本地运行与验证（`mvn test` / HTTP 接口）。

- Java 21 · Spring Boot 3.4 · 无数据库、无外部消息中间件
- 同聚合事件**稳定有序**消费，不同聚合**并行**且互不阻塞
- **至少一次投递 + 消费端幂等**：重复投递/重启重投/位点回放，业务副作用只生效一次
- 失败按**可配置退避策略**重试，超限转入**可检索死信区**并保留失败原因，**绝不静默丢弃**
- 位点提交/回放在越界或失效时以**可区分原因**明确拒绝

---

## 1. 模块结构

代码位于 `com.github.h3yow.eventoutboxpipelinejava.outbox`：

| 包 | 组件 | 职责 |
|---|---|---|
| `broker` | `InMemoryEventBroker` | 进程内消息代理：按聚合（分区）维护顺序日志；支持按位点读取、日志截断 |
| `offset` | `OffsetStore` / `InvalidOffsetException` / `OffsetRejectReason` | 消费位点存储与合法性校验（拒绝原因可区分） |
| `idempotency` | `IdempotencyStore` | 事件处理终态（`PROCESSED` / `DEAD`）的原子占坑与幂等记录 |
| `retry` | `RetryPolicy` / `BackoffStrategy` | 最大尝试次数 + 固定/指数退避 |
| `dlq` | `DeadLetterQueue` / `DeadLetterEntry` | 可检索死信区：事件、次数、失败类型与原因、状态 |
| `concurrent` | `AffiliatedExecutor` | 同 key 严格串行（FIFO）、不同 key 并行的归属执行器 |
| `handler` | `EventHandler` / `RecordingEventHandler` | 业务处理接口与演示实现（`fail:` 前缀模拟失败） |
| `core` | `OutboxEventPipeline` / `OutboxEvent` / `DeadLetterState` | 投递编排门面与领域模型 |
| `config` | `OutboxAutoConfiguration` / `PipelineProperties` | Spring 自动装配与配置项 |
| `web` | `OutboxController` / `OutboxExceptionHandler` | 本地验证用 HTTP 接口 |

---

## 2. 投递语义

### 2.1 顺序与并发

- 每个 `aggregateId` 对应归属执行器中的一条串行 lane，事件按**入队位点（sequence）**
  从小到大依次处理，因此**同一聚合严格保序**。
- 不同聚合的 lane 由一个共享固定线程池调度，可**真正并行**。
- 某条 lane 的慢任务（含退避等待）**不占用额外线程**，只阻塞该聚合自身；
  无关聚合可被其他工作线程立即领取推进（见测试 `slowEventOnOneAggregateDoesNotBlockOthers`）。

### 2.2 位点（offset）语义

- 位点表示“**下一个待处理事件的下标**”：提交 `n` 表示 `[0, n)` 均已处理完成。
- 位点按“消费者组 + 聚合”维护（默认组 `default-group`）。
- 事件处理**成功**或**转入死信区**后，位点才原子推进到下一个下标；
  处理失败进入退避等待时**不推进**，重试仍从原位点读取。
- 合法提交/回放窗口为 `[earliest, latest+1]`。越界或失效将抛出
  `InvalidOffsetException`，携带可区分的 `OffsetRejectReason`：

  | reason | 含义 |
  |---|---|
  | `NEGATIVE` | 负数位点，任何情况下都不合法 |
  | `EXPIRED` | 位点为正，但对应日志已被截断，永久失效 |
  | `BEYOND_LATEST` | 位点超出当前最新位点 + 1（事件尚不存在） |
  | `UNKNOWN_PARTITION` | 位点属于未知的消费者组/聚合分区 |

### 2.3 幂等：重复投递只生效一次

每次处理尝试先对 `eventId` 做**原子占坑**（`IdempotencyStore.acquire`）：

- 首次处理：占坑成功 → 调用业务处理器 → 成功写终态 `PROCESSED`。
- **重复投递**（重复唤醒、位点 `seek` 回放、消费者 `restart` 后重投）：
  读到 `PROCESSED` 终态 → **跳过业务**、仅推进位点，因此副作用**只生效一次**。
- 处理失败但仍有重试额度时释放占坑（`release`），使下次重试可重新获取。
- 重试耗尽：写终态 `DEAD` 并登记死信。`PROCESSED` 与死信**互斥**，
  不可能出现“成功与死信同时发生”。
- 终态不可逆；只有显式**死信重放**（`replayDeadLetter`）会清除该事件的终态后重新投递。

> 幂等针对“同一 `eventId`”。对同一 `eventId` 的重复**入队**会被
> `InMemoryEventBroker` 以 `IllegalStateException("duplicate eventId: ...")` 明确拒绝，
> 不会静默覆盖。

---

## 3. 重试与死信规则

### 3.1 重试

- `maxAttempts` = **总尝试次数（含首次）**：
  - `1`：不重试，首次失败即进死信；
  - `3`（默认）：最多处理 3 次（首次 + 2 次重试），第 3 次仍失败则进死信。
- 每次失败后按退避策略等待再重试（退避期间该聚合头部事件挂起，不推进位点）：
  - `BackoffStrategy.fixed(delay)`：固定退避；
  - `BackoffStrategy.exponential(initial, multiplier, max)`：
    `initial * multiplier^(attempt-1)`，并以 `max` 封顶。默认 `100ms × 2`，封顶 `5s`。
- 同聚合的失败事件在退避期间，新的入队唤醒会被合并/抑制；定时器到期后自动恢复投递。
  不同聚合的重试彼此独立、并行进行。

### 3.2 死信区

- 重试耗尽后，事件**原子地**进入 `DeadLetterQueue`，并写幂等终态 `DEAD`、推进位点。
- 每个条目保留：原始事件、消费者组、累计尝试次数、**最后失败类型**（异常类全限定名）、
  **失败原因**（异常消息）、失败时间与状态。
- 状态：`OPEN`（默认）→ `REPLAYED`（已重放）/ `DISCARDED`（显式丢弃，有审计记录）。
- 死信可按 `eventId` / 聚合 / 状态**检索**；任何情况下事件都**不会被静默丢弃**。
- 重放：`OutboxEventPipeline.replayDeadLetter(eventId)` 先校验位点（日志若已截断/越界
  会以 `EXPIRED` / `BEYOND_LATEST` 拒绝且不改变任何状态），再把位点 `seek` 回该事件、
  清除其幂等终态并标记死信为 `REPLAYED`，随后按正常流程重新投递。

---

## 4. 配置项（`application.properties`）

```properties
outbox.pipeline.consumer-group=default-group   # 消费者组
outbox.pipeline.max-attempts=3                 # 最大尝试次数（含首次）
outbox.pipeline.initial-backoff=100ms          # 初始退避
outbox.pipeline.backoff-multiplier=2.0         # 退避倍数
outbox.pipeline.max-backoff=5s                 # 退避上限
outbox.pipeline.worker-threads=4               # 并发消费工作线程数
outbox.pipeline.poll-interval=200ms            # 兜底扫描周期（入队/重试到期通常立即唤醒）
```

---

## 5. 本地验证

### 5.1 运行测试

```bash
mvn test
```

覆盖场景（17 个用例，位于 `src/test/.../tests`）：

| 测试类 | 覆盖点 |
|---|---|
| `RetryPolicyTest` | 重试判定依据、固定/指数退避序列与封顶、非法配置拒绝 |
| `OffsetStoreTest` | `NEGATIVE` / `EXPIRED` / `BEYOND_LATEST` 可区分拒绝；截断后回放被拒 |
| `DeliveryOrderingIdempotencyTest` | 同聚合严格保序、异聚合并行；重复投递/位点回放/重启只生效一次；重复 eventId 被拒 |
| `RetryAndDeadLetterTest` | 临时失败重试成功；永久失败按上限进死信且原因可检索；后续事件不被阻塞；死信重放成功 |
| `ConcurrentConsumptionTest` | 多生产者 × 多工作线程并发，无重复/丢失、位点连续、顺序稳定；慢事件不阻塞无关聚合 |
| `ConcurrentFailureConsistencyTest` | 并发叠加混合成功/间歇失败/永久失败，重试计数、位点、死信与成功态全局一致 |

测试日志会打印**事件标识、聚合、位点、`attempt/maxAttempts` 与重试/死信判定依据**，例如：

```
handling event: eventId=e2, aggregate=order-1, offset=1, attempt=1/3
processing failed -> RETRY: eventId=e2, ..., offset=1, attempt=1/3, backoffMs=100, reasonType=java.lang.IllegalStateException, reason=...
processing failed -> DEAD LETTER: eventId=e2, ..., offset=1, attempts=3/3, reasonType=..., reason=...
duplicate delivery skipped (no side effect): eventId=..., offset=..., terminalState=PROCESSED
```

### 5.2 HTTP 端到端（可选）

```bash
mvn spring-boot:run                     # 默认 8080；如被占用：-Dspring-boot.run.arguments=--server.port=28080

# 入队正常事件
curl -s -X POST localhost:8080/api/outbox/events -H 'Content-Type: application/json' \
  -d '{"aggregateId":"order-1","eventId":"e1","type":"Created","payload":"hello"}'

# 入队永久失败事件（payload 以 fail: 开头会被演示处理器拒绝，触发 3 次尝试后进死信）
curl -s -X POST localhost:8080/api/outbox/events -H 'Content-Type: application/json' \
  -d '{"aggregateId":"order-1","eventId":"e2","type":"Charged","payload":"fail:card declined"}'

curl -s localhost:8080/api/outbox/offsets                 # 各聚合消费位点
curl -s localhost:8080/api/outbox/processed               # 已生效副作用（去重后）
curl -s localhost:8080/api/outbox/dead-letters            # 死信列表（可加 ?state=OPEN）
curl -s localhost:8080/api/outbox/dead-letters/e2         # 死信详情（失败类型/原因）
```

错误响应示例：重复 `eventId` 返回 `409 ILLEGAL_STATE`；不存在的死信返回
`400 INVALID_ARGUMENT`；越界/失效位点（经编程式 `seek` 触发）返回
`422 INVALID_OFFSET` 并带可区分的 `reason`。

---

## 6. 边界与取舍说明

- 状态均为**进程内内存**，JVM 重启后代理/位点/死信/幂等记录会重置；
  测试通过 `restart()` 与“新流水线复用同一套组件”来模拟消费者重启/重平衡，
  验证“位点与幂等终态保留时不重复处理”。
- `RecordingEventHandler` 仅用于本地演示与测试；生产场景替换为自定义 `EventHandler` Bean
  （标注 `@Primary`）即可，幂等/重试/死信/顺序仍由外层流水线保证。
