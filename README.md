# Event Outbox Pipeline（进程内本地事件投递与异步消费）

基于 Spring Boot 3 / Java 21 的本地事件投递与异步消费实现。**不依赖外部消息代理**：
用进程内数据结构模拟分区日志（Broker）、消费位点（Offset）、重试计数与死信区（DLQ），
使事件在**重复投递、处理失败、并发消费**下的最终结果可预期。全部能力均可在本地通过
单元测试或一个 `main` 方法演示验证。

## 1. 架构与组件

| 组件 | 位置 | 职责 |
| --- | --- | --- |
| `InMemoryEventBroker` | `broker` | 分区追加日志；`aggregateKey` 稳定哈希到固定分区，分区内按位点（offset）严格有序；日志只追加、永不删除 |
| `ConsumerGroupState` | `consumer` | 各分区已提交位点（下一个待处理位点）、每事件重试次数、已成功处理的 `eventId` 集合（幂等依据） |
| `EventConsumer` | `consumer` | 1 个调度线程 + N 个工作线程；每分区最多 1 个在途任务，分区锁保护“处理/死信/位点推进” |
| `RetryPolicy` | `retry` | `maxAttempts`（含首次的最大尝试次数）、指数退避（`initialBackoff` → 上限 `maxBackoff`） |
| `InMemoryDeadLetterStore` | `dlq` | 可按 `eventId` / `(partition, offset)` 检索的死信区，保留尝试次数与失败原因，显式移除前不丢失 |
| `EventPipeline` | `pipeline` | 门面：入队、启停、位点回放、死信查询与重放 |
| `EventPipelineAutoConfiguration` | `config` | Spring Boot 自动装配，`event.outbox.*` 配置绑定 |

### 并发模型

- 调度线程循环扫描所有分区，发现“已提交位点 < 日志末尾”且该分区无在途任务时，
  向工作线程池提交一个分区任务。
- **同一分区（即同一 `aggregateKey`）严格串行、保序**；不同分区由不同线程并行处理。
- 一个慢事件只会让**同分区**后续事件等待，**不阻塞其他分区**（见 `ConcurrentConsumptionTests`）。

## 2. 投递语义

- **至少一次投递（at-least-once）+ 管道层幂等 = 业务副作用恰好一次（effectively-once）**。
- 幂等键为 `eventId`：
  - 入队幂等：相同 `eventId` 重复 `publish` 只映射到同一条物理消息；
  - 处理幂等：事件处理成功后 `eventId` 进入已处理集合，**即使位点被回退重投，
    也不会再次进入业务处理器**（`decision=SKIP_DUPLICATE`），副作用只生效一次。
- 位点只能单调提交；回退必须走显式回放 `seek(partition, offset)`。
- 位点语义：`committedOffset` = 下一个待处理位点，初始 `0`。

## 3. 失败重试与死信规则

1. 处理器抛异常即一次失败；第 N 次失败后：
   - `N < maxAttempts`：记录 `attempt`、按指数退避等待后重试（日志 `decision=RETRY`，
     打印事件标识、分区、位点、当前尝试次数、退避时长与异常原因）；
   - `N == maxAttempts`：在**同一分区锁内**先写死信记录、再提交位点 `offset+1`
     （日志 `decision=DEAD_LETTER`），两者不会出现“成功与死信同时发生”。
2. 死信记录包含：事件原文、分区、位点、总尝试次数、
   `RETRY_EXHAUSTED attempts=.. maxAttempts=.. cause=异常类型: 异常消息` 的可区分原因、时间戳。
3. 死信不阻断同分区后续事件（位点照常推进），且事件**永不静默丢弃**：
   每个事件最终恰处于“成功处理”或“死信区”两态之一。
4. 重放死信：`pipeline.replay(eventId)` —— 先从死信区移除（解除位点锁定），
   再回放到原位点并重置重试计数；成功则副作用生效一次，仍失败则再次进入死信。

## 4. 位点校验（可区分的拒绝原因）

`seek` / `seekToEvent` / `replay` 会先校验位点：

| 情况 | 异常 | 原因码/信息 |
| --- | --- | --- |
| 位点为负 | `OffsetOutOfRangeException` | `Reason.NEGATIVE_OFFSET` |
| 位点低于日志起始 | `OffsetOutOfRangeException` | `Reason.OFFSET_TRUNCATED` |
| 位点 ≥ 日志末尾 | `OffsetOutOfRangeException` | `Reason.OFFSET_TOO_HIGH` |
| 位点指向已死信事件 | `OffsetInvalidException` | 消息含 eventId 与原始死信原因 |
| eventId / 分区不存在 | `IllegalArgumentException` | — |

## 5. 配置项（application.properties）

```properties
event.outbox.partition-count=4   # 分区数
event.outbox.worker-count=4      # 工作线程数
event.outbox.max-attempts=3      # 最大处理次数（含首次）
event.outbox.initial-backoff=50ms
event.outbox.max-backoff=2s
event.outbox.auto-start=false    # true 时容器启动即消费；默认显式 pipeline.start()
```

容器中声明自己的 `EventHandler` Bean 即可替换默认空处理器。

## 6. 编程示例

```java
EventPipeline pipeline = new EventPipeline(
        new InMemoryEventBroker(4),
        new ConsumerGroupState(),
        new InMemoryDeadLetterStore(),
        event -> System.out.println("handle " + event.eventId()),
        new RetryPolicy(3, Duration.ofMillis(50), Duration.ofSeconds(2)),
        4);

pipeline.start();
EventEnvelope e = pipeline.enqueue("order-123", "OrderPaid", payload);
pipeline.awaitProcessed(10_000);

pipeline.seek("p2", 0);                       // 位点回放（越界/死信会被明确拒绝）
pipeline.deadLetters();                       // 检索死信
pipeline.replay(e.eventId());                 // 重放死信
pipeline.stop();
```

## 7. 本地验证

```bash
# 全部测试（14 个，无需任何外部中间件）
mvn test

# 按场景运行
mvn test -Dtest=OrderingTests                    # 同聚合保序、不丢不重
mvn test -Dtest=RetryAndDeadLetterTests          # 瞬时失败重试 / 超限死信与原因
mvn test -Dtest=IdempotencyAndOffsetTests        # 重复投递幂等 / 位点回放 / 越界拒绝
mvn test -Dtest=ConcurrentConsumptionTests       # 800 事件并发一致性 / 慢事件不阻塞无关分区
mvn test -Dtest=FailureUnderConcurrencyTests     # 成功+重试+死信混合并发，最终两态互斥
mvn test -Dtest=PipelineSpringWiringTests        # Spring Boot 装配与属性绑定

# 可执行演示（入队→部分失败重试→死信→回放，全程打印结构化决策日志）
mvn -q compile exec:java -Dexec.mainClass=com.github.h3yow.eventoutboxpipelinejava.demo.ConsoleDemo
```

测试日志统一包含 `eventId`、`partition`、`offset`、`attempt(s)` 与判定依据，例如：

```
decision=RETRY       eventId=.. partition=p1 offset=0 attempt=2 nextBackoffMs=10 cause='IllegalStateException: injected-failure ..'
decision=DEAD_LETTER eventId=.. partition=p1 offset=0 attempts=3 basis='retries-exhausted' reason='RETRY_EXHAUSTED attempts=3 maxAttempts=3 cause=..'
decision=SUCCEED     eventId=.. partition=p0 offset=0 attempt=1 basis='first-success'
decision=SKIP_DUPLICATE eventId=.. partition=p0 offset=0 basis='processed-event-id'
```

## 8. 测试覆盖矩阵

| 需求 | 测试 |
| --- | --- |
| 同聚合稳定顺序、位点齐平、不丢失 | `OrderingTests` |
| 重试成功 / 超限死信 / 原因可区分 / 位点越过死信继续 | `RetryAndDeadLetterTests` |
| 重复入队折叠、回放不重复生效、越界与死信位点可区分拒绝 | `IdempotencyAndOffsetTests` |
| 800 事件并发不重不丢、慢事件不阻塞无关分区 | `ConcurrentConsumptionTests` |
| 成功/重试/死信混合并发下两态互斥、计数与位点一致 | `FailureUnderConcurrencyTests` |
| Spring 自动装配、自定义处理器、属性绑定、自动启动 | `PipelineSpringWiringTests` |
