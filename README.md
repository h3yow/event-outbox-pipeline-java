# Event Outbox Pipeline（进程内本地事件投递与异步消费）

基于 Spring Boot 3 / Java 21 的本地事件投递与异步消费实现。**不依赖外部消息代理**：
用进程内数据结构模拟分区日志（Broker）、消费位点（Offset）、重试计数、死信区（DLQ）
与日志保留清理，使事件在**重复投递、处理失败、并发消费、上万聚合共用少量通道、
长时间不重启运行**下的最终结果可预期。全部能力均可在本地通过单元测试或一个 `main`
方法演示验证。

## 1. 架构与组件

| 组件 | 位置 | 职责 |
| --- | --- | --- |
| `InMemoryEventBroker` | `broker` | 分段追加日志；`aggregateKey` 稳定哈希到固定分区；支持前缀截断（位点是绝对位点，截断后 `logStartOffset` 推进），`eventIndex` 截断后仍保留 |
| `ConsumerGroupState` | `consumer` | 每分区连续终态前缀水位线、乱序终态位点集合、每事件重试次数、已成功 `eventId` 集合（幂等依据，不随截断删除） |
| `EventConsumer` | `consumer` | 1 个调度线程 + N 个工作线程；**调度单位是（分区, 聚合）**，每聚合一条 FIFO 队列、最多 1 个在途任务；另含 1 个可选的后台保留清理线程 |
| `RetryPolicy` | `retry` | `maxAttempts`（含首次）、指数退避（`initialBackoff` → 上限 `maxBackoff`） |
| `InMemoryDeadLetterStore` | `dlq` | 可按 `eventId` / `(partition, offset)` / 分区最早位点检索的死信区，显式移除前不丢失 |
| `EventPipeline` | `pipeline` | 门面：入队、启停、位点回放、死信查询与重放、手动清理 `compactLog()` |
| `EventPipelineAutoConfiguration` | `config` | Spring Boot 自动装配，`event.outbox.*` 配置绑定 |

## 2. 聚合级隔离保证（新增）

背景：聚合数量可达上万，但并行通道（工作线程）只有少量，大量互不相干的聚合
必然挤在同一条物理分区上。调度因此不再以分区为在途单位：

- **同聚合严格保序、串行、不并发**：每个聚合一条 FIFO 待办队列，同一时刻最多一个
  工作线程处理该聚合；同聚合事件按入队顺序依次生效，绝不乱序、不重复。
- **慢事件不拖累其它聚合**：业务处理器在分区锁**之外**执行。一个聚合的慢事件
  （处理中阻塞或退避睡眠）只占用该聚合自己的在途槽；同分区其它聚合的事件照常被
  其它线程调度推进。
- **积压不饿死邻居**：工作线程每处理完一个聚合的一张待办，就把该聚合的队列
  **让渡回全局就绪队列尾部**，与其它聚合公平轮转。某个聚合积压再多，
  其它聚合也能持续得到线程（见 `AggregateIsolationTests.backloggedAggregateDoesNotStarveOthersOnSamePartition`）。
- **乱序完成、有序水位线**：不同聚合可以乱序终态；单事件终态先记入按序集合，
  分区提交水位线只沿“连续终态前缀”推进——水位线之前的事件一定全部结束，
  与完成顺序无关。
- 回放使用**分区代际号（generation）**：回放瞬间递增代际，在途旧任务即使正处于
  处理器调用中，其结果也会按规则作废或仅补幂等标记，绝不产生第二次副作用。

## 3. 投递语义

- **至少一次投递（at-least-once）+ 管道层幂等 = 业务副作用恰好一次（effectively-once）**。
- 幂等键为 `eventId`：
  - 入队幂等：相同 `eventId` 重复 `publish` 只映射到同一条物理消息（**日志截断后
    该索引仍保留**）；
  - 处理幂等：成功处理后 `eventId` 进入已处理集合，位点回退重投也不会再次进入
    业务处理器（`decision=SKIP_DUPLICATE`）；该集合不随日志截断删除。
- 位点只能随连续终态前缀单调推进；回退必须走显式回放 `seek(partition, offset)`。
- 位点语义：`committedOffset` = 下一个待处理位点，初始 `0`。

## 4. 失败重试与死信规则

1. 处理器抛异常即一次失败；第 N 次失败后：
   - `N < maxAttempts`：记录 `attempt`、指数退避后重试（日志 `decision=RETRY`，
     含事件标识、聚合、分区、位点、尝试次数、退避时长与异常原因）；退避睡眠
     **不持任何锁、不占其它聚合的通道**；
   - `N == maxAttempts`：在**同一分区锁内**先写死信记录、再标记终态
     （日志 `decision=DEAD_LETTER`），二者不会“成功与死信同时发生”。
2. 死信记录包含：事件原文、分区、位点、总尝试次数、
   `RETRY_EXHAUSTED attempts=.. maxAttempts=.. cause=异常类型: 异常消息` 的可区分原因、时间戳。
3. 死信不阻断同分区/同聚合后续事件（终态照常推进），事件**永不静默丢弃**。
4. 重放死信：`pipeline.replay(eventId)` —— 先移除死信记录，再回放到原位点并重置
   重试计数；成功则副作用生效一次，仍失败则再次进入死信。

## 5. 安全清理与保留策略（新增）

长时间不重启运行时，已安全消费完的事件由后台（或手动）清理，避免内存无限上涨。

- 触发方式：
  - 后台：`event.outbox.retention-interval=30s`（`0`/不配置 = 不自动清理）；
  - 手动：`pipeline.compactLog()` 立即执行一轮，返回回收事件数。
- 每分区安全截断点：`safePoint = min(连续终态水位线, 该分区最早死信位点)`。
  因此只回收“已经彻底结束”的连续前缀：
  - ✅ 已成功处理完成的事件；
  - ✅ 最早死信位点**之前**已结束的连续前缀；
  - 🚫 还没轮到处理的、正在重试（形成空洞）的事件 —— 水位线停在空洞处；
  - 🚫 死信区中的事件，以及它**之后**的所有事件（死信是清理屏障，
    重放成功后屏障才解除）。
- 清理与消费/死信转移在同一分区锁内串行，但**业务处理本身不持分区锁**，
  清理线程不需要等待慢业务调用，不阻挡新入队与消费。
- 物理日志按固定大小分段（256/段）回收，每分区至多残留一个段的引用；
  截断后位点仍是绝对位点。
- **保留的索引/状态**：`eventIndex`（重复入队折叠、按 eventId 定位）与
  已成功 `eventId` 幂等集合不随截断删除——这是以小而有界的索引内存换取
  “长时间运行仍不丢幂等结论”；被删的只是事件负载本身。

## 6. 清理之后的位点生命周期（可区分的拒绝原因）

`seek` / `seekToEvent` / `replay` 校验顺序固定，四类拒绝一眼可分，日志均带
`decision=SEEK_REJECTED ... reason=..`：

| 情况 | 异常 | 原因码/信息 |
| --- | --- | --- |
| 位点为负 | `OffsetOutOfRangeException` | `Reason.NEGATIVE_OFFSET` |
| 位点落在已截断区间（`< logStart`） | `OffsetOutOfRangeException` | `Reason.OFFSET_TRUNCATED`（消息含 logStart） |
| 位点 ≥ 日志末尾 | `OffsetOutOfRangeException` | `Reason.OFFSET_TOO_HIGH`（消息含 logEnd） |
| 位点指向死信事件 | `OffsetInvalidException` | 消息含 eventId、offset 与原始死信原因 |
| eventId / 分区不存在 | `IllegalArgumentException` | — |

清理后“当前状态该读到的还能读到”：

- 已成功事件的幂等结论、死信记录、屏障后未截断事件的物理读取都不受影响；
- 水位线、新事件的入队/消费照常；
- 截断位点的回放被**明确拒绝**而非静默跳过；
- 重复投递（同 eventId 再 publish）仍折叠为同一位置；死信重放或位点回放后，
  业务副作用依旧只生效一次。

## 7. 配置项（application.properties）

```properties
event.outbox.partition-count=4     # 分区数
event.outbox.worker-count=4        # 工作线程数（并行通道，远少于聚合数）
event.outbox.max-attempts=3        # 最大处理次数（含首次）
event.outbox.initial-backoff=50ms
event.outbox.max-backoff=2s
event.outbox.auto-start=false      # true 时容器启动即消费
#event.outbox.retention-interval=30s  # 后台安全清理周期；0/不配置=只手动 compactLog()
```

## 8. 编程示例

```java
EventPipeline pipeline = new EventPipeline(
        new InMemoryEventBroker(4),
        new ConsumerGroupState(),
        new InMemoryDeadLetterStore(),
        event -> System.out.println("handle " + event.eventId()),
        new RetryPolicy(3, Duration.ofMillis(50), Duration.ofSeconds(2)),
        4,     // worker 数
        5L,    // 调度轮询间隔 ms
        30_000L); // 后台清理周期 ms（0 = 不自动清理）

pipeline.start();
EventEnvelope e = pipeline.enqueue("order-123", "OrderPaid", payload);
pipeline.awaitProcessed(10_000);

int removed = pipeline.compactLog();         // 手动安全清理
pipeline.logStartOffset("p2");               // 截断后的起始位点
pipeline.seek("p2", 0);                      // 截断/越界/死信位点均被明确拒绝
pipeline.deadLetters();                      // 检索死信
pipeline.replay(e.eventId());                // 重放死信
pipeline.stop();
```

## 9. 本地验证

```bash
# 全部测试（26 个，无需任何外部中间件）
mvn test

# 按场景运行
mvn test -Dtest=OrderingTests                      # 同聚合保序、不丢不重
mvn test -Dtest=AggregateIsolationTests            # 同分区聚合隔离：慢事件不挡邻居/积压不饿死/多聚合保序
mvn test -Dtest=LogRetentionTests                  # 清理边界：死信屏障/重试空洞/并发清理/幂等保留
mvn test -Dtest=OffsetLifecycleAfterTruncationTests # 截断后四类位点拒绝、状态可读、重放一次
mvn test -Dtest=RetryAndDeadLetterTests            # 瞬时失败重试 / 超限死信与原因
mvn test -Dtest=IdempotencyAndOffsetTests          # 重复投递幂等 / 位点回放 / 越界拒绝
mvn test -Dtest=ConcurrentConsumptionTests         # 并发一致性 / 慢事件不阻塞无关分区
mvn test -Dtest=FailureUnderConcurrencyTests       # 成功+重试+死信混合并发，最终两态互斥
mvn test -Dtest=PipelineSpringWiringTests          # Spring Boot 装配与属性绑定

# 可执行演示（三段：重试/死信/回放 → 聚合隔离 → 清理与位点生命周期）
mvn -q compile exec:java -Dexec.mainClass=com.github.h3yow.eventoutboxpipelinejava.demo.ConsoleDemo
```

关键决策日志均含 `eventId`、`aggregateKey`、`partition`、`offset` 与判定依据：

```
decision=SUCCEED          eventId=.. aggregateKey=.. partition=p0 offset=0 attempt=1 basis='first-success'
decision=RETRY            eventId=.. aggregateKey=.. partition=p0 offset=0 attempt=2 nextBackoffMs=10 cause='..'
decision=DEAD_LETTER      eventId=.. aggregateKey=.. partition=p0 offset=0 attempts=3 basis='retries-exhausted' reason='..'
decision=SKIP_DUPLICATE   eventId=.. aggregateKey=.. partition=p0 offset=0 basis='processed-event-id'
decision=SKIP_STALE       eventId=.. aggregateKey=.. partition=p0 offset=0 basis='generation-changed'
decision=REWIND           partition=p0 offset=1 drainedQueuedTickets=.. basis='explicit-replay'
decision=SEEK_REJECTED    partition=p0 offset=0 logStart=1 reason='offset-truncated'
decision=RETENTION_TRIM   partition=p0 previousStart=0 newStart=3 watermark=3 earliestDeadLetter=none removedEvents=3 basis='contiguous-terminal-prefix'
decision=RETENTION_KEEP   partition=p0 logStart=1 watermark=3 earliestDeadLetter=1 basis='nothing-safely-trimmable'
```

## 10. 测试覆盖矩阵

| 需求 | 测试 |
| --- | --- |
| 同聚合稳定顺序、位点齐平、不丢失 | `OrderingTests` |
| **同分区慢事件不挡其它聚合 / 积压不饿死邻居 / 上万聚合少通道下保序不重** | `AggregateIsolationTests` |
| **清理只动连续终态前缀、死信屏障、重试/未消费保留、并发清理不丢不重、幂等索引保留** | `LogRetentionTests` |
| **截断/负数/越界/死信四类位点可区分拒绝、状态可读、重放副作用一次** | `OffsetLifecycleAfterTruncationTests` |
| 重试成功 / 超限死信 / 原因可区分 / 位点越过死信继续 | `RetryAndDeadLetterTests` |
| 重复入队折叠、回放不重复生效、越界与死信位点可区分拒绝 | `IdempotencyAndOffsetTests` |
| 并发不重不丢、慢事件不阻塞无关分区 | `ConcurrentConsumptionTests` |
| 成功/重试/死信混合并发下两态互斥、计数与位点一致 | `FailureUnderConcurrencyTests` |
| Spring 自动装配、自定义处理器、属性绑定、自动启动 | `PipelineSpringWiringTests` |
