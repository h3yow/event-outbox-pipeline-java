# Event Outbox Pipeline（进程内本地事件投递与异步消费）

基于 Spring Boot 3 / Java 21 的本地事件投递与异步消费实现。**不依赖外部消息代理**：
用进程内数据结构模拟分区日志（Broker）、消费位点（Offset）、重试计数与死信区（DLQ），
使事件在**重复投递、处理失败、并发消费、日志清理**下的最终结果可预期。全部能力均可在
本地通过单元测试或一个 `main` 方法演示验证。

## 1. 架构与组件

| 组件 | 位置 | 职责 |
| --- | --- | --- |
| `InMemoryEventBroker` | `broker` | 分区追加日志；`aggregateKey` 稳定哈希到固定分区，分区内按位点（offset）严格有序；默认只追加，后台清理按安全水位截断连续前缀（`eventId → 位置`索引保留） |
| `ConsumerGroupState` | `consumer` | 各分区已提交位点、每事件重试次数、已成功 `eventId` 集合（幂等依据）、已终态但未齐平的位点集合、分区代际号 |
| `EventConsumer` | `consumer` | 1 个调度线程 + N 个工作线程；**调度单元为“分区内聚合”**；聚合槽位保证同聚合串行，分区锁只负责连续终态前缀折叠与回放 |
| `AggregateStateStore` | `state` | 每个聚合最后一个成功事件（当前状态投影）；独立于日志，日志被清理后依旧可读 |
| `EventLogCleaner` | `cleanup` | 后台守护线程，按安全水位小批量物理删除已彻底结束的日志前缀；也可手动触发一轮 |
| `RetryPolicy` | `retry` | `maxAttempts`（含首次的最大尝试次数）、指数退避（`initialBackoff` → 上限 `maxBackoff`） |
| `InMemoryDeadLetterStore` | `dlq` | 可按 `eventId` / `(partition, offset)` 检索的死信区，保留尝试次数与失败原因，并提供“分区最早死信位点”作为清理硬水位 |
| `EventPipeline` | `pipeline` | 门面：入队、启停、位点回放、死信查询与重放、聚合当前状态、手动清理 |
| `EventPipelineAutoConfiguration` | `config` | Spring Boot 自动装配，`event.outbox.*` 配置绑定 |

### 并发模型（聚合级隔离）

聚合数量可达上万，但工作通道（线程）只有固定 N 条，且大量互不相干的聚合会稳定哈希到
同一条物理分区。调度因此**细化到“分区内聚合”这一层**：

- 调度器为每个分区维护 `聚合 -> 有序待处理位点队列`；同一聚合任意时刻**最多一个在途任务**，
  事件严格按位点串行处理——不乱序、不并发重入、不重复；
- 不同聚合的任务可并行：某聚合的慢事件或退避睡眠**只占住该聚合自己的槽位**，
  同分区其他聚合照常推进（见 `AggregateIsolationTests`）；
- 全局许可数 = worker 数，在途任务总数恒不超过工作线程数，上万聚合积压也不会撑爆线程池；
- 调度按 **“最久未派发优先”（从未派发的新聚合排最前）** 选候选，单一重负载/积压聚合
  即使在只有 1 个工作线程时也**无法饿死**其他聚合；
- 位点仍按连续前缀齐平：先结束的高位点先记入终态集合，等更早日程的聚合结束后，
  在分区锁内把连续前缀折叠进 `committedOffset`。
- 显式回放（seek）会使分区**代际号**自增并重建调度队列；正处于退避中的旧任务醒来后
  凭代际判定自己已过期，不再触碰业务处理器或位点（`ABORT_STALE_GENERATION`）。

## 2. 投递语义

- **至少一次投递（at-least-once）+ 管道层幂等 = 业务副作用恰好一次（effectively-once）**。
- 幂等键为 `eventId`：
  - 入队幂等：相同 `eventId` 重复 `publish` 只映射到同一条物理消息；
  - 处理幂等：事件处理成功后 `eventId` 进入已处理集合，**即使位点被回退重投，
    也不会再次进入业务处理器**（`decision=SKIP_DUPLICATE`），副作用只生效一次。
  - 幂等标记只在对应物理事件被后台清理时才一并裁剪；仍存活区间内的已处理事件，
    其标记在清理后依然保留，重复投递/回放不会二次生效。
- 位点只能单调提交；回退必须走显式回放 `seek(partition, offset)`。
- 位点语义：`committedOffset` = 下一个待处理位点，初始 `0`。

## 3. 失败重试与死信规则

1. 处理器抛异常即一次失败；第 N 次失败后：
   - `N < maxAttempts`：记录 `attempt`、按指数退避等待后重试（日志 `decision=RETRY`，
     打印事件标识、分区、位点、聚合、当前尝试次数、退避时长与异常原因）；
   - `N == maxAttempts`：写死信记录、记入终态位点（日志 `decision=DEAD_LETTER`），
     分区位点在连续前缀可折叠时推进，不会出现“成功与死信同时发生”。
2. 死信记录包含：事件原文、分区、位点、总尝试次数、
   `RETRY_EXHAUSTED attempts=.. maxAttempts=.. cause=异常类型: 异常消息` 的可区分原因、时间戳。
3. 死信不阻断同分区**其他聚合**（它们并行推进），且事件**永不静默丢弃**：
   每个事件最终恰处于“成功处理”或“死信区”两态之一。
4. 重放死信：`pipeline.replay(eventId)` —— 先从死信区移除（解除位点锁定），
   再回放到原位点并重置重试计数；成功则副作用生效一次，仍失败则再次进入死信。

## 4. 后台日志清理（长跑内存治理）

清理在独立守护线程小批量执行（每分区每轮最多 `cleanup.batch-size` 条），
**不与入队/消费抢全局锁，不挡新的入队和消费**；也可调用 `pipeline.cleanupOnce()` 手动跑一轮。

每个分区的安全水位：

```
safeWatermark = min(committedOffset, earliestLiveDeadLetterOffset)
```

只物理删除位点严格小于安全水位的连续前缀：

| 事件状态 | 是否可被清理 |
| --- | --- |
| 已成功处理且已被连续位点越过 | 可删 |
| 已进入死信、后经正式重放成功并越过 | 可删（死信移除后水位放开） |
| 还没轮到处理的（位点 ≥ committed） | 不可删 |
| 正在退避重试中的（卡住连续前缀） | 不可删，且其后已完成事件也先保留 |
| **仍在死信区里的** | 不可删；它是硬水位，它和它之后的事件都保留 |

清理同步裁剪被删事件的幂等标记，但保留 `eventId → 位置`索引与 `AggregateStateStore`
聚合当前状态投影。判定日志：`decision=CLEANED ... basis='committed-and-past-live-dead-letter'`、
`decision=LOG_TRUNCATED`。

## 5. 位点生命周期（可区分的拒绝原因）

清理后日志区间会出现空洞（起始位点前移）。`seek` / `seekToEvent` / `replay`
按以下顺序校验，四种情况各自给出一眼可辨的拒绝原因：

| 情况 | 异常 | 原因码/信息 |
| --- | --- | --- |
| 位点为负 | `OffsetOutOfRangeException` | `Reason.NEGATIVE_OFFSET`，消息含 `must not be negative` |
| 位点落在已被清理的区间（`< logStart`） | `OffsetOutOfRangeException` | `Reason.OFFSET_TRUNCATED`，消息含 `cleaned up` 与当前 `logStart` |
| 位点 ≥ 日志末尾 | `OffsetOutOfRangeException` | `Reason.OFFSET_TOO_HIGH`，消息含 `beyond log end` 与 `logEnd` |
| 位点指向仍在死信区的事件 | `OffsetInvalidException` | 消息含 `dead-lettered event`、eventId 与原始死信原因 |
| eventId / 分区不存在 | `IllegalArgumentException` | — |

注意“数据被清理”不等于“数据缺失”：

- 物理消息读取（`broker.read`）对已删位点返回空，旧位点回放被明确判为 `OFFSET_TRUNCATED`；
- 但 `pipeline.currentAggregateState(aggregateKey)` 读取的是独立状态投影，
  **聚合当前状态在清理后照常可读**，只有从未见过的聚合才返回空。

## 6. 配置项（application.properties）

```properties
event.outbox.partition-count=4     # 分区数
event.outbox.worker-count=4        # 工作线程数（= 最大在途任务数）
event.outbox.max-attempts=3        # 最大处理次数（含首次）
event.outbox.initial-backoff=50ms
event.outbox.max-backoff=2s
event.outbox.auto-start=false      # true 时容器启动即消费；默认显式 pipeline.start()

# 后台日志清理（默认关闭；长时间不重启运行时置 true）
event.outbox.cleanup.enabled=false
event.outbox.cleanup.interval=2s   # 清理轮询间隔
event.outbox.cleanup.batch-size=500 # 每分区每轮最多物理删除条数
```

容器中声明自己的 `EventHandler` Bean 即可替换默认空处理器。

## 7. 编程示例

```java
EventLogCleaner cleaner = new EventLogCleaner(broker, state, dlq,
        Duration.ofSeconds(2).toMillis(), 500);
EventPipeline pipeline = new EventPipeline(
        broker, state, dlq, new AggregateStateStore(), cleaner,
        event -> System.out.println("handle " + event.eventId()),
        new RetryPolicy(3, Duration.ofMillis(50), Duration.ofSeconds(2)),
        4, 5L);

pipeline.start();
EventEnvelope e = pipeline.enqueue("order-123", "OrderPaid", payload);
pipeline.awaitProcessed(10_000);

pipeline.cleanupOnce();                          // 手动清理一轮
pipeline.currentAggregateState("order-123");     // 清理后仍读到最后成功事件
pipeline.seek("p0", 0);                          // 已被清理 -> OFFSET_TRUNCATED
pipeline.deadLetters();                          // 检索死信
pipeline.replay(e.eventId());                    // 重放死信
pipeline.stop();
```

## 8. 本地验证

```bash
# 全部测试（26 个，无需任何外部中间件）
mvn test

# 按场景运行
mvn test -Dtest=OrderingTests                    # 同聚合保序、不丢不重
mvn test -Dtest=AggregateIsolationTests          # 慢事件只卡本聚合 / 单线程公平不饿死 / 同聚合保序
mvn test -Dtest=RetryAndDeadLetterTests          # 瞬时失败重试 / 超限死信与原因
mvn test -Dtest=IdempotencyAndOffsetTests        # 重复投递幂等 / 位点回放 / 越界拒绝
mvn test -Dtest=LogCleanupTests                  # 清理边界：成功前缀可删、重试/待处理/死信保护
mvn test -Dtest=OffsetLifecycleTests             # 截断/负数/超末尾/死信四类拒绝 + 清理后状态与幂等
mvn test -Dtest=ConcurrentConsumptionTests       # 800 事件并发一致性
mvn test -Dtest=FailureUnderConcurrencyTests     # 成功+重试+死信混合并发，最终两态互斥
mvn test -Dtest=CleanupSpringWiringTests         # 清理器自动装配、后台运行不挡消费
mvn test -Dtest=PipelineSpringWiringTests        # Spring Boot 装配与属性绑定

# 可执行演示（单分区多聚合隔离→死信→清理水位→重放→截断位点拒绝，全程结构化日志）
mvn -q compile exec:java -Dexec.mainClass=com.github.h3yow.eventoutboxpipelinejava.demo.ConsoleDemo
```

测试日志统一包含 `eventId`、`partition`、`offset`、`aggregateKey`、`attempt(s)` 与判定依据，例如：

```
decision=RETRY            eventId=.. partition=p0 offset=2 aggregateKey=order-B attempt=2 nextBackoffMs=200 cause='IllegalStateException: ..'
decision=DEAD_LETTER      eventId=.. partition=p0 offset=2 aggregateKey=order-B attempts=3 basis='retries-exhausted' reason='RETRY_EXHAUSTED ..'
decision=SUCCEED          eventId=.. partition=p0 offset=3 aggregateKey=order-C attempt=1 basis='first-success'
decision=SKIP_DUPLICATE   eventId=.. partition=p0 offset=3 aggregateKey=order-C basis='processed-event-id'
decision=REWIND           partition=p0 offset=2 generation=1 basis='explicit-replay'
decision=ABORT_STALE_GENERATION partition=p0 offset=2 aggregateKey=order-B basis='rewind-occurred'
decision=CLEANED          partition=p0 removed=2 safeWatermark=2 committed=4 earliestDeadLetter=2 basis='committed-and-past-live-dead-letter'
decision=LOG_TRUNCATED    partition=p0 removed=2 newLogStart=2 logEnd=4 basis='safe-watermark'
```

## 9. 测试覆盖矩阵

| 需求 | 测试 |
| --- | --- |
| 同聚合稳定顺序、位点齐平、不丢失 | `OrderingTests` |
| 同分区慢事件不拖其它聚合、单 worker 公平不饿死、同聚合串行保序 | `AggregateIsolationTests` |
| 成功前缀可清理；重试中/待处理/存活死信受保护；重放后放开；并发清理一致性；清理后状态与幂等 | `LogCleanupTests` |
| 截断/负数/超末尾/死信四类位点拒绝可分辨；清理后聚合状态可读；存活区间重放副作用一次 | `OffsetLifecycleTests` |
| 重试成功 / 超限死信 / 原因可区分 / 位点越过死信继续 | `RetryAndDeadLetterTests` |
| 重复入队折叠、回放不重复生效、越界与死信位点可区分拒绝 | `IdempotencyAndOffsetTests` |
| 800 事件并发不重不丢 | `ConcurrentConsumptionTests` |
| 成功/重试/死信混合并发下两态互斥、计数与位点一致 | `FailureUnderConcurrencyTests` |
| 清理器 Spring 自动装配、后台运行不挡消费 | `CleanupSpringWiringTests` |
| Spring 自动装配、自定义处理器、属性绑定、自动启动 | `PipelineSpringWiringTests` |
