package com.github.h3yow.eventoutboxpipelinejava.outbox.core;

import com.github.h3yow.eventoutboxpipelinejava.outbox.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.outbox.concurrent.AffiliatedExecutor;
import com.github.h3yow.eventoutboxpipelinejava.outbox.dlq.DeadLetterEntry;
import com.github.h3yow.eventoutboxpipelinejava.outbox.dlq.DeadLetterQueue;
import com.github.h3yow.eventoutboxpipelinejava.outbox.handler.EventHandler;
import com.github.h3yow.eventoutboxpipelinejava.outbox.idempotency.IdempotencyStore;
import com.github.h3yow.eventoutboxpipelinejava.outbox.offset.OffsetStore;
import com.github.h3yow.eventoutboxpipelinejava.outbox.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地事件投递与异步消费流水线。
 *
 * <p>以进程内状态模拟消息代理（{@link InMemoryEventBroker}）、消费位点
 * （{@link OffsetStore}）与死信区（{@link DeadLetterQueue}），提供如下投递语义：
 * <ul>
 *   <li><b>同聚合严格保序</b>：每个聚合对应 {@link AffiliatedExecutor} 中一条串行 lane，
 *       事件按位点依次处理；不同聚合并行，单事件慢/等待退避不阻塞无关聚合。</li>
 *   <li><b>至少一次投递 + 消费端幂等</b>：重复投递、重启后重投、位点回放只可能读到事件，
 *       业务副作用经 {@link IdempotencyStore} 保证只生效一次。</li>
 *   <li><b>失败重试</b>：按 {@link RetryPolicy} 退避重试；同聚合并发安全的计数一致。</li>
 *   <li><b>死信不丢</b>：重试耗尽的事件原子地转入死信区并保留失败原因，
 *       成功与死信不可能同时发生；任何事件都不会被静默丢弃。</li>
 * </ul>
 */
public final class OutboxEventPipeline implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OutboxEventPipeline.class);

    /** drain 循环结果：队列已空，或头部事件正处于退避等待。 */
    private enum DrainOutcome { IDLE, WAITING_RETRY }

    private final InMemoryEventBroker broker;
    private final OffsetStore offsets;
    private final DeadLetterQueue deadLetters;
    private final IdempotencyStore idempotency;
    private final EventHandler handler;
    private final RetryPolicy retryPolicy;
    private final AffiliatedExecutor executor;
    private final String consumerGroup;
    private final long pollIntervalMillis;
    private final int workerThreads;

    /** 已有 drain 任务（在 lane 上排队/运行）的聚合集合，用于合并唤醒。 */
    private final ConcurrentHashMap.KeySetView<String, Boolean> drainScheduled =
            ConcurrentHashMap.newKeySet();
    /** 聚合 -> 头部事件下一次重试的到期时间戳（epoch millis）。 */
    private final ConcurrentHashMap<String, Long> retryDeadline = new ConcurrentHashMap<>();
    /** 已安排退避定时器的聚合 -> 定时器句柄。 */
    private final ConcurrentHashMap<String, ScheduledFuture<?>> retryTimers = new ConcurrentHashMap<>();
    /** eventId -> 已实际调用业务处理器的次数（含失败调用）。 */
    private final ConcurrentHashMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    private volatile ScheduledExecutorService scheduler;
    private volatile boolean started;
    /**
     * 保护“drain 在途标记摘/挂 + 唤醒决定”的临界区，消除入队唤醒与 drain 收尾
     * 交错时的唤醒丢失窗口（事件本身状态由各自并发容器保证一致）。
     */
    private final Object pipelineLock = new Object();

    public OutboxEventPipeline(InMemoryEventBroker broker,
                               OffsetStore offsets,
                               DeadLetterQueue deadLetters,
                               IdempotencyStore idempotency,
                               EventHandler handler,
                               RetryPolicy retryPolicy,
                               int workerThreads,
                               String consumerGroup) {
        this(broker, offsets, deadLetters, idempotency, handler, retryPolicy,
                workerThreads, consumerGroup, 200L);
    }

    public OutboxEventPipeline(InMemoryEventBroker broker,
                               OffsetStore offsets,
                               DeadLetterQueue deadLetters,
                               IdempotencyStore idempotency,
                               EventHandler handler,
                               RetryPolicy retryPolicy,
                               int workerThreads,
                               String consumerGroup,
                               long pollIntervalMillis) {
        if (consumerGroup == null || consumerGroup.isBlank()) {
            throw new IllegalArgumentException("consumerGroup must not be blank");
        }
        this.broker = broker;
        this.offsets = offsets;
        this.deadLetters = deadLetters;
        this.idempotency = idempotency;
        this.handler = handler;
        this.retryPolicy = retryPolicy;
        this.executor = new AffiliatedExecutor(workerThreads);
        this.consumerGroup = consumerGroup;
        this.pollIntervalMillis = pollIntervalMillis;
        this.workerThreads = workerThreads;
    }

    /** 启动调度：兜底扫描器 + 对所有已有聚合立即触发一次投递。可重复调用但仅生效一次。 */
    public synchronized void start() {
        if (started) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "outbox-scheduler");
            t.setDaemon(true);
            return t;
        });
        started = true;
        scheduler.scheduleAtFixedRate(this::pollAll,
                pollIntervalMillis, pollIntervalMillis, TimeUnit.MILLISECONDS);
        log.info("pipeline started: consumerGroup={}, maxAttempts={}, workerThreads={}",
                consumerGroup, retryPolicy.maxAttempts(), workerThreads);
        broker.aggregateIds().forEach(this::wake);
    }

    /**
     * 模拟消费者重启：停止调度、清空瞬时退避/唤醒状态，然后依据已提交位点重新投递。
     * 幂等终态与位点均保留，因此已处理事件不会重复产生副作用。
     */
    public synchronized void restart() {
        if (!started) {
            start();
            return;
        }
        retryTimers.forEach((agg, f) -> f.cancel(false));
        retryTimers.clear();
        retryDeadline.clear();
        drainScheduled.clear();
        attempts.clear();
        log.info("pipeline restarted (consumer rebalance simulation): consumerGroup={}", consumerGroup);
        broker.aggregateIds().forEach(this::wake);
    }

    private void pollAll() {
        try {
            for (String aggregateId : broker.aggregateIds()) {
                wake(aggregateId);
            }
        } catch (Throwable t) {
            log.warn("scheduled poll failed", t);
        }
    }

    /** 事件入队并立即唤醒该聚合的投递。eventId 重复将被明确拒绝（不静默覆盖）。 */
    public OutboxEvent enqueue(String aggregateId, String eventId, String type, String payload) {
        OutboxEvent event = broker.append(aggregateId, eventId, type, payload);
        log.info("event enqueued: eventId={}, aggregate={}, offset={}, type={}",
                event.eventId(), event.aggregateId(), event.sequence(), event.type());
        wake(aggregateId);
        return event;
    }

    /**
     * 唤醒某聚合的 drain：若正处于退避等待则忽略（定时器到期会唤醒）；
     * 若已有 drain 在 lane 上排队/运行则合并；否则向归属执行器提交一个 drain 任务。
     */
    private void wake(String aggregateId) {
        if (!started) {
            return;
        }
        boolean shouldSubmit = false;
        synchronized (pipelineLock) {
            long now = System.currentTimeMillis();
            Long deadline = retryDeadline.get(aggregateId);
            if (deadline != null && deadline > now) {
                ensureRetryTimer(aggregateId, deadline - now);
                return;
            }
            if (drainScheduled.add(aggregateId)) {
                shouldSubmit = true;
            }
        }
        if (shouldSubmit) {
            executor.submit(aggregateId, () -> runDrain(aggregateId));
        }
    }

    /** 安排一次性退避定时器（同聚合去重）。 */
    private void ensureRetryTimer(String aggregateId, long delayMillis) {
        if (retryTimers.containsKey(aggregateId)) {
            return;
        }
        ScheduledFuture<?> future = scheduler.schedule(() -> {
            retryTimers.remove(aggregateId);
            Long d = retryDeadline.get(aggregateId);
            // 仅当定时器仍对应当前这次等待时才清除到期时间并唤醒，
            // 避免清掉后续失败重新设置的更晚 deadline。
            if (d != null && d <= System.currentTimeMillis()) {
                retryDeadline.remove(aggregateId);
                log.debug("retry timer fired: aggregate={}", aggregateId);
                wake(aggregateId);
            }
        }, Math.max(1L, delayMillis), TimeUnit.MILLISECONDS);
        if (retryTimers.putIfAbsent(aggregateId, future) != null) {
            future.cancel(false);
        }
    }

    /** 单个聚合的 drain 任务：在该聚合的串行 lane 上依次推进可处理事件。 */
    private void runDrain(String aggregateId) {
        DrainOutcome outcome = DrainOutcome.IDLE;
        try {
            outcome = drainLoop(aggregateId);
        } catch (Throwable t) {
            log.error("drain loop failed unexpectedly: aggregate={}", aggregateId, t);
        } finally {
            // 在同一临界区内完成“摘除在途标记 + 判定下一步”，消除“摘除标记后、
            // 重新调度前新事件入队导致唤醒被合并”的窗口；同聚合 drain 串行，
            // 此期间不会有该聚合的另一个 drain 在运行。
            synchronized (pipelineLock) {
                long now = System.currentTimeMillis();
                Long deadline = retryDeadline.get(aggregateId);
                drainScheduled.remove(aggregateId);
                if (deadline != null && deadline > now) {
                    // 头部事件仍在退避：安排到期唤醒，期间任何入队/兜底扫描的唤醒都会被抑制。
                    ensureRetryTimer(aggregateId, deadline - now);
                } else if (deadline != null) {
                    // 退避恰好在收尾时到期：立即重新投递，不必等待兜底扫描。
                    retryDeadline.remove(aggregateId);
                    wake(aggregateId);
                } else if (hasReadyWork(aggregateId)) {
                    // 处理过程中位点之后又有新事件入队且唤醒被合并：立即补一次投递。
                    wake(aggregateId);
                }
            }
        }
    }

    /**
     * 连续处理同一聚合的就绪事件；遇到头部事件处于退避等待时返回 WAITING_RETRY。
     */
    private DrainOutcome drainLoop(String aggregateId) {
        while (true) {
            long earliest = broker.earliestOffset(aggregateId);
            long offset = offsets.committed(consumerGroup, aggregateId, earliest);
            Optional<OutboxEvent> fetched = broker.fetch(aggregateId, offset);
            if (fetched.isEmpty()) {
                return DrainOutcome.IDLE;
            }
            OutboxEvent event = fetched.get();
            long now = System.currentTimeMillis();
            Long deadline = retryDeadline.get(aggregateId);
            if (deadline != null && deadline > now) {
                return DrainOutcome.WAITING_RETRY;
            }
            processOne(aggregateId, event, offset);
            // processOne 成功/死信会继续循环；若安排了重试（retryDeadline 被置位）则返回。
            Long d = retryDeadline.get(aggregateId);
            if (d != null && d > System.currentTimeMillis()) {
                return DrainOutcome.WAITING_RETRY;
            }
        }
    }

    private boolean hasReadyWork(String aggregateId) {
        long earliest = broker.earliestOffset(aggregateId);
        long offset = offsets.committed(consumerGroup, aggregateId, earliest);
        Long deadline = retryDeadline.get(aggregateId);
        if (deadline != null && deadline > System.currentTimeMillis()) {
            return false;
        }
        return broker.fetch(aggregateId, offset).isPresent();
    }

    /** 处理单个事件：幂等判定 -> 业务调用 -> 成功推进 / 失败重试 / 超限死信。 */
    private void processOne(String aggregateId, OutboxEvent event, long offset) {
        String eventId = event.eventId();
        IdempotencyStore.AcquireResult acquired = idempotency.acquire(eventId, offset);
        if (acquired instanceof IdempotencyStore.AcquireResult.AlreadyTerminal terminal) {
            IdempotencyStore.Record rec = terminal.record();
            log.info("duplicate delivery skipped (no side effect): eventId={}, aggregate={}, offset={}, terminalState={}",
                    eventId, aggregateId, offset, rec.state());
            commitNext(aggregateId, offset);
            return;
        }

        int attempt = attempts.computeIfAbsent(eventId, k -> new AtomicInteger()).incrementAndGet();
        try {
            log.info("handling event: eventId={}, aggregate={}, offset={}, attempt={}/{}",
                    eventId, aggregateId, offset, attempt, retryPolicy.maxAttempts());
            handler.handle(event);
        } catch (Throwable failure) {
            String reason = failure.getMessage() != null
                    ? failure.getMessage()
                    : failure.getClass().getSimpleName();
            if (retryPolicy.shouldRetry(attempt)) {
                long delayMillis = retryPolicy.backoffAfter(attempt).toMillis();
                retryDeadline.put(aggregateId, System.currentTimeMillis() + delayMillis);
                idempotency.release(eventId);
                log.warn("processing failed -> RETRY: eventId={}, aggregate={}, offset={}, attempt={}/{}, backoffMs={}, reasonType={}, reason={}",
                        eventId, aggregateId, offset, attempt, retryPolicy.maxAttempts(),
                        delayMillis, failure.getClass().getName(), reason);
                return;
            }
            DeadLetterEntry entry = deadLetters.send(event, consumerGroup, attempt, failure);
            idempotency.markDead(eventId, offset);
            attempts.remove(eventId);
            retryDeadline.remove(aggregateId);
            log.error("processing failed -> DEAD LETTER: eventId={}, aggregate={}, offset={}, attempts={}/{}, reasonType={}, reason={}",
                    eventId, aggregateId, offset, attempt, retryPolicy.maxAttempts(),
                    entry.lastFailureType(), entry.lastFailureReason());
            commitNext(aggregateId, offset);
            return;
        }

        idempotency.markProcessed(eventId, offset);
        attempts.remove(eventId);
        retryDeadline.remove(aggregateId);
        log.info("processing succeeded: eventId={}, aggregate={}, offset={}, attempt={}/{}",
                eventId, aggregateId, offset, attempt, retryPolicy.maxAttempts());
        commitNext(aggregateId, offset);
    }

    private void commitNext(String aggregateId, long currentOffset) {
        long next = currentOffset + 1;
        offsets.commit(consumerGroup, aggregateId, next,
                broker.earliestOffset(aggregateId), broker.latestOffset(aggregateId));
    }

    /**
     * 重放死信：要求事件仍存在于代理日志中，位点将重置到该事件下标后重新投递。
     *
     * @throws IllegalArgumentException 事件不在死信区或已被处理过
     */
    public void replayDeadLetter(String eventId) {
        DeadLetterEntry entry = deadLetters.findByEventId(eventId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "dead letter entry not found: " + eventId));
        if (entry.state() != DeadLetterState.OPEN) {
            throw new IllegalArgumentException(
                    "dead letter " + eventId + " is not OPEN: " + entry.state());
        }
        String aggregateId = entry.event().aggregateId();
        long offset = entry.event().sequence();
        // 位点校验先发生：若日志已截断/越界，抛出 InvalidOffsetException 且不改变任何状态。
        offsets.seek(consumerGroup, aggregateId, offset,
                broker.earliestOffset(aggregateId), broker.latestOffset(aggregateId));
        deadLetters.markState(eventId, DeadLetterState.REPLAYED);
        idempotency.clear(eventId);
        attempts.remove(eventId);
        retryDeadline.remove(aggregateId);
        log.info("dead letter replayed: eventId={}, aggregate={}, seekOffset={}",
                eventId, aggregateId, offset);
        wake(aggregateId);
    }

    /**
     * 等待流水线安静：没有 drain 在运行、没有未到期的退避等待（重试均已得出最终结果）。
     *
     * <p>必须“连续稳定”若干个采样周期都保持安静才算完成，以避开两个 drain 批次之间
     * （旧 drain 刚摘除标记、新 drain 尚未提交）的误判窗口。
     */
    public void awaitQuiescence(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        long quietSince = -1L;
        final long requiredQuietMillis = pollIntervalMillis + 50L;
        while (System.currentTimeMillis() < deadline) {
            boolean timersPending = retryTimers.values().stream()
                    .anyMatch(f -> !f.isDone());
            boolean deadlinePending = retryDeadline.entrySet().stream()
                    .anyMatch(e -> e.getValue() > System.currentTimeMillis());
            boolean quiet = !timersPending && !deadlinePending && drainScheduled.isEmpty();
            if (quiet) {
                long now = System.currentTimeMillis();
                if (quietSince < 0) {
                    quietSince = now;
                } else if (now - quietSince >= requiredQuietMillis) {
                    return;
                }
            } else {
                quietSince = -1L;
            }
            try {
                Thread.sleep(5L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new IllegalStateException("pipeline did not reach quiescence within " + timeoutMillis + "ms");
    }

    @Override
    public void close() {
        synchronized (this) {
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
        }
        executor.shutdown();
        try {
            executor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public InMemoryEventBroker broker() {
        return broker;
    }

    public OffsetStore offsets() {
        return offsets;
    }

    public DeadLetterQueue deadLetters() {
        return deadLetters;
    }

    public IdempotencyStore idempotency() {
        return idempotency;
    }

    public AffiliatedExecutor executor() {
        return executor;
    }

    public String consumerGroup() {
        return consumerGroup;
    }
}
