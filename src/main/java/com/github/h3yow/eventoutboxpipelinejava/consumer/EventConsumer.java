package com.github.h3yow.eventoutboxpipelinejava.consumer;

import com.github.h3yow.eventoutboxpipelinejava.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.dlq.DeadLetterRecord;
import com.github.h3yow.eventoutboxpipelinejava.dlq.DeadLetterStore;
import com.github.h3yow.eventoutboxpipelinejava.dlq.InMemoryDeadLetterStore;
import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.handler.EventHandler;
import com.github.h3yow.eventoutboxpipelinejava.offset.OffsetInvalidException;
import com.github.h3yow.eventoutboxpipelinejava.offset.OffsetOutOfRangeException;
import com.github.h3yow.eventoutboxpipelinejava.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 异步消费者。
 *
 * <p>并发模型：单个调度线程扫描所有分区，每个分区最多有一个在途任务；
 * 工作线程池并发执行不同分区的任务。因此同一聚合（分区）严格串行、保序，
 * 而一个慢事件只会阻塞自身分区，不影响其他分区推进。
 *
 * <p>一致性：每个分区有独立锁，"业务处理 + 死信转移 + 位点推进" 在同一任务内完成；
 * 已成功处理过的 eventId 永不重复生效（幂等标记独立于位点持久存在）。
 */
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private final InMemoryEventBroker broker;
    private final ConsumerGroupState state;
    private final InMemoryDeadLetterStore deadLetters;
    private final EventHandler handler;
    private final RetryPolicy retryPolicy;
    private final int workerCount;
    private final long pollIntervalMillis;

    private final Map<String, AtomicBoolean> inflight = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> partitionLocks = new ConcurrentHashMap<>();

    private volatile ExecutorService workers;
    private volatile Thread dispatcher;
    private volatile boolean running;

    public EventConsumer(InMemoryEventBroker broker,
                         ConsumerGroupState state,
                         DeadLetterStore deadLetters,
                         EventHandler handler,
                         RetryPolicy retryPolicy,
                         int workerCount) {
        this(broker, state, deadLetters, handler, retryPolicy, workerCount, 5L);
    }

    public EventConsumer(InMemoryEventBroker broker,
                         ConsumerGroupState state,
                         DeadLetterStore deadLetters,
                         EventHandler handler,
                         RetryPolicy retryPolicy,
                         int workerCount,
                         long pollIntervalMillis) {
        if (!(deadLetters instanceof InMemoryDeadLetterStore)) {
            throw new IllegalArgumentException("deadLetters must be InMemoryDeadLetterStore");
        }
        this.broker = broker;
        this.state = state;
        this.deadLetters = (InMemoryDeadLetterStore) deadLetters;
        this.handler = handler;
        this.retryPolicy = retryPolicy;
        this.workerCount = Math.max(1, workerCount);
        this.pollIntervalMillis = Math.max(1, pollIntervalMillis);
        for (String partition : broker.partitions()) {
            inflight.put(partition, new AtomicBoolean(false));
            partitionLocks.put(partition, new ReentrantLock());
        }
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        ThreadFactory tf = new ThreadFactory() {
            private final AtomicInteger seq = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "event-worker-" + seq.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        };
        workers = Executors.newFixedThreadPool(workerCount, tf);
        dispatcher = new Thread(this::dispatchLoop, "event-dispatcher");
        dispatcher.setDaemon(true);
        dispatcher.start();
        log.info("EventConsumer started: partitions={}, workers={}, maxAttempts={}",
                broker.partitions().size(), workerCount, retryPolicy.maxAttempts());
    }

    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (dispatcher != null) {
            dispatcher.interrupt();
        }
        if (workers != null) {
            workers.shutdown();
            try {
                if (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
                    workers.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                workers.shutdownNow();
            }
        }
        log.info("EventConsumer stopped");
    }

    private void dispatchLoop() {
        while (running) {
            boolean scheduledAny = false;
            for (String partition : broker.partitions()) {
                if (!running) {
                    break;
                }
                if (inflight.get(partition).compareAndSet(false, true)) {
                    long offset = state.committedOffset(partition);
                    long end = broker.logEndOffset(partition);
                    if (offset < end) {
                        try {
                            workers.execute(() -> processPartition(partition));
                            scheduledAny = true;
                        } catch (RuntimeException e) {
                            // 线程池拒绝/关闭：释放槽位，下轮重试，事件不丢失
                            inflight.get(partition).set(false);
                            if (!running) {
                                return;
                            }
                            throw e;
                        }
                    } else {
                        inflight.get(partition).set(false);
                    }
                }
            }
            if (!scheduledAny) {
                try {
                    Thread.sleep(pollIntervalMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void processPartition(String partition) {
        try {
            while (running) {
                long offset = state.committedOffset(partition);
                long end = broker.logEndOffset(partition);
                if (offset >= end) {
                    return;
                }
                Optional<EventEnvelope> found = broker.read(partition, offset);
                if (found.isEmpty()) {
                    // 理论上不会发生（end > offset），防御性退出等待下轮
                    log.warn("missing event at partition={}, offset={}", partition, offset);
                    return;
                }
                // 成功提交后继续拉同分区下一条；处于退避等待时任务结束、槽位释放
                processOne(partition, offset, found.get());
                if (state.committedOffset(partition) <= offset) {
                    return;
                }
            }
        } finally {
            inflight.get(partition).set(false);
        }
    }

    private void processOne(String partition, long offset, EventEnvelope event) {
        // 注意：本方法不负责释放分区在途槽位——processPartition 在同分区连续处理多条事件时
        // 会循环调用本方法；槽位只能由其最外层 finally 释放一次，否则调度器会在处理间隙
        // 为同一分区重复派发任务，破坏“同分区串行 + 位点/死信一致性”。
        while (true) {
            int attempt = processAttempt(partition, offset, event);
            if (attempt == 0) {
                // 事件已提交（成功/死信/重复）或因回放中止
                return;
            }
            // attempt > 0：第 attempt 次处理失败且仍可重试，退避后继续（不持锁）
            long backoff = retryPolicy.backoffAfter(attempt).toMillis();
            if (backoff > 0) {
                // 不持分区锁睡眠：rewind 可在此期间介入；
                // 槽位仍被本任务占用，同分区后续事件不会插队（保序）。
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            if (state.committedOffset(partition) != offset || !running) {
                log.info("decision=ABORT_AFTER_BACKOFF eventId={} partition={} offset={} attempt={}",
                        event.eventId(), partition, offset, attempt);
                return;
            }
        }
    }

    /**
     * 执行一次处理尝试。
     *
     * @return 0 表示事件生命周期已结束（成功提交 / 死信 / 重复 / 回放中止）；
     *         正数表示该次尝试失败、需要按返回的尝试次数继续重试
     */
    private int processAttempt(String partition, long offset, EventEnvelope event) {
        ReentrantLock lock = partitionLocks.get(partition);
        lock.lock();
        try {
            // 拿到分区锁后重新校验位点（退避期间可能已被回放/其他路径处理）
            long current = state.committedOffset(partition);
            if (current != offset) {
                log.info("decision=SKIP_STALE eventId={} partition={} offset={} currentOffset={}",
                        event.eventId(), partition, offset, current);
                return 0;
            }
            if (deadLetters.containsLocation(partition, offset)) {
                // 死信位点的明确防线：正常消费流绝不跳过死信事件，也不会与成功同时发生。
                // 正常路径不可达（死信转移与位点推进在同一锁内完成），仅作防御。
                DeadLetterRecord dlq = deadLetters.findByLocation(partition, offset).orElseThrow();
                log.error("decision=DEAD_LETTER_BLOCKED eventId={} partition={} offset={} reason='{}'",
                        event.eventId(), partition, offset, dlq.reason());
                return 0;
            }
            if (state.isProcessed(event.eventId())) {
                // 重复投递：位点被回放到了已成功处理的事件，副作用不得再次生效
                log.info("decision=SKIP_DUPLICATE eventId={} partition={} offset={} basis='processed-event-id'",
                        event.eventId(), partition, offset);
                state.commit(partition, offset + 1);
                return 0;
            }
            int attempt = state.incrementAttempts(partition, offset);
            long start = System.currentTimeMillis();
            try {
                handler.handle(event);
            } catch (Exception failure) {
                String cause = failure.getClass().getSimpleName() + ": " + failure.getMessage();
                if (retryPolicy.shouldRetry(attempt)) {
                    log.warn("decision=RETRY eventId={} partition={} offset={} attempt={} nextBackoffMs={} cause='{}'",
                            event.eventId(), partition, offset, attempt,
                            retryPolicy.backoffAfter(attempt).toMillis(), cause);
                    return attempt;
                }
                String reason = "RETRY_EXHAUSTED attempts=" + attempt
                        + " maxAttempts=" + retryPolicy.maxAttempts() + " cause=" + cause;
                DeadLetterRecord record = new DeadLetterRecord(
                        event, partition, offset, attempt, reason, System.currentTimeMillis());
                // 先转死信、再推进位点，二者在分区锁内顺序完成，不会成功与死信并存
                deadLetters.put(record);
                state.commit(partition, offset + 1);
                log.error("decision=DEAD_LETTER eventId={} partition={} offset={} attempts={} basis='retries-exhausted' reason='{}'",
                        event.eventId(), partition, offset, attempt, reason);
                return 0;
            }
            // 业务无异常：幂等标记 + 位点推进在分区锁内原子完成。
            // 到达此处的事件此前必未处理过（已在上方按 eventId 拦截），故 markProcessed 必为 true。
            state.markProcessed(event.eventId());
            state.commit(partition, offset + 1);
            log.info("decision=SUCCEED eventId={} partition={} offset={} attempt={} elapsedMs={} basis='first-success'",
                    event.eventId(), partition, offset, attempt, System.currentTimeMillis() - start);
            return 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 等待所有已入队事件被处理完成（成功或死信）。
     */
    public boolean awaitIdle(long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            boolean idle = true;
            for (String partition : broker.partitions()) {
                if (state.committedOffset(partition) < broker.logEndOffset(partition)) {
                    idle = false;
                    break;
                }
                if (inflight.get(partition).get()) {
                    idle = false;
                    break;
                }
            }
            if (idle) {
                return true;
            }
            Thread.sleep(pollIntervalMillis);
        }
        return false;
    }

    /**
     * 位点回放（重启后重放）。位点必须落在合法且未死信的位置，否则抛出可区分异常：
     * <ul>
     *   <li>{@link OffsetOutOfRangeException}：负数 / 越界；</li>
     *   <li>{@link OffsetInvalidException}：目标事件已死信（需先移除死信再重放）。</li>
     * </ul>
     */
    public void rewind(String partition, long offset) {
        long startOffset = broker.logStartOffset(partition);
        long endOffset = broker.logEndOffset(partition);
        if (offset < 0) {
            throw new OffsetOutOfRangeException(OffsetOutOfRangeException.Reason.NEGATIVE_OFFSET,
                    "offset must not be negative: partition=" + partition + ", offset=" + offset);
        }
        if (offset < startOffset) {
            throw new OffsetOutOfRangeException(OffsetOutOfRangeException.Reason.OFFSET_TRUNCATED,
                    "offset below log start: partition=" + partition + ", offset=" + offset
                            + ", logStart=" + startOffset);
        }
        if (offset >= endOffset) {
            throw new OffsetOutOfRangeException(OffsetOutOfRangeException.Reason.OFFSET_TOO_HIGH,
                    "offset beyond log end: partition=" + partition + ", offset=" + offset
                            + ", logEnd=" + endOffset);
        }
        Optional<DeadLetterRecord> dlq = deadLetters.findByLocation(partition, offset);
        if (dlq.isPresent()) {
            throw new OffsetInvalidException(
                    "offset points to dead-lettered event: partition=" + partition
                            + ", offset=" + offset + ", eventId=" + dlq.get().event().eventId()
                            + ", reason='" + dlq.get().reason() + "'");
        }
        ReentrantLock lock = partitionLocks.get(partition);
        // 等待在途任务退出后持锁回放。等待与加锁之间调度器可能又派发了新任务
        // （新任务会阻塞在分区锁上，拿到锁后按位点失效跳过），因此用重试循环消除该竞态。
        while (true) {
            while (inflight.get(partition).get()) {
                Thread.onSpinWait();
                if (!running) {
                    throw new IllegalStateException("consumer is not running");
                }
            }
            lock.lock();
            try {
                if (inflight.get(partition).get()) {
                    // 极端竞态：等待期间新任务已启动，放开锁重来
                    continue;
                }
                // 等待在途任务期间，事件可能刚刚转入死信，复核后给出可区分拒绝
                Optional<DeadLetterRecord> nowDead = deadLetters.findByLocation(partition, offset);
                if (nowDead.isPresent()) {
                    throw new OffsetInvalidException(
                            "offset points to dead-lettered event: partition=" + partition
                                    + ", offset=" + offset + ", eventId=" + nowDead.get().event().eventId()
                                    + ", reason='" + nowDead.get().reason() + "'");
                }
                state.seek(partition, offset);
                state.resetAttemptsFrom(partition, offset);
                log.info("decision=REWIND partition={} offset={} basis='explicit-replay'", partition, offset);
                return;
            } finally {
                lock.unlock();
            }
        }
    }

    public long committedOffset(String partition) {
        return state.committedOffset(partition);
    }
}
