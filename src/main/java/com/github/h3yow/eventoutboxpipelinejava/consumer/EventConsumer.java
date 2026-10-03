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
import com.github.h3yow.eventoutboxpipelinejava.state.AggregateStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 异步消费者。
 *
 * <p>隔离粒度：<b>分区内按聚合（aggregateKey）调度</b>。一个分区上可能挤着上万个
 * 互不相干的聚合，但只有固定数量的工作线程：
 * <ul>
 *   <li>同一聚合任意时刻最多一个在途任务，事件严格按位点串行处理，不乱序、不重复；</li>
 *   <li>不同聚合的任务可并行：一个聚合的慢事件/退避睡眠只占用该聚合自己的槽位，
 *       不阻塞同分区其他聚合推进；</li>
 *   <li>调度器按“最久未派发优先”选择候选聚合，新聚合（从未派发）优先级最高，
 *       某个聚合积压再多也不会饿死其他聚合；</li>
 *   <li>全局许可（数量等于 worker 数）保证在途任务数不超过工作线程数。</li>
 * </ul>
 *
 * <p>位点齐平：不同聚合并行结束会使位点乱序完成。先结束的位点进入每分区的
 * 终态位点集合，由完成任务在分区锁内把连续前缀折叠进 committedOffset；
 * 聚合状态、成功/死信的互斥以及“处理 + 终态 + 位点”的一致性不依赖全局串行。
 *
 * <p>回放：显式 seek 在分区锁内等待全部在途任务退出，随后代际号自增并重建
 * 调度队列；代际过期的旧任务即使正从退避中醒来也不会再触碰业务或位点。
 */
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    /** 一个待派发的聚合候选；lastDispatchNanos 越小优先级越高（防饿死）。 */
    private record Candidate(String partition, String aggregate, long offset, long lastDispatchNanos) {
    }

    /** 单分区调度状态：仅在分区锁内访问（dispatcher 与 rewind 持锁，worker 折叠时持锁）。 */
    private static final class PartitionSchedule {
        private final ReentrantLock lock = new ReentrantLock();
        // aggregateKey -> 该聚合按位点排列的待处理位点
        private final Map<String, Deque<Long>> queues = new ConcurrentHashMap<>();
        // aggregateKey -> 是否有在途任务（worker 在锁外读取自身标志时仍按代际判定是否有权清理）
        private final Map<String, AtomicBoolean> inflight = new ConcurrentHashMap<>();
        private final Map<String, Long> lastDispatchNanos = new ConcurrentHashMap<>();
        private final AtomicLong inflightCount = new AtomicLong();
        private long discoveredUntil = 0;
    }

    private final InMemoryEventBroker broker;
    private final ConsumerGroupState state;
    private final InMemoryDeadLetterStore deadLetters;
    private final AggregateStateStore aggregateState;
    private final EventHandler handler;
    private final RetryPolicy retryPolicy;
    private final int workerCount;
    private final long pollIntervalMillis;

    private final Map<String, PartitionSchedule> schedules = new ConcurrentHashMap<>();
    // 全局在途许可：在途任务数恒 <= workerCount，线程池队列不会因上万聚合积压而膨胀
    private final Semaphore permits;

    private volatile ExecutorService workers;
    private volatile Thread dispatcher;
    private volatile boolean running;

    public EventConsumer(InMemoryEventBroker broker,
                         ConsumerGroupState state,
                         DeadLetterStore deadLetters,
                         EventHandler handler,
                         RetryPolicy retryPolicy,
                         int workerCount) {
        this(broker, state, deadLetters, new AggregateStateStore(), handler, retryPolicy, workerCount, 5L);
    }

    public EventConsumer(InMemoryEventBroker broker,
                         ConsumerGroupState state,
                         DeadLetterStore deadLetters,
                         AggregateStateStore aggregateState,
                         EventHandler handler,
                         RetryPolicy retryPolicy,
                         int workerCount) {
        this(broker, state, deadLetters, aggregateState, handler, retryPolicy, workerCount, 5L);
    }

    public EventConsumer(InMemoryEventBroker broker,
                         ConsumerGroupState state,
                         DeadLetterStore deadLetters,
                         EventHandler handler,
                         RetryPolicy retryPolicy,
                         int workerCount,
                         long pollIntervalMillis) {
        this(broker, state, deadLetters, new AggregateStateStore(), handler,
                retryPolicy, workerCount, pollIntervalMillis);
    }

    public EventConsumer(InMemoryEventBroker broker,
                         ConsumerGroupState state,
                         DeadLetterStore deadLetters,
                         AggregateStateStore aggregateState,
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
        this.aggregateState = aggregateState;
        this.handler = handler;
        this.retryPolicy = retryPolicy;
        this.workerCount = Math.max(1, workerCount);
        this.pollIntervalMillis = Math.max(1, pollIntervalMillis);
        this.permits = new Semaphore(this.workerCount, false);
        for (String partition : broker.partitions()) {
            schedules.put(partition, new PartitionSchedule());
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
        log.info("EventConsumer started: partitions={}, workers={}, maxAttempts={}, isolation=aggregate",
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

    // ---- 调度 ----

    private void dispatchLoop() {
        while (running) {
            boolean scheduledAny = false;
            List<Candidate> candidates = new ArrayList<>();
            for (String partition : broker.partitions()) {
                if (!running) {
                    return;
                }
                PartitionSchedule schedule = schedules.get(partition);
                schedule.lock.lock();
                try {
                    discover(partition, schedule);
                    collectCandidates(partition, schedule, candidates);
                } finally {
                    schedule.lock.unlock();
                }
            }
            // 公平：最久未派发的聚合最先；从未派发者（lastDispatch=0）最先，避免被重负载聚合饿死
            candidates.sort((a, b) -> Long.compare(a.lastDispatchNanos(), b.lastDispatchNanos()));
            for (Candidate candidate : candidates) {
                if (!running) {
                    return;
                }
                // 先占全局许可，保证在途任务总数不超过 worker 数
                if (!permits.tryAcquire()) {
                    break;
                }
                PartitionSchedule schedule = schedules.get(candidate.partition());
                AtomicBoolean flag = schedule.inflight.get(candidate.aggregate());
                boolean claimed = flag != null && flag.compareAndSet(false, true);
                if (!claimed) {
                    // 与回放重建竞争导致的极端情况：归还许可，下轮再来
                    permits.release();
                    continue;
                }
                schedule.inflightCount.incrementAndGet();
                schedule.lastDispatchNanos.put(candidate.aggregate(), System.nanoTime());
                long generation = state.generation(candidate.partition());
                try {
                    workers.execute(() -> processAggregate(
                            candidate.partition(), candidate.aggregate(), candidate.offset(), generation));
                    scheduledAny = true;
                } catch (RuntimeException e) {
                    schedule.inflightCount.decrementAndGet();
                    flag.set(false);
                    permits.release();
                    if (!running) {
                        return;
                    }
                    throw e;
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

    /**
     * 扫描日志新增区间，把每条事件按聚合登记到其有序待处理队列尾部。
     * 调用方持有分区锁。
     */
    private void discover(String partition, PartitionSchedule schedule) {
        long end = broker.logEndOffset(partition);
        for (long offset = schedule.discoveredUntil; offset < end; offset++) {
            EventEnvelope event = broker.read(partition, offset).orElse(null);
            if (event == null) {
                // 截断发生在扫描间隙（截断只删连续前缀且水位 <= committed），跳到新起始位点
                long start = broker.logStartOffset(partition);
                if (offset < start) {
                    schedule.discoveredUntil = Math.max(schedule.discoveredUntil, start);
                    offset = start - 1;
                    continue;
                }
                break;
            }
            schedule.queues
                    .computeIfAbsent(event.aggregateKey(), k -> new ConcurrentLinkedDeque<>())
                    .addLast(offset);
            schedule.inflight.computeIfAbsent(event.aggregateKey(), k -> new AtomicBoolean(false));
        }
        schedule.discoveredUntil = end;
    }

    /**
     * 收集当前可派发聚合（无在途任务）及其有效队首位点。
     * 已提交/已截断/已终态的队首残留直接弹出。调用方持有分区锁。
     */
    private void collectCandidates(String partition, PartitionSchedule schedule, List<Candidate> out) {
        long committed = state.committedOffset(partition);
        long logStart = broker.logStartOffset(partition);
        for (Map.Entry<String, Deque<Long>> entry : schedule.queues.entrySet()) {
            String aggregate = entry.getKey();
            AtomicBoolean flag = schedule.inflight.get(aggregate);
            if (flag != null && flag.get()) {
                continue; // 同聚合并发禁止：上一条仍在处理（含退避等待）
            }
            Long head = entry.getValue().peekFirst();
            while (head != null) {
                if (head >= committed && head >= logStart
                        && !state.isTerminalOffset(partition, head)) {
                    break; // 有效待处理位点
                }
                entry.getValue().pollFirst();
                head = entry.getValue().peekFirst();
            }
            if (head == null) {
                continue;
            }
            out.add(new Candidate(partition, aggregate, head,
                    schedule.lastDispatchNanos.getOrDefault(aggregate, 0L)));
        }
    }

    // ---- 任务执行 ----

    private void processAggregate(String partition, String aggregate, long startOffset, long generation) {
        boolean finished = false;
        try {
            long offset = startOffset;
            while (running) {
                if (state.generation(partition) != generation) {
                    log.info("decision=ABORT_STALE_GENERATION eventId=? partition={} offset={} aggregateKey={} basis='rewind-occurred'",
                            partition, offset, aggregate);
                    return;
                }
                Optional<EventEnvelope> found = broker.read(partition, offset);
                if (found.isEmpty()) {
                    log.warn("missing event at partition={}, offset={}", partition, offset);
                    finished = true;
                    return;
                }
                EventEnvelope event = found.get();
                int attempt = processAttempt(partition, aggregate, offset, event, generation);
                if (attempt == 0) {
                    finished = true;
                    return; // 事件已终态（成功/死信/重复/防御性死信拦截）
                }
                // attempt > 0：失败且仍可重试，不释放聚合槽位（同聚合后续不得插队），退避后继续
                long backoff = retryPolicy.backoffAfter(attempt).toMillis();
                if (backoff > 0) {
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                if (state.generation(partition) != generation || !running) {
                    log.info("decision=ABORT_AFTER_BACKOFF eventId={} partition={} offset={} attempt={} basis='generation-or-stop'",
                            event.eventId(), partition, offset, attempt);
                    return;
                }
            }
        } finally {
            releaseTask(partition, aggregate, startOffset, generation, finished);
        }
    }

    private void releaseTask(String partition, String aggregate, long offset,
                             long generation, boolean finished) {
        PartitionSchedule schedule = schedules.get(partition);
        if (finished && state.generation(partition) == generation) {
            // 同代际且事件终态：弹出队首并释放本聚合槽位（均在分区锁内，与 discover/rewind 互斥）
            schedule.lock.lock();
            try {
                if (state.generation(partition) == generation) {
                    Deque<Long> queue = schedule.queues.get(aggregate);
                    if (queue != null && queue.peekFirst() != null && queue.peekFirst() == offset) {
                        queue.pollFirst();
                    }
                    AtomicBoolean flag = schedule.inflight.get(aggregate);
                    if (flag != null) {
                        flag.set(false);
                    }
                }
            } finally {
                schedule.lock.unlock();
            }
        }
        // 代际过期（回放已重建队列与标志）或任务中止：不得清理新代际的调度状态
        schedule.inflightCount.decrementAndGet();
        permits.release();
    }

    /**
     * 执行一次处理尝试。
     *
     * @return 0 表示事件生命周期已结束（成功 / 死信 / 重复 / 拦截 / 代际过期）；
     *         正数表示该次尝试失败、需要按返回的尝试次数继续重试
     */
    private int processAttempt(String partition, String aggregate, long offset,
                               EventEnvelope event, long generation) {
        if (state.generation(partition) != generation) {
            return 0;
        }
        long committed = state.committedOffset(partition);
        if (offset < committed || state.isTerminalOffset(partition, offset)) {
            log.info("decision=SKIP_STALE eventId={} partition={} offset={} currentOffset={}",
                    event.eventId(), partition, offset, committed);
            return 0;
        }
        if (deadLetters.containsLocation(partition, offset)) {
            DeadLetterRecord dlq = deadLetters.findByLocation(partition, offset).orElseThrow();
            log.error("decision=DEAD_LETTER_BLOCKED eventId={} partition={} offset={} reason='{}'",
                    event.eventId(), partition, offset, dlq.reason());
            state.markTerminalOffset(partition, offset);
            advanceCommitted(partition, generation);
            return 0;
        }
        if (state.isProcessed(event.eventId())) {
            // 重复投递：位点被回放到了已成功处理的事件，副作用不得再次生效
            log.info("decision=SKIP_DUPLICATE eventId={} partition={} offset={} aggregateKey={} basis='processed-event-id'",
                    event.eventId(), partition, offset, aggregate);
            state.markTerminalOffset(partition, offset);
            advanceCommitted(partition, generation);
            return 0;
        }
        int attempt = state.incrementAttempts(partition, offset);
        long start = System.currentTimeMillis();
        try {
            handler.handle(event);
        } catch (Exception failure) {
            String cause = failure.getClass().getSimpleName() + ": " + failure.getMessage();
            if (retryPolicy.shouldRetry(attempt)) {
                log.warn("decision=RETRY eventId={} partition={} offset={} aggregateKey={} attempt={} nextBackoffMs={} cause='{}'",
                        event.eventId(), partition, offset, aggregate, attempt,
                        retryPolicy.backoffAfter(attempt).toMillis(), cause);
                return attempt;
            }
            String reason = "RETRY_EXHAUSTED attempts=" + attempt
                    + " maxAttempts=" + retryPolicy.maxAttempts() + " cause=" + cause;
            DeadLetterRecord record = new DeadLetterRecord(
                    event, partition, offset, attempt, reason, System.currentTimeMillis());
            deadLetters.put(record);
            state.markTerminalOffset(partition, offset);
            advanceCommitted(partition, generation);
            log.error("decision=DEAD_LETTER eventId={} partition={} offset={} aggregateKey={} attempts={} basis='retries-exhausted' reason='{}'",
                    event.eventId(), partition, offset, aggregate, attempt, reason);
            return 0;
        }
        // 业务无异常：幂等标记 + 聚合当前状态 + 终态位点；副作用对该 eventId 只生效一次
        state.markProcessed(event.eventId());
        aggregateState.recordSuccess(aggregate, event, partition, offset);
        state.markTerminalOffset(partition, offset);
        advanceCommitted(partition, generation);
        log.info("decision=SUCCEED eventId={} partition={} offset={} aggregateKey={} attempt={} elapsedMs={} basis='first-success'",
                event.eventId(), partition, offset, aggregate, attempt,
                System.currentTimeMillis() - start);
        return 0;
    }

    /**
     * 在分区锁内把已终态的连续前缀折叠进 committedOffset。
     * 代际过期（回放刚发生）时跳过：旧任务无权触碰新位点。
     */
    private void advanceCommitted(String partition, long generation) {
        PartitionSchedule schedule = schedules.get(partition);
        schedule.lock.lock();
        try {
            if (state.generation(partition) != generation) {
                return;
            }
            long next = state.committedOffset(partition);
            while (state.isTerminalOffset(partition, next)) {
                state.removeTerminalOffset(partition, next);
                next++;
            }
            state.commit(partition, next);
        } finally {
            schedule.lock.unlock();
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
                if (schedules.get(partition).inflightCount.get() > 0) {
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
     *   <li>{@link OffsetOutOfRangeException}：负数 / 已被清理截断 / 超过日志末尾；</li>
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
                    "offset below log start (cleaned up): partition=" + partition + ", offset=" + offset
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
        PartitionSchedule schedule = schedules.get(partition);
        // 等待该分区全部在途聚合任务退出后持锁回放；旧任务折叠位点也要拿这把锁，
        // 因此持锁后必须复核 inflightCount，非零则放开重来（与派发/发现互斥）。
        while (true) {
            while (schedule.inflightCount.get() > 0) {
                Thread.onSpinWait();
                if (!running) {
                    throw new IllegalStateException("consumer is not running");
                }
            }
            schedule.lock.lock();
            try {
                if (schedule.inflightCount.get() > 0) {
                    continue;
                }
                // 等待期间事件可能刚刚转入死信，复核后给出可区分拒绝
                Optional<DeadLetterRecord> nowDead = deadLetters.findByLocation(partition, offset);
                if (nowDead.isPresent()) {
                    throw new OffsetInvalidException(
                            "offset points to dead-lettered event: partition=" + partition
                                    + ", offset=" + offset + ", eventId=" + nowDead.get().event().eventId()
                                    + ", reason='" + nowDead.get().reason() + "'");
                }
                long generation = state.bumpGeneration(partition);
                // 重建调度队列：旧代际任务即使尚未被线程调度也无权触碰业务/位点
                schedule.queues.clear();
                schedule.inflight.clear();
                schedule.lastDispatchNanos.clear();
                schedule.discoveredUntil = offset;
                state.seek(partition, offset);
                state.resetAttemptsFrom(partition, offset);
                state.removeTerminalOffsetsFrom(partition, offset);
                log.info("decision=REWIND partition={} offset={} generation={} basis='explicit-replay'",
                        partition, offset, generation);
                return;
            } finally {
                schedule.lock.unlock();
            }
        }
    }

    public long committedOffset(String partition) {
        return state.committedOffset(partition);
    }
}
