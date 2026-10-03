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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 异步消费者。
 *
 * <p><b>隔离粒度：聚合。</b>同一条物理分区上可承载上万个互不相干的聚合，
 * 调度不再以分区为在途单位，而是以（分区, aggregateKey）为单位：
 * <ul>
 *   <li>每个聚合一条 FIFO 待办队列，同一时刻最多一个工作线程处理该聚合，
 *       因此同聚合内部严格保序、串行、不并发；</li>
 *   <li>一个聚合的慢事件（含退避睡眠）只占用该聚合自己的在途槽，
 *       同分区其它聚合照常被调度推进，互不相挡；</li>
 *   <li>工作线程每处理完一张待办就把该聚合的队列让渡回就绪队列尾部，
 *       任何聚合都不会因为别的聚合积压过多而永久得不到线程。</li>
 * </ul>
 *
 * <p><b>位点：</b>不同聚合可乱序完成；单个事件终态（成功/死信/重复跳过）
 * 先记入按序集合，分区水位线只沿“连续终态前缀”推进。
 *
 * <p><b>一致性：</b>“业务处理 + 死信转移 + 终态推进”在同一分区锁内完成；
 * 已成功处理过的 eventId 永不重复生效。回放使用代际号让在途旧任务失效。
 */
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    /**
     * 一张待办票：扫描时创建，携带事件原文（即使日志随后被截断也不影响在途处理）
     * 与扫描时刻的分区代际号。
     */
    private record Ticket(String partition, int partitionIndex, String aggregateKey,
                          long offset, EventEnvelope event, long generation) {
    }

    /**
     * 单个聚合的 FIFO 队列 + 调度所有权标志。{@code scheduled=true} 表示该队列
     * 已在就绪队列中或正被某个工作线程持有，调度器不会重复派发。
     */
    private static final class AggregateQueue {
        private final String key;
        private final ConcurrentLinkedQueue<Ticket> tickets = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean scheduled = new AtomicBoolean(false);
        private final Object lock = new Object();

        private AggregateQueue(String key) {
            this.key = key;
        }

        /** 入队；仅在 无在途任务 -> 有在途任务 的翻转时加入就绪队列。 */
        private void offer(Ticket t, ConcurrentLinkedQueue<AggregateQueue> ready) {
            synchronized (lock) {
                tickets.add(t);
                if (scheduled.compareAndSet(false, true)) {
                    ready.add(this);
                }
            }
        }

        /**
         * 取出队首。必须在同一把锁内“判空 + 交出所有权”：
         * offer() 是“先置 scheduled、再入队”，若拆成两步会在交错窗口里丢票。
         */
        private Ticket acquire() {
            synchronized (lock) {
                Ticket t = tickets.poll();
                if (t == null) {
                    scheduled.set(false);
                }
                return t;
            }
        }

        /** 处理完一张票后让渡：仍有票则回到就绪队列尾部（公平轮转），否则交出所有权。 */
        private boolean yieldWithRemaining(ConcurrentLinkedQueue<AggregateQueue> ready) {
            synchronized (lock) {
                if (tickets.isEmpty()) {
                    scheduled.set(false);
                    return false;
                }
                ready.add(this);
                return true;
            }
        }

        /** 回放时清空：返回被丢弃的票数（用于校正在途计数）。 */
        private int drain() {
            synchronized (lock) {
                int n = 0;
                while (tickets.poll() != null) {
                    n++;
                }
                scheduled.set(false);
                return n;
            }
        }
    }

    private final InMemoryEventBroker broker;
    private final ConsumerGroupState state;
    private final InMemoryDeadLetterStore deadLetters;
    private final EventHandler handler;
    private final RetryPolicy retryPolicy;
    private final int workerCount;
    private final long pollIntervalMillis;
    private final long retentionIntervalMillis;

    private final ReentrantLock[] partitionLocks;
    private final AtomicLong[] generations;
    private final boolean[] paused;
    private final AtomicLong[] scanCursors;

    private final ConcurrentMap<String, AggregateQueue> aggregateQueues = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<AggregateQueue> readyQueue = new ConcurrentLinkedQueue<>();
    // 每分区已派发未终态的票数（含排队中与处理中），awaitIdle 依据
    private final ConcurrentMap<String, AtomicInteger> pendingTickets = new ConcurrentHashMap<>();
    // 每分区当前正在工作线程内（含退避睡眠）的票数，回放等待依据
    private final ConcurrentMap<String, AtomicInteger> inflightTickets = new ConcurrentHashMap<>();

    private volatile ExecutorService workers;
    private volatile Thread dispatcher;
    private volatile Thread retention;
    private volatile boolean running;

    public EventConsumer(InMemoryEventBroker broker,
                         ConsumerGroupState state,
                         DeadLetterStore deadLetters,
                         EventHandler handler,
                         RetryPolicy retryPolicy,
                         int workerCount) {
        this(broker, state, deadLetters, handler, retryPolicy, workerCount, 5L, 0L);
    }

    public EventConsumer(InMemoryEventBroker broker,
                         ConsumerGroupState state,
                         DeadLetterStore deadLetters,
                         EventHandler handler,
                         RetryPolicy retryPolicy,
                         int workerCount,
                         long pollIntervalMillis) {
        this(broker, state, deadLetters, handler, retryPolicy, workerCount, pollIntervalMillis, 0L);
    }

    public EventConsumer(InMemoryEventBroker broker,
                         ConsumerGroupState state,
                         DeadLetterStore deadLetters,
                         EventHandler handler,
                         RetryPolicy retryPolicy,
                         int workerCount,
                         long pollIntervalMillis,
                         long retentionIntervalMillis) {
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
        // <=0 表示不启动后台保留清理，仅允许手动触发
        this.retentionIntervalMillis = retentionIntervalMillis;
        int n = broker.partitions().size();
        this.partitionLocks = new ReentrantLock[n];
        this.generations = new AtomicLong[n];
        this.paused = new boolean[n];
        this.scanCursors = new AtomicLong[n];
        for (int i = 0; i < n; i++) {
            partitionLocks[i] = new ReentrantLock();
            generations[i] = new AtomicLong(0);
            scanCursors[i] = new AtomicLong(0);
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
        if (retentionIntervalMillis > 0) {
            retention = new Thread(this::retentionLoop, "event-retention");
            retention.setDaemon(true);
            retention.start();
        }
        log.info("EventConsumer started: partitions={}, workers={}, maxAttempts={}, retentionIntervalMs={}",
                broker.partitions().size(), workerCount, retryPolicy.maxAttempts(),
                retentionIntervalMillis);
    }

    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (dispatcher != null) {
            dispatcher.interrupt();
        }
        if (retention != null) {
            retention.interrupt();
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

    // ------------------------------------------------------------------
    // 调度
    // ------------------------------------------------------------------

    private void dispatchLoop() {
        while (running) {
            boolean anyReady = scanPartitions();
            dispatchReadyQueues();
            if (!anyReady && readyQueue.isEmpty()) {
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
     * 扫描各分区新增日志，按聚合拆成待办票。扫描持分区锁，与回放互斥；
     * 被暂停（回放中）的分区跳过。
     */
    private boolean scanPartitions() {
        boolean added = false;
        List<String> partitions = broker.partitions();
        for (int index = 0; index < partitions.size(); index++) {
            if (!running) {
                break;
            }
            String partition = partitions.get(index);
            ReentrantLock lock = partitionLocks[index];
            lock.lock();
            try {
                if (paused[index]) {
                    continue;
                }
                long end = broker.logEndOffset(partition);
                long cursor = scanCursors[index].get();
                long generation = generations[index].get();
                for (long offset = cursor; offset < end; offset++) {
                    final long currentOffset = offset;
                    EventEnvelope event = broker.read(partition, currentOffset)
                            .orElseThrow(() -> new IllegalStateException(
                                    "missing event at " + partition + "#" + currentOffset));
                    Ticket ticket = new Ticket(partition, index,
                            event.aggregateKey(), currentOffset, event, generation);
                    String key = index + "#" + event.aggregateKey();
                    AggregateQueue queue = aggregateQueues.computeIfAbsent(key, AggregateQueue::new);
                    queue.offer(ticket, readyQueue);
                    pendingTickets.computeIfAbsent(partition, k -> new AtomicInteger()).incrementAndGet();
                    added = true;
                }
                scanCursors[index].set(end);
            } finally {
                lock.unlock();
            }
        }
        return added;
    }

    private void dispatchReadyQueues() {
        AggregateQueue queue;
        while (running && (queue = readyQueue.poll()) != null) {
            final AggregateQueue target = queue;
            try {
                workers.execute(() -> runAggregate(target));
            } catch (RuntimeException e) {
                // 线程池拒绝/关闭：放回就绪队列，下轮再派发（不丢失调度所有权）
                synchronized (target.lock) {
                    if (target.scheduled.get()) {
                        readyQueue.add(target);
                    }
                }
                if (!running) {
                    return;
                }
            }
        }
    }

    /**
     * 工作线程对单个聚合的一次“所有权轮次”：串行处理该聚合的票，
     * 每处理完一张就让渡回就绪队列尾部，与其它聚合公平轮转。
     */
    private void runAggregate(AggregateQueue queue) {
        while (running) {
            Ticket ticket = queue.acquire();
            if (ticket == null) {
                return;
            }
            boolean finished;
            try {
                finished = processTicket(ticket);
            } catch (RuntimeException e) {
                log.error("unexpected error while processing eventId={} partition={} offset={}",
                        ticket.event().eventId(), ticket.partition(), ticket.offset(), e);
                finished = false;
            }
            decrement(pendingTickets, ticket.partition());
            if (finished) {
                if (queue.yieldWithRemaining(readyQueue)) {
                    return; // 所有权经就绪队列移交给下一个工作线程（公平）
                }
            } else {
                // 中止（回放代际变化 / 停机）：交回队列让后续轮次清理陈旧票
                synchronized (queue.lock) {
                    if (queue.tickets.isEmpty()) {
                        queue.scheduled.set(false);
                    } else {
                        readyQueue.add(queue);
                    }
                }
                return;
            }
        }
    }

    private void decrement(ConcurrentMap<String, AtomicInteger> counters, String partition) {
        AtomicInteger value = counters.get(partition);
        if (value != null) {
            value.decrementAndGet();
        }
    }

    /**
     * 处理一张票（含同票内的重试退避循环）。退避期间不持任何锁，
     * 只占用该聚合自身的在途槽：同聚合后续事件不插队，其它聚合不受影响。
     *
     * @return true 表示事件已终态；false 表示因回放代际变化或停机中止
     */
    private boolean processTicket(Ticket ticket) {
        inflightTickets.computeIfAbsent(ticket.partition(), k -> new AtomicInteger()).incrementAndGet();
        try {
            while (true) {
                int attempt = processAttempt(ticket);
                if (attempt == 0) {
                    return true;
                }
                long backoff = retryPolicy.backoffAfter(attempt).toMillis();
                if (backoff > 0) {
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
                if (!running || generations[ticket.partitionIndex()].get() != ticket.generation()) {
                    log.info("decision=ABORT_AFTER_BACKOFF eventId={} aggregateKey={} partition={} offset={} attempt={} basis='generation-changed-or-stopped'",
                            ticket.event().eventId(), ticket.aggregateKey(),
                            ticket.partition(), ticket.offset(), attempt);
                    return false;
                }
                if (state.isTerminal(ticket.partition(), ticket.offset())) {
                    log.info("decision=ABORT_AFTER_BACKOFF eventId={} aggregateKey={} partition={} offset={} attempt={} basis='already-terminal'",
                            ticket.event().eventId(), ticket.aggregateKey(),
                            ticket.partition(), ticket.offset(), attempt);
                    return false;
                }
            }
        } finally {
            decrement(inflightTickets, ticket.partition());
        }
    }

    /**
     * 执行一次处理尝试。分区锁只保护“状态判定/变更”的短临界区，
     * <b>业务处理器在锁外执行</b>：慢事件不会挡住同分区其它聚合，也不会卡住扫描。
     * 与回放的互斥由 in-flight 等待保证：成功事件的幂等标记一定在回放重置前落地。
     *
     * @return 0 表示事件生命周期已结束（成功/死信/重复/陈旧中止）；
     *         正数表示该次尝试失败、需按返回尝试次数继续重试
     */
    private int processAttempt(Ticket ticket) {
        String partition = ticket.partition();
        int index = ticket.partitionIndex();
        long offset = ticket.offset();
        EventEnvelope event = ticket.event();
        ReentrantLock lock = partitionLocks[index];

        // ---- 锁内：处理前判定 + 重试计数 ----
        int attempt;
        lock.lock();
        try {
            if (generations[index].get() != ticket.generation()) {
                log.info("decision=SKIP_STALE eventId={} aggregateKey={} partition={} offset={} basis='generation-changed'",
                        event.eventId(), ticket.aggregateKey(), partition, offset);
                return 0;
            }
            if (state.isTerminal(partition, offset)) {
                log.info("decision=SKIP_STALE eventId={} aggregateKey={} partition={} offset={} basis='already-terminal'",
                        event.eventId(), ticket.aggregateKey(), partition, offset);
                return 0;
            }
            if (deadLetters.containsLocation(partition, offset)) {
                // 正常路径不可达（死信转移与终态推进成对完成，且截断不得越过死信位点）。
                DeadLetterRecord dlq = deadLetters.findByLocation(partition, offset).orElseThrow();
                state.markTerminal(partition, offset);
                log.error("decision=DEAD_LETTER_BLOCKED eventId={} aggregateKey={} partition={} offset={} basis='dlq-record-at-offset' reason='{}'",
                        event.eventId(), ticket.aggregateKey(), partition, offset, dlq.reason());
                return 0;
            }
            if (state.isProcessed(event.eventId())) {
                // 重复投递：位点被回放到了已成功处理的事件，副作用不得再次生效
                state.markTerminal(partition, offset);
                log.info("decision=SKIP_DUPLICATE eventId={} aggregateKey={} partition={} offset={} basis='processed-event-id'",
                        event.eventId(), ticket.aggregateKey(), partition, offset);
                return 0;
            }
            attempt = state.incrementAttempts(partition, offset);
        } finally {
            lock.unlock();
        }

        // ---- 锁外：业务处理（慢事件只占用该聚合自己的在途槽）----
        long start = System.currentTimeMillis();
        boolean succeeded;
        Exception failure;
        try {
            handler.handle(event);
            succeeded = true;
            failure = null;
        } catch (Exception e) {
            succeeded = false;
            failure = e;
        }

        // ---- 锁内：结果落盘（与死信转移、清理截断互斥）----
        lock.lock();
        try {
            if (succeeded) {
                // 副作用已经发生：无论回放是否已把代际递增，幂等标记都必须落地，
                // 否则回放重投会让副作用生效第二次。
                state.markProcessed(event.eventId());
                state.markTerminal(partition, offset);
                log.info("decision=SUCCEED eventId={} aggregateKey={} partition={} offset={} attempt={} elapsedMs={} basis='first-success'",
                        event.eventId(), ticket.aggregateKey(), partition, offset, attempt,
                        System.currentTimeMillis() - start);
                return 0;
            }
            String cause = failure.getClass().getSimpleName() + ": " + failure.getMessage();
            if (generations[index].get() != ticket.generation()) {
                // 处理期间发生回放：失败结果作废，由回放后的新代际重新处理
                log.info("decision=SKIP_STALE eventId={} aggregateKey={} partition={} offset={} basis='generation-changed-after-failure' cause='{}'",
                        event.eventId(), ticket.aggregateKey(), partition, offset, cause);
                return 0;
            }
            if (retryPolicy.shouldRetry(attempt)) {
                log.warn("decision=RETRY eventId={} aggregateKey={} partition={} offset={} attempt={} nextBackoffMs={} cause='{}'",
                        event.eventId(), ticket.aggregateKey(), partition, offset, attempt,
                        retryPolicy.backoffAfter(attempt).toMillis(), cause);
                return attempt;
            }
            String reason = "RETRY_EXHAUSTED attempts=" + attempt
                    + " maxAttempts=" + retryPolicy.maxAttempts() + " cause=" + cause;
            DeadLetterRecord record = new DeadLetterRecord(
                    event, partition, offset, attempt, reason, System.currentTimeMillis());
            // 先转死信、再推进终态，二者在分区锁内顺序完成，不会成功与死信并存
            deadLetters.put(record);
            state.markTerminal(partition, offset);
            log.error("decision=DEAD_LETTER eventId={} aggregateKey={} partition={} offset={} attempts={} basis='retries-exhausted' reason='{}'",
                    event.eventId(), ticket.aggregateKey(), partition, offset, attempt, reason);
            return 0;
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // 保留清理（后台，不阻挡入队与消费）
    // ------------------------------------------------------------------

    private void retentionLoop() {
        while (running) {
            try {
                Thread.sleep(retentionIntervalMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (running) {
                sweepRetention();
            }
        }
    }

    /**
     * 执行一轮安全清理。每个分区的安全截断点 =
     * min(连续终态水位线, 该分区最早死信位点)：
     * 只回收“已彻底结束”的连续前缀，未轮到、重试中、死信区及其之后的事件一律保留。
     * 截断与消费/死信转移在同一分区锁内串行，因此清理永不与消费竞态。
     *
     * @return 本轮实际回收的事件总数
     */
    public int sweepRetention() {
        int removedTotal = 0;
        List<String> partitions = broker.partitions();
        for (int index = 0; index < partitions.size(); index++) {
            String partition = partitions.get(index);
            ReentrantLock lock = partitionLocks[index];
            lock.lock();
            try {
                long logStart = broker.logStartOffset(partition);
                long watermark = state.committedOffset(partition);
                long dlqBound = deadLetters.earliestOffset(partition).orElse(Long.MAX_VALUE);
                long safePoint = Math.min(watermark, dlqBound);
                if (safePoint > logStart) {
                    broker.trimPrefix(partition, safePoint);
                    state.pruneBefore(partition, safePoint);
                    removedTotal += (int) (safePoint - logStart);
                    log.info("decision=RETENTION_TRIM partition={} previousStart={} newStart={} watermark={} earliestDeadLetter={} removedEvents={} basis='contiguous-terminal-prefix'",
                            partition, logStart, safePoint, watermark,
                            dlqBound == Long.MAX_VALUE ? "none" : dlqBound,
                            safePoint - logStart);
                } else {
                    log.info("decision=RETENTION_KEEP partition={} logStart={} watermark={} earliestDeadLetter={} basis='nothing-safely-trimmable'",
                            partition, logStart, watermark,
                            dlqBound == Long.MAX_VALUE ? "none" : dlqBound);
                }
            } finally {
                lock.unlock();
            }
        }
        return removedTotal;
    }

    // ------------------------------------------------------------------
    // 空闲等待与回放
    // ------------------------------------------------------------------

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
                AtomicInteger pending = pendingTickets.get(partition);
                if (pending != null && pending.get() > 0) {
                    idle = false;
                    break;
                }
            }
            if (idle && readyQueue.isEmpty()) {
                return true;
            }
            Thread.sleep(pollIntervalMillis);
        }
        return false;
    }

    /**
     * 位点回放（重启后重放）。位点必须落在合法且未死信的位置，否则抛出可区分异常：
     * <ul>
     *   <li>{@link OffsetOutOfRangeException}：负数 / 已被清理截断 / 超出末尾；</li>
     *   <li>{@link OffsetInvalidException}：目标事件已死信（需先移除死信再重放）。</li>
     * </ul>
     */
    public void rewind(String partition, long offset) {
        int index = partitionIndex(partition);
        long startOffset = broker.logStartOffset(partition);
        long endOffset = broker.logEndOffset(partition);
        if (offset < 0) {
            log.info("decision=SEEK_REJECTED partition={} offset={} reason='negative-offset'", partition, offset);
            throw new OffsetOutOfRangeException(OffsetOutOfRangeException.Reason.NEGATIVE_OFFSET,
                    "offset must not be negative: partition=" + partition + ", offset=" + offset);
        }
        if (offset < startOffset) {
            log.info("decision=SEEK_REJECTED partition={} offset={} logStart={} reason='offset-truncated'",
                    partition, offset, startOffset);
            throw new OffsetOutOfRangeException(OffsetOutOfRangeException.Reason.OFFSET_TRUNCATED,
                    "offset below log start (event data has been trimmed): partition=" + partition
                            + ", offset=" + offset + ", logStart=" + startOffset);
        }
        if (offset >= endOffset) {
            log.info("decision=SEEK_REJECTED partition={} offset={} logEnd={} reason='offset-too-high'",
                    partition, offset, endOffset);
            throw new OffsetOutOfRangeException(OffsetOutOfRangeException.Reason.OFFSET_TOO_HIGH,
                    "offset beyond log end: partition=" + partition + ", offset=" + offset
                            + ", logEnd=" + endOffset);
        }
        Optional<DeadLetterRecord> dlq = deadLetters.findByLocation(partition, offset);
        if (dlq.isPresent()) {
            log.info("decision=SEEK_REJECTED partition={} offset={} eventId={} reason='dead-letter-offset'",
                    partition, offset, dlq.get().event().eventId());
            throw new OffsetInvalidException(
                    "offset points to dead-lettered event: partition=" + partition
                            + ", offset=" + offset + ", eventId=" + dlq.get().event().eventId()
                            + ", reason='" + dlq.get().reason() + "'");
        }

        ReentrantLock lock = partitionLocks[index];
        lock.lock();
        try {
            if (paused[index]) {
                throw new IllegalStateException("partition is paused by another rewind: " + partition);
            }
            paused[index] = true;
            generations[index].incrementAndGet();
        } finally {
            lock.unlock();
        }

        try {
            // 等待本分区在途任务退出（它们观察到代际变化后会立即中止，最坏等一次处理器调用）
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (true) {
                AtomicInteger inflight = inflightTickets.get(partition);
                if (inflight == null || inflight.get() == 0) {
                    break;
                }
                if (!running) {
                    throw new IllegalStateException("consumer is not running");
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("timeout waiting in-flight tasks on " + partition);
                }
                Thread.onSpinWait();
                if ((System.nanoTime() & 0x3F) == 0) {
                    Thread.sleep(1);
                }
            }

            lock.lock();
            try {
                // 等待期间可能有事件刚转入死信，复核后给出可区分拒绝
                Optional<DeadLetterRecord> nowDead = deadLetters.findByLocation(partition, offset);
                if (nowDead.isPresent()) {
                    throw new OffsetInvalidException(
                            "offset points to dead-lettered event: partition=" + partition
                                    + ", offset=" + offset + ", eventId=" + nowDead.get().event().eventId()
                                    + ", reason='" + nowDead.get().reason() + "'");
                }
                // 清空该分区所有聚合的排队待办（陈旧代际，扫描游标重置后会重新派发）
                String prefix = index + "#";
                int drained = 0;
                for (Iterator<Map.Entry<String, AggregateQueue>> it = aggregateQueues.entrySet().iterator();
                     it.hasNext(); ) {
                    Map.Entry<String, AggregateQueue> entry = it.next();
                    if (!entry.getKey().startsWith(prefix)) {
                        continue;
                    }
                    drained += entry.getValue().drain();
                    // 空聚合队列直接移除，避免上万聚合并发后留下空壳
                    it.remove();
                }
                for (int i = 0; i < drained; i++) {
                    decrement(pendingTickets, partition);
                }
                state.seek(partition, offset);
                state.resetAttemptsFrom(partition, offset);
                scanCursors[index].set(offset);
                log.info("decision=REWIND partition={} offset={} drainedQueuedTickets={} basis='explicit-replay'",
                        partition, offset, drained);
            } finally {
                paused[index] = false;
                lock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while rewinding " + partition, e);
        } finally {
            // 异常路径也要解除暂停
            lock.lock();
            try {
                paused[index] = false;
            } finally {
                lock.unlock();
            }
        }
    }

    private int partitionIndex(String partition) {
        List<String> partitions = broker.partitions();
        int index = partitions.indexOf(partition);
        if (index < 0) {
            throw new IllegalArgumentException("unknown partition: " + partition);
        }
        return index;
    }

    public long committedOffset(String partition) {
        return state.committedOffset(partition);
    }
}
