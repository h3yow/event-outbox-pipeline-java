package com.github.h3yow.eventoutboxpipelinejava.cleanup;

import com.github.h3yow.eventoutboxpipelinejava.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.consumer.ConsumerGroupState;
import com.github.h3yow.eventoutboxpipelinejava.dlq.DeadLetterStore;
import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 后台日志清理：把“已彻底结束”的事件前缀物理删除，控制长跑内存。
 *
 * <p>每个分区的安全水位：
 * <pre>
 *     safeWatermark = min(committedOffset, earliestLiveDeadLetterOffset)
 * </pre>
 * 即只删除既已提交（成功 / 死信 / 重复跳过都已越过）、又不早于任何存活死信位点
 * 的连续前缀。因此：
 * <ul>
 *   <li>还没轮到处理的、正在退避重试的：位点 >= committedOffset，不删；</li>
 *   <li>仍在死信区的：earliestLiveDeadLetterOffset 兜底，死信事件及其后不删；</li>
 *   <li>已成功处理、或死信被正式重放移除后越过的事件：可删。</li>
 * </ul>
 *
 * <p>清理在独立守护线程小批量执行（每分区每轮最多 batchSize 条），不与入队/消费
 * 抢全局锁；被删事件的幂等标记同步裁剪，但 eventId 索引保留，旧位点仍能被
 * 明确判定为 OFFSET_TRUNCATED 而非“事件不存在”。
 */
public class EventLogCleaner {

    private static final Logger log = LoggerFactory.getLogger(EventLogCleaner.class);

    private final InMemoryEventBroker broker;
    private final ConsumerGroupState state;
    private final DeadLetterStore deadLetters;
    private final long intervalMillis;
    private final int batchSizePerPartition;

    private volatile Thread cleanerThread;
    private volatile boolean running;

    public EventLogCleaner(InMemoryEventBroker broker,
                           ConsumerGroupState state,
                           DeadLetterStore deadLetters,
                           long intervalMillis,
                           int batchSizePerPartition) {
        this.broker = broker;
        this.state = state;
        this.deadLetters = deadLetters;
        this.intervalMillis = Math.max(1, intervalMillis);
        this.batchSizePerPartition = Math.max(1, batchSizePerPartition);
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        cleanerThread = new Thread(this::cleanLoop, "event-log-cleaner");
        cleanerThread.setDaemon(true);
        cleanerThread.start();
        log.info("EventLogCleaner started: intervalMs={}, batchSize={}",
                intervalMillis, batchSizePerPartition);
    }

    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (cleanerThread != null) {
            cleanerThread.interrupt();
        }
        log.info("EventLogCleaner stopped");
    }

    private void cleanLoop() {
        while (running) {
            try {
                cleanupOnce();
                Thread.sleep(intervalMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // 清理失败不得影响消费主循环
                log.warn("decision=CLEANUP_ERROR cause='{}: {}'",
                        e.getClass().getSimpleName(), e.getMessage());
            }
        }
    }

    /**
     * 执行一轮清理（可手动调用）。
     *
     * @return 本轮所有分区实际删除的事件总数
     */
    public int cleanupOnce() {
        int totalRemoved = 0;
        for (String partition : broker.partitions()) {
            long committed = state.committedOffset(partition);
            Optional<Long> earliestDead = deadLetters.earliestOffset(partition);
            long safeWatermark = earliestDead.map(d -> Math.min(committed, d)).orElse(committed);
            long logStart = broker.logStartOffset(partition);
            if (safeWatermark <= logStart) {
                continue;
            }
            List<EventEnvelope> removed =
                    broker.truncatePrefix(partition, safeWatermark, batchSizePerPartition);
            if (removed.isEmpty()) {
                continue;
            }
            List<String> removedIds = new ArrayList<>(removed.size());
            for (EventEnvelope e : removed) {
                removedIds.add(e.eventId());
            }
            state.pruneProcessed(removedIds);
            totalRemoved += removed.size();
            log.info("decision=CLEANED partition={} removed={} safeWatermark={} committed={} earliestDeadLetter={} "
                            + "basis='committed-and-past-live-dead-letter'",
                    partition, removed.size(), safeWatermark, committed,
                    earliestDead.map(String::valueOf).orElse("-"));
        }
        return totalRemoved;
    }
}
