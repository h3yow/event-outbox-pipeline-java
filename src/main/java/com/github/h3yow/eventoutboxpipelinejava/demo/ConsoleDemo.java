package com.github.h3yow.eventoutboxpipelinejava.demo;

import com.github.h3yow.eventoutboxpipelinejava.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.consumer.ConsumerGroupState;
import com.github.h3yow.eventoutboxpipelinejava.dlq.DeadLetterRecord;
import com.github.h3yow.eventoutboxpipelinejava.dlq.InMemoryDeadLetterStore;
import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.offset.OffsetOutOfRangeException;
import com.github.h3yow.eventoutboxpipelinejava.pipeline.EventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * 纯 main 方法的本地演示，不启动 Spring / Web 容器。覆盖：
 * 正常事件、瞬时失败重试、死信与重放、聚合级隔离、安全清理与清理后的位点生命周期。
 *
 * <p>运行：mvn -q compile exec:java -Dexec.mainClass=com.github.h3yow.eventoutboxpipelinejava.demo.ConsoleDemo
 */
public final class ConsoleDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsoleDemo.class);

    private ConsoleDemo() {
    }

    public static void main(String[] args) throws Exception {
        demonstrateRetryDeadLetterAndReplay();
        demonstrateAggregateIsolation();
        demonstrateRetentionAndOffsetLifecycle();
    }

    /** 场景一：成功 / 重试成功 / 死信 / 修复后重放，副作用始终一次。 */
    private static void demonstrateRetryDeadLetterAndReplay() throws Exception {
        log.info("======== scenario 1: retry / dead-letter / replay ========");
        InMemoryEventBroker broker = new InMemoryEventBroker(2);
        ConsumerGroupState state = new ConsumerGroupState();
        InMemoryDeadLetterStore dlq = new InMemoryDeadLetterStore();
        Map<String, Integer> failTimes = new ConcurrentHashMap<>(Map.of("evt-flaky", 2));
        Set<String> applied = ConcurrentHashMap.newKeySet();

        RetryPolicy policy = new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(1));
        EventPipeline pipeline = new EventPipeline(broker, state, dlq,
                event -> {
                    Integer remaining = failTimes.get(event.eventId());
                    if (remaining != null && remaining > 0) {
                        failTimes.put(event.eventId(), remaining - 1);
                        throw new IllegalStateException("simulated transient failure: " + event.eventId());
                    }
                    applied.add(event.eventId());
                }, policy, 3, 5L);

        pipeline.start();
        pipeline.enqueue("order-A", "OrderPaid", null);
        pipeline.publish(new EventEnvelope("evt-flaky", "order-A", "OrderUpdated",
                new byte[0], System.currentTimeMillis()));
        pipeline.publish(new EventEnvelope("evt-poison", "order-B", "OrderClosed",
                new byte[0], System.currentTimeMillis()));
        failTimes.put("evt-poison", Integer.MAX_VALUE);

        pipeline.awaitProcessed(10_000);
        log.info("sideEffectsApplied={} dlqSize={}", applied, dlq.size());

        failTimes.remove("evt-poison");
        pipeline.replay("evt-poison");
        pipeline.awaitProcessed(10_000);
        pipeline.seekToEvent("evt-flaky"); // 重复投递：幂等拦截
        pipeline.awaitProcessed(10_000);
        log.info("after replay: sideEffectsApplied={} (each once), dlqSize={}", applied, dlq.size());
        pipeline.stop();
    }

    /** 场景二：同一分区上，慢聚合不阻塞其它聚合。 */
    private static void demonstrateAggregateIsolation() throws Exception {
        log.info("======== scenario 2: aggregate-level isolation (single partition) ========");
        InMemoryEventBroker broker = new InMemoryEventBroker(1);
        ConsumerGroupState state = new ConsumerGroupState();
        InMemoryDeadLetterStore dlq = new InMemoryDeadLetterStore();
        Set<String> handled = ConcurrentHashMap.newKeySet();

        // 单分区下任意两个聚合都共享同一条物理通道，直接取两个即可
        final String slow = "iso-slow";
        final String fast = "iso-fast";
        log.info("slowAgg={} fastAgg={} share partition={}",
                slow, fast, broker.partitionOf(slow));

        CountDownLatch insideSlow = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        EventPipeline pipeline = new EventPipeline(broker, state, dlq,
                event -> {
                    if (event.aggregateKey().equals(slow) && insideSlow.getCount() > 0) {
                        insideSlow.countDown();
                        releaseSlow.await();
                    }
                    handled.add(event.eventId());
                }, RetryPolicy.noRetry(), 2, 2L);

        pipeline.start();
        pipeline.enqueue(slow, "Slow", null);
        insideSlow.await();
        for (int i = 0; i < 5; i++) {
            pipeline.enqueue(fast, "Fast", null);
        }
        Thread.sleep(100);
        log.info("while slow event blocked: neighbourHandledCount={} (isolation works when 5 fast events are already handled)",
                handled.size());
        releaseSlow.countDown();
        pipeline.awaitProcessed(10_000);
        pipeline.stop();
    }

    /** 场景三：安全清理推进 logStart；清理后四类位点拒绝原因一眼可分。 */
    private static void demonstrateRetentionAndOffsetLifecycle() throws Exception {
        log.info("======== scenario 3: retention trimming & offset lifecycle ========");
        InMemoryEventBroker broker = new InMemoryEventBroker(1);
        ConsumerGroupState state = new ConsumerGroupState();
        InMemoryDeadLetterStore dlq = new InMemoryDeadLetterStore();
        EventPipeline pipeline = new EventPipeline(broker, state, dlq,
                event -> {
                    if (event.eventId().equals("evt-doom")) {
                        throw new IllegalStateException("permanent failure");
                    }
                }, RetryPolicy.noRetry(), 2, 2L);

        pipeline.start();
        pipeline.enqueue("agg", "A", null);
        pipeline.publish(new EventEnvelope("evt-doom", "agg", "Bad",
                new byte[0], System.currentTimeMillis()));
        pipeline.enqueue("agg", "C", null);
        pipeline.awaitProcessed(10_000);

        log.info("before compact: logStart=0 logEnd={} dlqSize={}", pipeline.logEndOffset("p0"), dlq.size());
        int removed = pipeline.compactLog(); // 只能清掉 offset 0（offset 1 死信是屏障）
        log.info("compact removed={}, after: logStart={} (dead letter at offset 1 is the barrier)",
                removed, pipeline.logStartOffset("p0"));

        reject(pipeline, 0, "trimmed");
        reject(pipeline, -1, "negative");
        reject(pipeline, 99, "too-high");
        reject(pipeline, 1, "dead-letter");

        pipeline.stop();
    }

    private static void reject(EventPipeline pipeline, long offset, String label) {
        try {
            pipeline.seek("p0", offset);
        } catch (OffsetOutOfRangeException e) {
            log.info("seek offset={} ({}) -> rejected: {}: {}", offset, label, e.getReason(), e.getMessage());
        } catch (RuntimeException e) {
            log.info("seek offset={} ({}) -> rejected: {}: {}", offset, label,
                    e.getClass().getSimpleName(), e.getMessage());
        }
    }
}
