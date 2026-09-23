package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory.TestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发消费一致性：
 * - 多线程并发入队 + 多工作线程消费，结果无重复、无丢失、各分区位点齐平；
 * - 同一聚合事件严格保序；
 * - 某个分区的慢事件不阻塞其他分区推进。
 */
class ConcurrentConsumptionTests {

    @Test
    void concurrentPublishingAndConsumingIsConsistent() throws Exception {
        int partitions = 8;
        TestHarness harness = PipelineFactory.newHarness(partitions, 8, 2, Duration.ofMillis(5));
        var pipeline = harness.pipeline();

        int publisherThreads = 8;
        int eventsPerThread = 100;
        int total = publisherThreads * eventsPerThread;
        ExecutorService publishers = Executors.newFixedThreadPool(publisherThreads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<EventEnvelope> all = new ConcurrentLinkedQueue<>();

        pipeline.start();
        for (int t = 0; t < publisherThreads; t++) {
            int threadId = t;
            publishers.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < eventsPerThread; i++) {
                    // 同一工作线程使用固定聚合，制造同分区序列
                    String aggregate = "agg-" + (threadId % partitions);
                    all.add(pipeline.enqueue(aggregate, "Concurrent",
                            (threadId + "-" + i).getBytes()));
                }
            });
        }
        start.countDown();
        publishers.shutdown();
        assertTrue(publishers.awaitTermination(10, TimeUnit.SECONDS));

        assertTrue(pipeline.awaitProcessed(15000), "all events drain");
        pipeline.stop();

        // 无丢失、无重复
        long distinct = all.stream().map(EventEnvelope::eventId).distinct().count();
        assertEquals(total, distinct, "all enqueued eventIds are unique");
        long processed = all.stream()
                .filter(e -> harness.handler().callCount(e.eventId()) == 1).count();
        assertEquals(total, processed,
                "every event processed exactly once (no duplicate, no loss)");

        // 每个分区：物理消息数 == 已提交位点 == 成功处理数（本场景全部成功）
        for (String partition : harness.broker().partitions()) {
            long end = harness.broker().logEndOffset(partition);
            assertEquals(end, pipeline.committedOffset(partition),
                    "offset consistent on " + partition);
        }
        assertEquals(0, harness.dlq().size());
        assertTrue(harness.handler().concurrentReentries().isEmpty(),
                "no event handled by two workers concurrently: "
                        + harness.handler().concurrentReentries());
    }

    @Test
    void slowEventOnOnePartitionDoesNotBlockOthers() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(4, 4, 1, Duration.ZERO);
        var pipeline = harness.pipeline();

        // 找到两个映射到不同分区的聚合
        String slowAgg = null;
        String fastAgg = null;
        for (int i = 0; i < 1000 && (slowAgg == null || fastAgg == null); i++) {
            String candidate = "candidate-" + i;
            if (slowAgg == null) {
                slowAgg = candidate;
            } else if (!harness.broker().partitionOf(candidate)
                    .equals(harness.broker().partitionOf(slowAgg))) {
                fastAgg = candidate;
            }
        }
        assertTrue(slowAgg != null && fastAgg != null);

        // 自定义一个会阻塞一次的处理器：慢事件等待放行；其余直接通过
        final String blockerAgg = slowAgg;
        CountDownLatch insideSlow = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        var baseHandler = harness.handler();
        var pipelineOverride = new com.github.h3yow.eventoutboxpipelinejava.pipeline.EventPipeline(
                harness.broker(), harness.state(), harness.dlq(),
                event -> {
                    if (event.aggregateKey().equals(blockerAgg) && insideSlow.getCount() > 0) {
                        insideSlow.countDown();
                        assertTrue(releaseSlow.await(10, TimeUnit.SECONDS),
                                "slow event must eventually be released");
                    }
                    baseHandler.handle(event);
                },
                harness.policy(), 4, 2L);

        pipelineOverride.start();
        EventEnvelope slow = pipelineOverride.enqueue(slowAgg, "Slow", null);
        insideSlow.await(); // 确认慢事件已在处理中
        // 慢事件阻塞期间，其他分区的事件必须照常推进
        List<EventEnvelope> fast = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            fast.add(pipelineOverride.enqueue(fastAgg, "Fast", null));
        }
        // 给快分区调度时间后断言其已全部处理，而慢分区仍停在 0
        Thread.sleep(100);
        String fastPartition = harness.broker().partitionOf(fastAgg);
        String slowPartition = harness.broker().partitionOf(slowAgg);
        assertEquals(10, pipelineOverride.committedOffset(fastPartition),
                "unrelated partitions are not blocked by the slow event");
        assertEquals(0, pipelineOverride.committedOffset(slowPartition),
                "slow partition itself stays ordered behind the in-flight event");

        releaseSlow.countDown();
        assertTrue(pipelineOverride.awaitProcessed(5000));
        pipelineOverride.stop();

        assertEquals(1, harness.handler().callCount(slow.eventId()));
        for (EventEnvelope e : fast) {
            assertEquals(1, harness.handler().callCount(e.eventId()));
        }
        assertEquals(10, pipelineOverride.committedOffset(fastPartition));
        assertEquals(1, pipelineOverride.committedOffset(slowPartition));
    }
}
