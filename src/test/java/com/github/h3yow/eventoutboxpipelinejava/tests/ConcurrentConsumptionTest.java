package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEvent;
import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.outbox.handler.RecordingEventHandler;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发消费一致性场景：
 * <ul>
 *   <li>多生产者线程并发入队、多工作线程并行消费后，无重复/丢失的副作用；</li>
 *   <li>同聚合位点连续推进到末尾，事件标识全局恰好处理一次；</li>
 *   <li>某聚合的慢事件不得阻塞无关聚合的推进（并行度可观测）。</li>
 * </ul>
 */
class ConcurrentConsumptionTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentConsumptionTest.class);

    private static final class TrackingHandler extends RecordingEventHandler {
        /** aggregate -> 按观察顺序记录的 eventId，用于并发后校验同聚合顺序与唯一。 */
        private final ConcurrentHashMap<String, List<String>> seenByAggregate = new ConcurrentHashMap<>();
        private final Set<String> currentlyRunning = ConcurrentHashMap.newKeySet();
        private final AtomicInteger maxParallel = new AtomicInteger();
        private final AtomicInteger parallelSamples = new AtomicInteger();
        private final AtomicInteger running = new AtomicInteger();
        /** 需要变慢的事件。 */
        private final Set<String> slowEventIds;
        private final long slowMillis;

        TrackingHandler(Set<String> slowEventIds, long slowMillis) {
            this.slowEventIds = slowEventIds;
            this.slowMillis = slowMillis;
        }

        @Override
        public void handle(OutboxEvent event) {
            List<String> list = seenByAggregate.computeIfAbsent(
                    event.aggregateId(), k -> Collections.synchronizedList(new ArrayList<>()));
            list.add(event.eventId());

            int active = running.incrementAndGet();
            maxParallel.accumulateAndGet(active, Math::max);
            parallelSamples.incrementAndGet();
            currentlyRunning.add(event.eventId());
            try {
                if (slowEventIds.contains(event.eventId())) {
                    try {
                        Thread.sleep(slowMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            } finally {
                currentlyRunning.remove(event.eventId());
                running.decrementAndGet();
            }
            super.handle(event);
        }
    }

    @Test
    void concurrentProducersAndWorkersStayConsistent() throws Exception {
        int aggregates = 12;
        int perAggregate = 80;
        int producers = 6;
        Set<String> slowIds = new HashSet<>();
        slowIds.add("agg-0-0");   // 0 号聚合首个事件变慢
        slowIds.add("agg-1-10");
        TrackingHandler handler = new TrackingHandler(slowIds, 400L);

        try (OutboxEventPipeline pipeline = Pipelines.create(handler, 1, 0L, 8)) {
            pipeline.start();

            ExecutorService producersPool = Executors.newFixedThreadPool(producers);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(producers);
            for (int p = 0; p < producers; p++) {
                final int producerId = p;
                producersPool.submit(() -> {
                    try {
                        start.await();
                        for (int agg = producerId; agg < aggregates; agg += producers) {
                            for (int i = 0; i < perAggregate; i++) {
                                pipeline.enqueue("agg-" + agg, "agg-" + agg + "-" + i, "T", "p");
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            long t0 = System.currentTimeMillis();
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
            producersPool.shutdown();

            pipeline.awaitQuiescence(15000);
            long elapsed = System.currentTimeMillis() - t0;

            int total = aggregates * perAggregate;
            log.info("[concurrency] total events={}, successCount={}, invocationCount={}, elapsedMs={}",
                    total, handler.successCount(), handler.invocationCount(), elapsed);
            assertEquals(total, handler.successCount(), "不得丢失或重复生效副作用");
            assertEquals(total, handler.invocationCount(), "全部成功时调用次数应等于事件数（无重复处理）");
            assertEquals(total, new HashSet<>(handler.processedEventIds()).size(),
                    "eventId 必须全局唯一处理");

            for (int agg = 0; agg < aggregates; agg++) {
                String key = "agg-" + agg;
                long offset = pipeline.offsets().committed("test-group", key, 0L);
                assertEquals(perAggregate, offset, "聚合 " + key + " 位点必须连续推进到末尾");
                List<String> seen = handler.seenByAggregate.get(key);
                assertEquals(perAggregate, seen.size());
                for (int i = 0; i < perAggregate; i++) {
                    assertEquals(key + "-" + i, seen.get(i),
                            "同聚合事件必须按稳定顺序消费: " + key);
                }
            }
            log.info("[concurrency] offsets all reached {} for {} aggregates, ordering verified",
                    perAggregate, aggregates);
        }
    }

    @Test
    void slowEventOnOneAggregateDoesNotBlockOthers() {
        Set<String> slowIds = new HashSet<>();
        slowIds.add("slow-agg-0");
        TrackingHandler handler = new TrackingHandler(slowIds, 500L);
        try (OutboxEventPipeline pipeline = Pipelines.create(handler, 1, 0L, 4)) {
            pipeline.start();
            // 慢事件先入队占据 agg-slow；随后给其它聚合大量事件
            pipeline.enqueue("agg-slow", "slow-agg-0", "T", "p");
            for (int i = 0; i < 20; i++) {
                pipeline.enqueue("agg-fast", "fast-" + i, "T", "p");
            }
            long deadline = System.currentTimeMillis() + 300;
            // 300ms（小于慢事件 500ms）内，无关聚合应已全部处理完成
            while (System.currentTimeMillis() < deadline
                    && handler.seenByAggregate.getOrDefault("agg-fast", List.of()).size() < 20) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            int fastDone = handler.seenByAggregate.getOrDefault("agg-fast", List.of()).size();
            int slowDone = handler.processedEventIds().contains("slow-agg-0") ? 1 : 0;
            log.info("[concurrency] while slow event still in-flight (<500ms): fastAggregateDone={}/20, slowDone={} (expect 20,0)",
                    fastDone, slowDone);
            assertEquals(20, fastDone, "慢事件不得阻塞无关聚合的推进");
            assertEquals(0, slowDone, "慢事件此时应仍在处理中");

            pipeline.awaitQuiescence(5000);
            assertEquals(21, handler.successCount());
        }
    }
}
