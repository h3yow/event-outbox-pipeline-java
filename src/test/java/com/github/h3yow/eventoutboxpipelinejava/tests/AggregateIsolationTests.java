package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.consumer.ConsumerGroupState;
import com.github.h3yow.eventoutboxpipelinejava.dlq.InMemoryDeadLetterStore;
import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.pipeline.EventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.retry.RetryPolicy;
import com.github.h3yow.eventoutboxpipelinejava.support.RecordingHandler;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 聚合级隔离（即使多个互不相干的聚合挤在同一条物理分区上）：
 * 1. 一个聚合的慢事件（处理中阻塞或退避睡眠）不拖住同分区其它聚合的推进；
 * 2. 某个聚合大量积压不会饿死同分区其它聚合；
 * 3. 同一聚合内部仍然严格保序、串行、不并发；
 * 4. 大量聚合共享少量通道时，同聚合保序 + 不重不丢。
 */
class AggregateIsolationTests {

    /** 找出两个稳定哈希到同一分区的不同聚合。 */
    private static String[] samePartitionPair(InMemoryEventBroker broker) {
        Map<String, String> byPartition = new HashMap<>();
        for (int i = 0; i < 100_000; i++) {
            String candidate = "agg-" + UUID.randomUUID();
            String p = broker.partitionOf(candidate);
            String first = byPartition.get(p);
            if (first != null) {
                return new String[]{first, candidate};
            }
            byPartition.put(p, candidate);
        }
        throw new IllegalStateException("cannot find two aggregates on the same partition");
    }

    @Test
    void slowEventDoesNotBlockOtherAggregatesOnSamePartition() throws Exception {
        // 单分区、2 个工作线程：慢聚合的一个事件在处理器里阻塞，占用一个线程；
        // 同分区快聚合的事件必须在慢事件仍阻塞期间被另一个线程处理完，
        // 而慢聚合同聚合的第二条必须严格排在慢事件之后（保序、不插队）。
        InMemoryEventBroker broker = new InMemoryEventBroker(1);
        ConsumerGroupState state = new ConsumerGroupState();
        InMemoryDeadLetterStore dlq = new InMemoryDeadLetterStore();
        RecordingHandler base = new RecordingHandler();
        var pair = samePartitionPair(broker);
        String slowAgg = pair[0];
        String fastAgg = pair[1];
        assertEquals(broker.partitionOf(slowAgg), broker.partitionOf(fastAgg));

        CountDownLatch insideSlow = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        EventPipeline pipeline = new EventPipeline(broker, state, dlq, event -> {
            if (event.aggregateKey().equals(slowAgg) && insideSlow.getCount() > 0) {
                insideSlow.countDown();
                assertTrue(releaseSlow.await(10, TimeUnit.SECONDS), "slow event released");
            }
            base.handle(event);
        }, new RetryPolicy(1, Duration.ZERO, Duration.ZERO), 2, 2L);

        pipeline.start();
        EventEnvelope slow = pipeline.enqueue(slowAgg, "Slow", null);
        assertTrue(insideSlow.await(10, TimeUnit.SECONDS), "slow event entered handler");

        List<EventEnvelope> fast = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            fast.add(pipeline.enqueue(fastAgg, "Fast", null));
        }
        EventEnvelope slow2 = pipeline.enqueue(slowAgg, "Slow2", null);

        // 关键隔离断言：慢事件仍在阻塞，同分区快聚合 20 条必须全部处理完
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline
                && fast.stream().anyMatch(e -> base.callCount(e.eventId()) == 0)) {
            Thread.sleep(5);
        }
        for (EventEnvelope e : fast) {
            assertEquals(1, base.callCount(e.eventId()),
                    "fast aggregate on SAME partition progresses while slow event is blocked");
        }
        // 慢聚合第二条仍未处理（同聚合严格串行，排在在途慢事件之后）
        assertEquals(0, base.callCount(slow2.eventId()),
                "same aggregate's later event must wait behind the in-flight slow event");
        // insideSlow 已归零说明慢事件确实进入并停留在业务处理器内（release 尚未放行）
        assertEquals(0, insideSlow.getCount(),
                "slow event is in-flight inside the handler");

        releaseSlow.countDown();
        assertTrue(pipeline.awaitProcessed(10_000), "everything drains after release");
        pipeline.stop();

        assertEquals(1, base.callCount(slow.eventId()));
        assertEquals(1, base.callCount(slow2.eventId()));
        List<String> order = base.handledOrder();
        assertTrue(order.indexOf(slow.eventId()) < order.indexOf(slow2.eventId()),
                "same aggregate stays strictly ordered");
        assertTrue(base.concurrentReentries().isEmpty());
    }

    @Test
    void backloggedAggregateDoesNotStarveOthersOnSamePartition() throws Exception {
        // 2 个工作线程、单分区：backlogAgg 大量事件持续占满，lightAgg 的少量事件必须在
        // 有界时间内处理完（轮转让渡保证公平），而不是等到 backlogAgg 全部清空。
        InMemoryEventBroker broker = new InMemoryEventBroker(1);
        ConsumerGroupState state = new ConsumerGroupState();
        InMemoryDeadLetterStore dlq = new InMemoryDeadLetterStore();
        RecordingHandler handler = new RecordingHandler();
        var pair = samePartitionPair(broker);
        String backlogAgg = pair[0];
        String lightAgg = pair[1];
        RetryPolicy policy = new RetryPolicy(1, Duration.ZERO, Duration.ZERO);
        EventPipeline pipeline = new EventPipeline(broker, state, dlq, handler, policy, 2, 2L);

        pipeline.start();
        List<EventEnvelope> backlog = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            backlog.add(pipeline.enqueue(backlogAgg, "Backlog", null));
        }
        List<EventEnvelope> light = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            light.add(pipeline.enqueue(lightAgg, "Light", null));
        }

        // 公平轮转：轻量聚合不必等 2000 条积压清空（宽松 10s 上限，实际应在毫秒级完成）
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline
                && light.stream().anyMatch(e -> handler.callCount(e.eventId()) == 0)) {
            Thread.sleep(5);
        }
        for (EventEnvelope e : light) {
            assertEquals(1, handler.callCount(e.eventId()),
                    "light aggregate must not be starved by a backlogged neighbour");
        }
        // 此时 backlogAgg 不允许已经全部清空后才轮到 lightAgg（否则谈不上公平隔离）
        long backlogDone = backlog.stream().filter(e -> handler.callCount(e.eventId()) > 0).count();
        assertTrue(backlogDone < backlog.size(),
                "light aggregate got scheduled while backlog still draining: done=" + backlogDone);

        assertTrue(pipeline.awaitProcessed(30_000));
        pipeline.stop();

        // 最终不重不丢
        for (EventEnvelope e : backlog) {
            assertEquals(1, handler.callCount(e.eventId()));
        }
        assertTrue(handler.concurrentReentries().isEmpty(),
                "no concurrent re-entry within an aggregate: " + handler.concurrentReentries());
    }

    @Test
    void sameAggregateStaysOrderedUnderManyAggregatesAndFewWorkers() throws Exception {
        // 1 个物理分区承载 200 个聚合，仅 3 个工作线程：同聚合保序、不并发、不丢不重
        InMemoryEventBroker broker = new InMemoryEventBroker(1);
        ConsumerGroupState state = new ConsumerGroupState();
        InMemoryDeadLetterStore dlq = new InMemoryDeadLetterStore();
        RecordingHandler handler = new RecordingHandler();
        EventPipeline pipeline = new EventPipeline(broker, state, dlq, handler,
                new RetryPolicy(2, Duration.ofMillis(1), Duration.ofMillis(5)), 3, 2L);

        int aggregates = 200;
        int perAggregate = 10;
        List<EventEnvelope> all = new ArrayList<>();
        pipeline.start();
        for (int a = 0; a < aggregates; a++) {
            String agg = "many-agg-" + a;
            for (int i = 0; i < perAggregate; i++) {
                all.add(pipeline.enqueue(agg, "T", (a + "-" + i).getBytes()));
            }
        }
        assertTrue(pipeline.awaitProcessed(30_000), "all aggregates drain");
        pipeline.stop();

        assertEquals(aggregates * perAggregate, all.size());
        for (EventEnvelope e : all) {
            assertEquals(1, handler.callCount(e.eventId()),
                    "exactly-once for " + e.eventId());
            assertEquals(1, handler.sideEffectCount(e.eventId()));
        }
        // 校验每个聚合在全局处理序列中的相对顺序与入队顺序一致
        List<String> handled = handler.handledOrder();
        for (int a = 0; a < aggregates; a++) {
            String agg = "many-agg-" + a;
            List<EventEnvelope> enqueued = all.stream()
                    .filter(e -> e.aggregateKey().equals(agg)).toList();
            int cursor = 0;
            for (String eventId : handled) {
                if (cursor < enqueued.size() && eventId.equals(enqueued.get(cursor).eventId())) {
                    cursor++;
                }
            }
            assertEquals(enqueued.size(), cursor,
                    "aggregate " + agg + " consumed in enqueue order, no loss");
        }
        assertTrue(handler.concurrentReentries().isEmpty());
    }
}
