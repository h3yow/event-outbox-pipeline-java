package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.pipeline.EventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory.TestHarness;
import com.github.h3yow.eventoutboxpipelinejava.support.RecordingHandler;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 聚合级隔离（同一物理分区上挤着大量互不相干聚合时）：
 * - 一个聚合的慢事件只占住该聚合的串行槽位，其他聚合照常推进；
 * - 调度按“最久未派发优先”，单一重负载/积压聚合不会饿死其他聚合
 *   （只有 1 个工作线程时同样成立——公平性不依赖并行度）；
 * - 同一聚合内部依旧严格保序、串行，绝不并发重入。
 */
class AggregateIsolationTests {

    /** 在共享 harness 的组件上套一个可注入阻塞行为的管道。 */
    private EventPipeline blockingPipeline(TestHarness harness, int workers,
                                           String slowAgg, CountDownLatch insideSlow,
                                           CountDownLatch releaseSlow, RecordingHandler base) {
        return new EventPipeline(harness.broker(), harness.state(), harness.dlq(),
                event -> {
                    if (event.aggregateKey().equals(slowAgg) && insideSlow.getCount() > 0) {
                        insideSlow.countDown();
                        assertTrue(releaseSlow.await(10, TimeUnit.SECONDS),
                                "slow event must eventually be released");
                    }
                    base.handle(event);
                },
                harness.policy(), workers, 2L);
    }

    @Test
    void slowEventOfOneAggregateDoesNotBlockOtherAggregatesOnSamePartition() throws Exception {
        // 单分区：所有聚合挤在同一条通道；4 个工作线程
        TestHarness harness = PipelineFactory.newHarness(1, 4, 1, Duration.ZERO);
        String slowAgg = "agg-slow";
        List<String> fastAggs = List.of("agg-fast-1", "agg-fast-2", "agg-fast-3", "agg-fast-4");

        CountDownLatch insideSlow = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        EventPipeline pipeline = blockingPipeline(harness, 4, slowAgg, insideSlow, releaseSlow,
                harness.handler());

        pipeline.start();
        EventEnvelope slow = pipeline.enqueue(slowAgg, "Slow", null);
        assertTrue(insideSlow.await(5, TimeUnit.SECONDS), "slow event entered handler");

        // 慢事件卡住期间：同聚合后续事件排队，其他聚合（同一分区）必须全部推进完
        List<EventEnvelope> queuedBehindSlow = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            queuedBehindSlow.add(pipeline.enqueue(slowAgg, "SlowQueued", null));
        }
        List<EventEnvelope> fastEvents = new ArrayList<>();
        for (String fastAgg : fastAggs) {
            for (int i = 0; i < 10; i++) {
                fastEvents.add(pipeline.enqueue(fastAgg, "Fast", null));
            }
        }

        // 给足调度时间后断言：快聚合全部处理，慢聚合一条都没越过在途事件。
        // 注意慢事件在途期间该分区 committedOffset 停在 0（位点按连续前缀折叠），
        // 但快聚合事件已并行终态，体现在副作用计数上。
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline
                && harness.handler().sideEffects().size() < fastEvents.size()) {
            Thread.sleep(10);
        }
        for (EventEnvelope e : fastEvents) {
            assertEquals(1, harness.handler().sideEffectCount(e.eventId()),
                    "unrelated aggregate on the SAME partition must not be blocked: " + e.eventId());
        }
        // 慢事件其后同聚合事件一个都没开始
        for (EventEnvelope e : queuedBehindSlow) {
            assertEquals(0, harness.handler().callCount(e.eventId()),
                    "same-aggregate events stay strictly behind the in-flight slow event");
        }
        // 慢聚合当前没有完成任何副作用
        assertEquals(0, harness.handler().sideEffectCount(slow.eventId()));
        assertEquals(0, harness.pipeline().committedOffset("p0"),
                "committed offset cannot fold past the in-flight head offset");

        releaseSlow.countDown();
        assertTrue(pipeline.awaitProcessed(5000), "everything drains after release");
        pipeline.stop();

        for (EventEnvelope e : fastEvents) {
            assertEquals(1, harness.handler().callCount(e.eventId()));
        }
        assertEquals(1, harness.handler().callCount(slow.eventId()));
        for (EventEnvelope e : queuedBehindSlow) {
            assertEquals(1, harness.handler().callCount(e.eventId()),
                    "queued same-aggregate events handled exactly once after release");
        }
        assertTrue(harness.handler().concurrentReentries().isEmpty(),
                "no same-aggregate concurrent processing: " + harness.handler().concurrentReentries());
    }

    @Test
    void singleHeavyAggregateCannotStarveOthersWithSingleWorker() throws Exception {
        // 公平性的极端验证：1 个分区 + 1 个工作线程。重负载聚合事件每次处理很慢，
        // 其它聚合的事件必须穿插被服务（最久未派发优先），而不是等重负载聚合全部跑完。
        TestHarness harness = PipelineFactory.newHarness(1, 1, 1, Duration.ZERO);
        RecordingHandler base = harness.handler();
        String heavyAgg = "agg-heavy";
        List<String> lightAggs = List.of("agg-light-1", "agg-light-2", "agg-light-3");

        EventPipeline pipeline = new EventPipeline(
                harness.broker(), harness.state(), harness.dlq(),
                event -> {
                    if (event.aggregateKey().equals(heavyAgg)) {
                        Thread.sleep(40); // 重负载：每条都慢
                    }
                    base.handle(event);
                },
                harness.policy(), 1, 2L);

        pipeline.start();
        for (int i = 0; i < 100; i++) {
            pipeline.enqueue(heavyAgg, "Heavy", null);
        }
        List<EventEnvelope> lightEvents = new ArrayList<>();
        for (String lightAgg : lightAggs) {
            for (int i = 0; i < 5; i++) {
                lightEvents.add(pipeline.enqueue(lightAgg, "Light", null));
            }
        }

        // 100 条重负载串行需要 ~4s；若被饿死，轻聚合在这之前一条都动不了。
        // 公平调度下 15 条轻聚合应在远小于 4s 的时间内穿插完成（给 2.5s 上限）。
        long deadline = System.currentTimeMillis() + 2500;
        while (System.currentTimeMillis() < deadline
                && countSideEffects(base, lightEvents) < lightEvents.size()) {
            Thread.sleep(10);
        }
        assertEquals(lightEvents.size(), countSideEffects(base, lightEvents),
                "light aggregates are served interleaved with the single heavy aggregate (no starvation)");

        assertTrue(pipeline.awaitProcessed(15000), "heavy backlog eventually drains");
        pipeline.stop();

        for (EventEnvelope e : lightEvents) {
            assertEquals(1, base.callCount(e.eventId()));
        }
        // 重负载聚合 100 条同样恰好各一次，且任何聚合都没有并发重入（同聚合串行）
        assertTrue(base.concurrentReentries().isEmpty(),
                "serial per aggregate even with single worker: " + base.concurrentReentries());
    }

    @Test
    void eventsOfSameAggregateRemainStrictlyOrdered() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(2, 4, 1, Duration.ZERO);
        EventPipeline pipeline = harness.pipeline();
        String aggregate = "ordered-agg";
        List<String> enqueued = new ArrayList<>();

        pipeline.start();
        for (int i = 0; i < 30; i++) {
            enqueued.add(pipeline.enqueue(aggregate, "T", ("v" + i).getBytes()).eventId());
        }
        assertTrue(pipeline.awaitProcessed(5000));
        pipeline.stop();

        List<String> handledForAgg = harness.handler().handledOrder().stream()
                .filter(enqueued::contains).toList();
        assertEquals(enqueued, handledForAgg,
                "same aggregate stays strictly in enqueue order despite per-aggregate parallel scheduling");
        assertTrue(harness.handler().concurrentReentries().isEmpty());
    }

    private static long countSideEffects(RecordingHandler handler, List<EventEnvelope> events) {
        return events.stream().filter(e -> handler.sideEffectCount(e.eventId()) == 1).count();
    }
}
