package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory.TestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 后台安全清理（保留策略）：
 * - 只清理已彻底结束的连续前缀（成功处理 / 已进死信区）；
 * - 未轮到处理、正在重试、死信区中的事件以及死信屏障之后的事件都不能被清掉；
 * - 清理与入队/消费并发进行时不丢事件、不阻断推进，重复投递/回放后副作用仍只生效一次；
 * - 后台周期性清理可自动回收内存。
 */
class LogRetentionTests {

    @Test
    void trimsOnlyContiguousTerminalPrefixAndBumpsLogStart() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(1, 2, 1, Duration.ZERO);
        var pipeline = harness.pipeline();
        pipeline.start();
        for (int i = 0; i < 6; i++) {
            pipeline.enqueue("agg-trim", "T", null);
        }
        assertTrue(pipeline.awaitProcessed(5000));

        assertEquals(0, pipeline.logStartOffset("p0"));
        int removed = pipeline.compactLog();
        assertEquals(6, removed);
        assertEquals(6, pipeline.logStartOffset("p0"));
        assertEquals(6, pipeline.logEndOffset("p0"));
        assertEquals(0, pipeline.retainedSize("p0"), "physical storage reclaimed");

        // 截断区间内的位点读不到物理事件
        assertFalse(harness.broker().read("p0", 0).isPresent());
        assertFalse(harness.broker().read("p0", 5).isPresent());
        // 但按 eventId 的定位仍可用（索引保留）
        pipeline.stop();
    }

    @Test
    void deadLetterIsARetentionBarrierNothingAfterItGetsTrimmed() throws Exception {
        // 序列：ok(0) doomed(1 死信) ok(2..) —— 安全点只能到 1，死信及其后全部保留
        TestHarness harness = PipelineFactory.newHarness(1, 2, 1, Duration.ZERO);
        var pipeline = harness.pipeline();
        pipeline.start();
        EventEnvelope ok0 = pipeline.enqueue("agg-d", "Ok", null);
        EventEnvelope doomed = new EventEnvelope(UUID.randomUUID().toString(),
                "agg-d", "Bad", new byte[0], System.currentTimeMillis());
        harness.handler().failFirst(doomed.eventId(), 10);
        pipeline.publish(doomed);
        EventEnvelope ok2 = pipeline.enqueue("agg-d", "Ok", null);
        EventEnvelope ok3 = pipeline.enqueue("agg-d", "Ok", null);
        assertTrue(pipeline.awaitProcessed(5000));

        int removed = pipeline.compactLog();
        assertEquals(1, removed, "only the single contiguous successful prefix event is reclaimed");
        assertEquals(1, pipeline.logStartOffset("p0"));
        assertEquals(4, pipeline.logEndOffset("p0"));
        // 死信事件与其后事件物理保留、可读
        assertEquals(doomed.eventId(), harness.broker().read("p0", 1).orElseThrow().eventId());
        assertEquals(ok3.eventId(), harness.broker().read("p0", 3).orElseThrow().eventId());
        assertEquals(1, harness.dlq().size());
        assertTrue(harness.broker().read("p0", 0).isEmpty(), "trimmed prefix physically gone");

        // 再次清理：没有新增安全空间，幂等
        assertEquals(0, pipeline.compactLog());
        assertEquals(1, pipeline.logStartOffset("p0"));

        // 死信重放成功后，整段剩余都可清理
        harness.handler().failFirst(doomed.eventId(), 0);
        pipeline.replay(doomed.eventId());
        assertTrue(pipeline.awaitProcessed(5000));
        assertEquals(0, harness.dlq().size());
        assertEquals(3, pipeline.compactLog());
        assertEquals(4, pipeline.logStartOffset("p0"));
        assertEquals(1, harness.handler().sideEffectCount(doomed.eventId()),
                "replayed dead letter side effect applies once");
        pipeline.stop();
    }

    @Test
    void retryingAndUnconsumedEventsAreNeverTrimmed() throws Exception {
        // 序列：flaky(0，前 5 次失败，退避中) ok1 ok2
        // 在 flaky 仍重试期间清理：水位线=0 -> 什么都不能清，即使它后面的事件先处理完
        TestHarness harness = PipelineFactory.newHarness(1, 4, 6, Duration.ofMillis(50));
        var pipeline = harness.pipeline();
        pipeline.start();
        EventEnvelope flaky = new EventEnvelope(UUID.randomUUID().toString(),
                "agg-r", "Flaky", new byte[0], System.currentTimeMillis());
        harness.handler().failFirst(flaky.eventId(), 5);
        pipeline.publish(flaky);
        // 另一个聚合（同分区）事件会先完成，制造乱序终态
        EventEnvelope later1 = pipeline.enqueue("agg-r-other", "Ok", null);
        EventEnvelope later2 = pipeline.enqueue("agg-r-other", "Ok", null);

        // 等 later 事件处理完，而 flaky 仍在重试
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline
                && (harness.handler().callCount(later1.eventId()) == 0
                    || harness.handler().callCount(later2.eventId()) == 0)) {
            Thread.sleep(5);
        }
        assertEquals(1, harness.handler().callCount(later1.eventId()));
        assertEquals(1, harness.handler().callCount(later2.eventId()));
        assertTrue(harness.handler().callCount(flaky.eventId()) >= 1
                && harness.handler().callCount(flaky.eventId()) <= 5);

        assertEquals(0, pipeline.compactLog(),
                "hole at offset 0 (retrying) blocks any trimming");
        assertEquals(0, pipeline.logStartOffset("p0"));
        for (int offset = 0; offset < 3; offset++) {
            assertTrue(harness.broker().read("p0", offset).isPresent(),
                    "nothing physically removed while an event is retrying: " + offset);
        }

        assertTrue(pipeline.awaitProcessed(20_000));
        pipeline.stop();
        // 全部终态后才允许清理整段
        assertEquals(3, pipeline.compactLog());
        assertEquals(3, pipeline.logStartOffset("p0"));
        assertEquals(1, harness.handler().sideEffectCount(flaky.eventId()));
    }

    @Test
    void trimmingDoesNotBlockOrLoseConcurrentPublishingAndConsumption() throws Exception {
        // 后台清理周期很短，与持续入队/消费并发：最终不丢、不重、副作用一次
        TestHarness harness = PipelineFactory.newHarness(4, 4, 2,
                Duration.ofMillis(2), 20L);
        var pipeline = harness.pipeline();
        pipeline.start();

        int total = 3000;
        List<EventEnvelope> all = new java.util.ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            all.add(pipeline.enqueue("agg-stream-" + (i % 50), "T", null));
            if (i % 37 == 0) {
                pipeline.compactLog(); // 同时穿插手动清理
            }
        }
        assertTrue(pipeline.awaitProcessed(60_000), "concurrent stream drains");
        pipeline.stop();

        for (EventEnvelope e : all) {
            assertEquals(1, harness.handler().callCount(e.eventId()),
                    "no loss/duplication under concurrent trimming: " + e.eventId());
            assertEquals(1, harness.handler().sideEffectCount(e.eventId()));
        }
        // 全部终态后最终一轮清理可以把所有分区回收干净
        pipeline.compactLog(); // 调用本身不抛异常
        for (String p : harness.broker().partitions()) {
            assertEquals(pipeline.logEndOffset(p), pipeline.logStartOffset(p),
                    "everything reclaimed on " + p);
            assertEquals(0, pipeline.retainedSize(p));
        }
        assertTrue(harness.handler().concurrentReentries().isEmpty());
    }

    @Test
    void redeliveryAfterTrimmingStillAppliesSideEffectOnce() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(1, 2, 1, Duration.ZERO);
        var pipeline = harness.pipeline();
        pipeline.start();
        EventEnvelope e = pipeline.enqueue("agg-idem", "T", null);
        assertTrue(pipeline.awaitProcessed(5000));
        pipeline.compactLog();
        assertEquals(1, pipeline.logStartOffset("p0"));
        // 事件物理已删除，但幂等标记仍在：无法 seek 到截断位点（拒绝原因可区分，见另一个测试类）；
        // 相同 eventId 重复入队仍折叠为同一位置，不产生新消息
        var loc = pipeline.publish(new EventEnvelope(e.eventId(), "agg-idem", "T",
                new byte[0], System.currentTimeMillis()));
        assertEquals(harness.broker().locate(e.eventId()).orElseThrow(), loc);
        assertEquals(1, harness.handler().callCount(e.eventId()));
        assertEquals(1, harness.handler().sideEffectCount(e.eventId()));
        pipeline.stop();
    }
}
