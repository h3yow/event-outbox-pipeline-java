package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.cleanup.EventLogCleaner;
import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.offset.OffsetInvalidException;
import com.github.h3yow.eventoutboxpipelinejava.offset.OffsetOutOfRangeException;
import com.github.h3yow.eventoutboxpipelinejava.pipeline.EventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory.TestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 后台清理边界：
 * - 已成功处理的连续前缀可被物理删除，logStart 前移；
 * - 还没轮到处理 / 正在重试的事件不能被清；
 * - 仍在死信区的事件是硬水位：它和它之后的事件都不删；
 * - 死信被正式重放成功后，水位放开，可以继续清；
 * - 清理与消费/入队并发进行不丢事件，幂等与聚合当前状态在清理后依旧成立。
 */
class LogCleanupTests {

    private EventPipeline pipelineWithCleaner(TestHarness harness, long intervalMs, int batch) {
        EventLogCleaner cleaner = new EventLogCleaner(harness.broker(), harness.state(),
                harness.dlq(), intervalMs, batch);
        return new EventPipeline(harness.broker(), harness.state(), harness.dlq(),
                harness.handler(), harness.policy(), 4, 2L, cleaner);
    }

    @Test
    void succeededPrefixIsTruncatedButPendingEventsRemain() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(2, 2, 1, Duration.ZERO);
        EventPipeline pipeline = pipelineWithCleaner(harness, 60_000L, 100);

        pipeline.start();
        for (int i = 0; i < 10; i++) {
            pipeline.enqueue("agg-a-" + i, "T", null); // 10 个不同聚合
        }
        assertTrue(pipeline.awaitProcessed(5000));

        // 选一个确实有事件的分区验证截断
        String p0 = harness.broker().partitions().stream()
                .filter(p -> harness.broker().logEndOffset(p) > 0)
                .findFirst().orElseThrow();
        long end = harness.broker().logEndOffset(p0);
        int removed = pipeline.cleanupOnce();
        assertTrue(removed > 0, "finished events are removable");

        // 清完后再跑一轮：没有更多可删（已齐平且无死信）
        int second = pipeline.cleanupOnce();
        assertEquals(0, second);

        // p0 起始位点前移到自身末尾，已删位点读不到
        long start = harness.broker().logStartOffset(p0);
        assertEquals(end, start, "p0 fully finished -> truncated up to its end");
        assertEquals(end, harness.broker().logEndOffset(p0));
        if (end > 0) {
            assertTrue(harness.broker().read(p0, 0).isEmpty());
        }

        // 清理后继续入队 + 消费，编号接在旧末尾之后，一切正常
        EventEnvelope after = pipeline.enqueue("agg-after", "T", null);
        String partition = harness.broker().partitionOf("agg-after");
        assertTrue(pipeline.awaitProcessed(5000));
        assertEquals(harness.broker().logEndOffset(partition),
                pipeline.committedOffset(partition));
        assertEquals(1, harness.handler().callCount(after.eventId()));
        pipeline.stop();
    }

    @Test
    void retryingAndPendingEventsAreNeverCleaned() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(1, 2, 5, Duration.ofMinutes(1));
        EventPipeline pipeline = pipelineWithCleaner(harness, 60_000L, 100);

        pipeline.start();
        // 事件 0：持续失败、处于退避重试中（退避 1 分钟，测试期间绝不会推进连续位点）
        EventEnvelope stuck = new EventEnvelope(UUID.randomUUID().toString(),
                "agg-stuck", "Bad", new byte[0], System.currentTimeMillis());
        harness.handler().failFirst(stuck.eventId(), 1_000);
        pipeline.publish(stuck);
        // 事件 1、2：其他聚合——聚合隔离下它们会被并行处理（副作用生效），
        // 但因 offset 0 仍在重试，committedOffset 停在 0，这些已终态事件仍不得被清理。
        EventEnvelope other1 = pipeline.enqueue("agg-p1", "T", null);
        EventEnvelope other2 = pipeline.enqueue("agg-p2", "T", null);

        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline
                && harness.handler().callCount(stuck.eventId()) < 1) {
            Thread.sleep(10);
        }
        assertTrue(harness.handler().callCount(stuck.eventId()) >= 1, "stuck event started retrying");
        // 等待其他聚合并行处理完成
        deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline
                && (harness.handler().callCount(other1.eventId()) < 1
                        || harness.handler().callCount(other2.eventId()) < 1)) {
            Thread.sleep(10);
        }
        assertEquals(1, harness.handler().callCount(other1.eventId()),
                "other aggregate progresses independently of the retrying head");
        assertEquals(1, harness.handler().callCount(other2.eventId()));

        int removed = pipeline.cleanupOnce();
        assertEquals(0, removed,
                "retrying head pins committed offset at 0 -> even finished later events are retained");
        assertEquals(0, harness.broker().logStartOffset("p0"));
        // 重试事件与已终态但未折叠的事件物理上都还在（重试事件稍后仍要按原位点继续重试）
        assertTrue(harness.broker().read("p0", 0).isPresent(), "retrying event retained");
        assertTrue(harness.broker().read("p0", 1).isPresent(), "finished-but-unfolded event retained");
        assertTrue(harness.broker().read("p0", 2).isPresent());
        assertEquals(0, pipeline.committedOffset("p0"));
        pipeline.stop();
    }

    @Test
    void liveDeadLetterIsHardWatermarkUntilSuccessfullyReplayed() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(1, 2, 1, Duration.ZERO);
        EventPipeline pipeline = pipelineWithCleaner(harness, 60_000L, 50);

        pipeline.start();
        EventEnvelope doomed = new EventEnvelope(UUID.randomUUID().toString(),
                "agg-d", "Bad", new byte[0], System.currentTimeMillis());
        harness.handler().failFirst(doomed.eventId(), 100);
        pipeline.publish(doomed); // offset 0 -> DLQ
        EventEnvelope later1 = pipeline.enqueue("agg-l1", "T", null); // offset 1 成功
        EventEnvelope later2 = pipeline.enqueue("agg-l2", "T", null); // offset 2 成功
        assertTrue(pipeline.awaitProcessed(5000));
        assertEquals(3, pipeline.committedOffset("p0"));
        assertEquals(1, harness.dlq().size());

        // 清理：offset 0 是存活死信 -> 硬水位，一条都不能删（即使 1、2 已成功）
        int removed = pipeline.cleanupOnce();
        assertEquals(0, removed, "live dead letter pins truncation watermark at its offset");
        assertEquals(0, harness.broker().logStartOffset("p0"));
        assertTrue(harness.broker().read("p0", 0).isPresent(), "dead-lettered event retained");
        assertTrue(harness.broker().read("p0", 1).isPresent(), "events after DLQ retained too");
        assertTrue(pipeline.isProcessed(later1.eventId()));
        assertTrue(pipeline.isProcessed(later2.eventId()));

        // 修复并重放该死信：移除 DLQ -> 回放 -> 成功，随后清理水位放开
        harness.handler().failFirst(doomed.eventId(), 0);
        pipeline.replay(doomed.eventId());
        assertTrue(pipeline.awaitProcessed(5000));
        assertEquals(0, harness.dlq().size());
        int removedAfter = pipeline.cleanupOnce();
        assertTrue(removedAfter >= 3, "whole prefix removable after DLQ released and replayed");
        assertEquals(3, harness.broker().logStartOffset("p0"));
        // 重放成功的副作用只生效一次
        assertEquals(1, harness.handler().sideEffectCount(doomed.eventId()));
        pipeline.stop();
    }

    @Test
    void backgroundCleanupRunsConcurrentWithConsumingAndPreservesSemantics() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(4, 4, 2, Duration.ofMillis(2));
        // 10ms 间隔、每分区最多 3 条：强制与入队/消费交错发生
        EventPipeline pipeline = pipelineWithCleaner(harness, 10L, 3);

        pipeline.start();
        for (int i = 0; i < 400; i++) {
            EventEnvelope flaky = new EventEnvelope(UUID.randomUUID().toString(),
                    "agg-f-" + (i % 20), "T", new byte[0], System.currentTimeMillis());
            harness.handler().failFirst(flaky.eventId(), 1);
            pipeline.publish(flaky);
            if (i % 7 == 0) {
                pipeline.enqueue("agg-ok-" + (i % 15), "T", null);
            }
            if (i % 50 == 0) {
                Thread.sleep(5); // 让清理/消费持续交错
            }
        }
        assertTrue(pipeline.awaitProcessed(30000), "all events drain despite concurrent truncation");
        pipeline.stop();

        for (String partition : harness.broker().partitions()) {
            assertEquals(harness.broker().logEndOffset(partition),
                    pipeline.committedOffset(partition),
                    "offset consistent on " + partition + " after concurrent cleanup");
            // 起始位点只能单调前移且不越过末尾
            assertTrue(harness.broker().logStartOffset(partition)
                    <= harness.broker().logEndOffset(partition));
        }
        assertEquals(0, harness.dlq().size());
        assertTrue(harness.handler().concurrentReentries().isEmpty());
    }

    @Test
    void aggregateStateAndIdempotencySurviveCleanup() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(1, 1, 1, Duration.ZERO);
        EventPipeline pipeline = pipelineWithCleaner(harness, 60_000L, 100);

        pipeline.start();
        EventEnvelope e1 = pipeline.enqueue("agg-x", "T1", null);
        EventEnvelope e2 = pipeline.enqueue("agg-x", "T2", null);
        EventEnvelope other = pipeline.enqueue("agg-y", "T", null);
        assertTrue(pipeline.awaitProcessed(5000));

        // 清理后物理消息没了，但聚合当前状态（最后成功事件）仍可读
        assertTrue(pipeline.cleanupOnce() >= 3);
        assertTrue(harness.broker().read("p0", 0).isEmpty(),
                "cleaned physical message is gone");
        var stateX = pipeline.currentAggregateState("agg-x").orElseThrow();
        assertEquals(e2.eventId(), stateX.event().eventId(),
                "aggregate current state still reads latest success after cleanup");
        var stateY = pipeline.currentAggregateState("agg-y").orElseThrow();
        assertEquals(other.eventId(), stateY.event().eventId());

        // 幂等标记随物理事件一起裁剪：事件已不存在，无法重放（seek 被截断拒绝）
        OffsetOutOfRangeException truncated = assertThrows(OffsetOutOfRangeException.class,
                () -> pipeline.seek("p0", 0));
        assertSame(OffsetOutOfRangeException.Reason.OFFSET_TRUNCATED, truncated.getReason());
        assertTrue(truncated.getMessage().contains("logStart"));

        // 仍存活的已处理事件（若有）回放不重复生效——这里通过未清理的聚合状态确认副作用次数
        assertEquals(1, harness.handler().sideEffectCount(e1.eventId()));
        assertEquals(1, harness.handler().sideEffectCount(e2.eventId()));
        // 死信位点拒绝原因类型依旧独立可辨
        EventEnvelope doomed = new EventEnvelope(UUID.randomUUID().toString(),
                "agg-z", "Bad", new byte[0], System.currentTimeMillis());
        harness.handler().failFirst(doomed.eventId(), 10);
        pipeline.publish(doomed);
        assertTrue(pipeline.awaitProcessed(5000));
        var loc = harness.broker().locate(doomed.eventId()).orElseThrow();
        assertThrows(OffsetInvalidException.class,
                () -> pipeline.seek(loc.partition(), loc.offset()));
        pipeline.stop();
    }
}
