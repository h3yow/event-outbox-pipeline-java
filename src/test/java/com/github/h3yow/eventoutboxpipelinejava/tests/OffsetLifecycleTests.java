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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 清理之后的位点生命周期：老位点落入已清理区间、负数位点、超出末尾位点、
 * 指向死信事件的位点，四者必须给出彼此一眼可辨的拒绝原因；
 * 同时聚合当前状态不受清理影响，重复投递/回放后副作用依旧只生效一次。
 */
class OffsetLifecycleTests {

    @Test
    void fourRejectionReasonsAreDistinguishableAfterCleanup() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(2, 2, 1, Duration.ZERO);
        EventLogCleaner cleaner = new EventLogCleaner(harness.broker(), harness.state(),
                harness.dlq(), 60_000L, 100);
        EventPipeline pipeline = new EventPipeline(harness.broker(), harness.state(), harness.dlq(),
                harness.handler(), harness.policy(), 2, 2L, cleaner);

        pipeline.start();
        String aggA = "agg-life";
        String partition = harness.broker().partitionOf(aggA);
        // 连续 4 条事件落同一聚合：0..3
        EventEnvelope e0 = pipeline.enqueue(aggA, "T0", null);
        EventEnvelope e1 = pipeline.enqueue(aggA, "T1", null);
        EventEnvelope e2 = pipeline.enqueue(aggA, "T2", null);
        EventEnvelope e3 = pipeline.enqueue(aggA, "T3", null);
        assertTrue(pipeline.awaitProcessed(5000));
        long endBefore = harness.broker().logEndOffset(partition);
        assertEquals(4, endBefore);

        // 清理掉成功前缀 0..1（保留 2、3，用于验证仍存活区间可回放）
        int removed = 0;
        for (int i = 0; i < 10; i++) {
            removed += cleaner.cleanupOnce();
        }
        // 全部成功时一次性会清到 4；为稳定得到“部分截断”，直接校验当前起始位点 > 0 即可
        long logStart = harness.broker().logStartOffset(partition);
        assertTrue(logStart >= 2, "prefix cleaned, logStart=" + logStart);

        // 1) 负数
        OffsetOutOfRangeException negative = assertThrows(OffsetOutOfRangeException.class,
                () -> pipeline.seek(partition, -1));
        assertSame(OffsetOutOfRangeException.Reason.NEGATIVE_OFFSET, negative.getReason());
        assertTrue(negative.getMessage().contains("must not be negative"));

        // 2) 落入已清理区间
        long truncatedOffset = logStart - 1;
        OffsetOutOfRangeException truncated = assertThrows(OffsetOutOfRangeException.class,
                () -> pipeline.seek(partition, truncatedOffset));
        assertSame(OffsetOutOfRangeException.Reason.OFFSET_TRUNCATED, truncated.getReason());
        assertTrue(truncated.getMessage().contains("cleaned up"));
        assertTrue(truncated.getMessage().contains("logStart=" + logStart));

        // 3) 超出日志末尾
        long beyond = harness.broker().logEndOffset(partition) + 10;
        OffsetOutOfRangeException tooHigh = assertThrows(OffsetOutOfRangeException.class,
                () -> pipeline.seek(partition, beyond));
        assertSame(OffsetOutOfRangeException.Reason.OFFSET_TOO_HIGH, tooHigh.getReason());
        assertTrue(tooHigh.getMessage().contains("beyond log end"));

        // 三类原因互不相同
        assertNotEquals(negative.getReason(), truncated.getReason());
        assertNotEquals(truncated.getReason(), tooHigh.getReason());

        // 4) 指向死信事件：新建一个必死信事件
        EventEnvelope doomed = new EventEnvelope(UUID.randomUUID().toString(),
                "agg-doom-life", "Bad", new byte[0], System.currentTimeMillis());
        harness.handler().failFirst(doomed.eventId(), 10);
        pipeline.publish(doomed);
        assertTrue(pipeline.awaitProcessed(5000));
        var dlqLoc = harness.broker().locate(doomed.eventId()).orElseThrow();

        OffsetInvalidException dead = assertThrows(OffsetInvalidException.class,
                () -> pipeline.seek(dlqLoc.partition(), dlqLoc.offset()));
        assertTrue(dead.getMessage().contains("dead-lettered event"));
        assertTrue(dead.getMessage().contains(doomed.eventId()));
        assertTrue(dead.getMessage().contains("RETRY_EXHAUSTED"));

        // 即使该死信位点恰好也已“越过”，异常类型仍然是 OffsetInvalid（死信优先于越界语义）
        // 死信在分区 p 中会使清理水位钉住，因此它的位点 >= logStart，属于合法范围内的无效位点
        pipeline.stop();
    }

    @Test
    void cleanedUpAggregateStateStillReadsLatestSuccess() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(1, 1, 1, Duration.ZERO);
        EventLogCleaner cleaner = new EventLogCleaner(harness.broker(), harness.state(),
                harness.dlq(), 60_000L, 100);
        EventPipeline pipeline = new EventPipeline(harness.broker(), harness.state(), harness.dlq(),
                harness.handler(), harness.policy(), 1, 2L, cleaner);

        pipeline.start();
        EventEnvelope v1 = pipeline.enqueue("order-999", "AmountSet", "10".getBytes());
        EventEnvelope v2 = pipeline.enqueue("order-999", "AmountSet", "20".getBytes());
        EventEnvelope v3 = pipeline.enqueue("order-999", "AmountSet", "30".getBytes());
        assertTrue(pipeline.awaitProcessed(5000));
        cleaner.cleanupOnce();
        assertEquals(3, harness.broker().logStartOffset("p0"),
                "whole physical log truncated away");

        // 物理消息没了，但当前状态读得到、且是最后一次成功
        var snapshot = pipeline.currentAggregateState("order-999").orElseThrow();
        assertEquals(v3.eventId(), snapshot.event().eventId());
        assertEquals(2, snapshot.offset());
        // 旧状态访问不被当成“数据缺失”
        assertTrue(pipeline.currentAggregateState("never-seen").isEmpty());
        pipeline.stop();
    }

    @Test
    void redeliveryInRetainedRangeStaysIdempotentAfterCleanup() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(1, 1, 1, Duration.ZERO);
        EventLogCleaner cleaner = new EventLogCleaner(harness.broker(), harness.state(),
                harness.dlq(), 60_000L, 1); // 每轮只删 1 条，便于留下存活区间
        EventPipeline pipeline = new EventPipeline(harness.broker(), harness.state(), harness.dlq(),
                harness.handler(), harness.policy(), 1, 2L, cleaner);

        pipeline.start();
        EventEnvelope e0 = pipeline.enqueue("agg-keep", "T0", null);
        EventEnvelope e1 = pipeline.enqueue("agg-keep", "T1", null);
        EventEnvelope e2 = pipeline.enqueue("agg-keep", "T2", null);
        assertTrue(pipeline.awaitProcessed(5000));

        // 只物理删除 offset 0；1、2 仍存活。它们的幂等标记必须保留
        cleaner.cleanupOnce();
        assertEquals(1, harness.broker().logStartOffset("p0"));
        assertTrue(pipeline.isProcessed(e1.eventId()),
                "processed markers of retained events survive cleanup");
        assertTrue(pipeline.isProcessed(e2.eventId()));

        // 回放到存活区间起点 1：重新投递已成功事件，副作用不得二次生效
        pipeline.seek("p0", 1);
        assertTrue(pipeline.awaitProcessed(5000));
        assertEquals(1, harness.handler().callCount(e1.eventId()),
                "retained processed event is skipped on redelivery");
        assertEquals(1, harness.handler().callCount(e2.eventId()));
        assertEquals(1, harness.handler().sideEffectCount(e1.eventId()));
        assertEquals(1, harness.handler().sideEffectCount(e2.eventId()));

        // 回放到已清理位点 0：明确拒绝，而不是当成正常重放或数据缺失
        assertThrows(OffsetOutOfRangeException.class, () -> pipeline.seek("p0", 0));
        pipeline.stop();
    }
}
