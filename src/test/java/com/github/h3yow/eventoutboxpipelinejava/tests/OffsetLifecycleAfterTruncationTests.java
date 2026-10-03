package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.offset.OffsetInvalidException;
import com.github.h3yow.eventoutboxpipelinejava.offset.OffsetOutOfRangeException;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory.TestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 清理之后的位点生命周期：
 * - 落到已截断区间的位点 -> OFFSET_TRUNCATED；
 * - 负数位点 -> NEGATIVE_OFFSET；
 * - 超出末尾的位点 -> OFFSET_TOO_HIGH；
 * - 指向死信事件的位点 -> OffsetInvalidException；
 * 四类拒绝原因必须一眼可分，且当前状态（幂等结果、死信记录、后续可读事件）不受清理影响。
 */
class OffsetLifecycleAfterTruncationTests {

    /** 构造：成功 a@0、死信 doomed@1、成功 c@2，截断后 logStart=1。 */
    private TestHarness prepareTrimmedPartition() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(1, 2, 1, Duration.ZERO);
        var pipeline = harness.pipeline();
        pipeline.start();
        EventEnvelope a = pipeline.enqueue("agg-l", "A", null);
        EventEnvelope doomed = new EventEnvelope(UUID.randomUUID().toString(),
                "agg-l", "Bad", new byte[0], System.currentTimeMillis());
        harness.handler().failFirst(doomed.eventId(), 10);
        pipeline.publish(doomed);
        EventEnvelope c = pipeline.enqueue("agg-l", "C", null);
        assertTrue(pipeline.awaitProcessed(5000));
        assertEquals(1, harness.dlq().size());
        int removed = pipeline.compactLog();
        assertEquals(1, removed);
        return harness;
    }

    @Test
    void fourRejectionReasonsAreDistinguishableAfterTrimming() throws Exception {
        TestHarness harness = prepareTrimmedPartition();
        var pipeline = harness.pipeline();
        // logStart=1（死信屏障），end=3；offset 1 为死信，offset 2 为成功事件

        OffsetOutOfRangeException truncated = assertThrows(OffsetOutOfRangeException.class,
                () -> pipeline.seek("p0", 0));
        assertSame(OffsetOutOfRangeException.Reason.OFFSET_TRUNCATED, truncated.getReason());
        assertTrue(truncated.getMessage().contains("logStart=1"),
                "truncated rejection states the log start: " + truncated.getMessage());

        OffsetOutOfRangeException negative = assertThrows(OffsetOutOfRangeException.class,
                () -> pipeline.seek("p0", -1));
        assertSame(OffsetOutOfRangeException.Reason.NEGATIVE_OFFSET, negative.getReason());

        OffsetOutOfRangeException tooHigh = assertThrows(OffsetOutOfRangeException.class,
                () -> pipeline.seek("p0", 99));
        assertSame(OffsetOutOfRangeException.Reason.OFFSET_TOO_HIGH, tooHigh.getReason());
        assertTrue(tooHigh.getMessage().contains("logEnd=3"));

        OffsetInvalidException deadLetter = assertThrows(OffsetInvalidException.class,
                () -> pipeline.seek("p0", 1));
        assertTrue(deadLetter.getMessage().contains("dead-lettered"));
        assertTrue(deadLetter.getMessage().contains("offset=1"));

        pipeline.stop();
    }

    @Test
    void currentStateRemainsReadableAfterTrimming() throws Exception {
        TestHarness harness = prepareTrimmedPartition();
        var pipeline = harness.pipeline();

        // 水位线推进到了日志末尾（成功/死信都算终态）
        assertEquals(3, pipeline.committedOffset("p0"));
        assertEquals(1, pipeline.logStartOffset("p0"));
        // 截断点之前已成功的事件，幂等结论仍可读（不被当成“数据缺失”）
        EventEnvelope a = harness.broker().read("p0", 0).orElse(null);
        assertEquals(null, a, "physical event gone");
        // 已处理状态无法直接按 eventId 取回信封，但成功集合仍保留结论：
        // 后续新事件正常入队/消费，证明当前状态未被清理破坏
        EventEnvelope next = pipeline.enqueue("agg-l", "D", null);
        assertTrue(pipeline.awaitProcessed(5000));
        assertEquals(4, pipeline.committedOffset("p0"));
        assertTrue(pipeline.isProcessed(next.eventId()));
        assertEquals(1, harness.handler().callCount(next.eventId()));

        // 死信记录完整保留、可检索
        assertEquals(1, harness.dlq().size());
        var deadRecord = harness.dlq().findAll().get(0);
        assertEquals(1, deadRecord.offset());
        // 屏障后的成功事件物理可读
        assertEquals(next.eventId(), harness.broker().read("p0", 3).orElseThrow().eventId());

        pipeline.stop();
    }

    @Test
    void replayAfterTrimmingStillAppliesSideEffectOnceAndThenTrims() throws Exception {
        TestHarness harness = prepareTrimmedPartition();
        var pipeline = harness.pipeline();

        var deadRecord = harness.dlq().findAll().get(0);
        String deadId = deadRecord.event().eventId();
        assertEquals(0, harness.handler().sideEffectCount(deadId));
        assertFalse(pipeline.isProcessed(deadId));

        // 修复处理器后走正式重放：移除死信 -> 回放位点 -> 成功，副作用仅一次
        harness.handler().failFirst(deadId, 0);
        pipeline.replay(deadId);
        assertTrue(pipeline.awaitProcessed(5000));
        assertEquals(1, harness.handler().sideEffectCount(deadId),
                "side effect applies exactly once on replay");
        assertEquals(0, harness.dlq().size());
        // 再回放一次（此时该位点是普通成功位点，合法）：幂等集合拦截，副作用不再生效
        pipeline.seek("p0", 1);
        assertTrue(pipeline.awaitProcessed(5000));
        assertEquals(1, harness.handler().sideEffectCount(deadId));

        // 死信屏障解除后，清理可推进到日志末尾（此前 logStart=1，本轮回收 2 条）
        int removed = pipeline.compactLog();
        assertEquals(2, removed, "all remaining events including replayed one are reclaimable");
        assertEquals(3, pipeline.logStartOffset("p0"));
        assertEquals(3, pipeline.logEndOffset("p0"));
        pipeline.stop();
    }

    @Test
    void seekToTruncatedEventIdIsRejectedNotSilentlySkipped() throws Exception {
        TestHarness harness = prepareTrimmedPartition();
        var pipeline = harness.pipeline();

        // offset 0 上的事件已成功处理且被截断：按位点回放必须明确拒绝，而不是静默跳过
        OffsetOutOfRangeException ex = assertThrows(OffsetOutOfRangeException.class,
                () -> pipeline.seek("p0", 0));
        assertSame(OffsetOutOfRangeException.Reason.OFFSET_TRUNCATED, ex.getReason());

        // 未知分区/未知 eventId 仍然是参数错误
        assertThrows(IllegalArgumentException.class,
                () -> pipeline.seek("nope", 0));
        assertThrows(IllegalArgumentException.class,
                () -> pipeline.seekToEvent(UUID.randomUUID().toString()));
        pipeline.stop();
    }
}
