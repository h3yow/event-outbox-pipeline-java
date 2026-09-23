package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.offset.OffsetInvalidException;
import com.github.h3yow.eventoutboxpipelinejava.offset.OffsetOutOfRangeException;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory.TestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 幂等与位点管理：
 * - 重复入队（相同 eventId）不产生重复消息；
 * - 位点回放重新投递已成功事件时业务副作用只生效一次；
 * - 越界位点以可区分原因拒绝；已死信位点以 OffsetInvalidException 拒绝。
 */
class IdempotencyAndOffsetTests {

    @Test
    void duplicatePublishDoesNotDuplicateMessage() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(2, 2, 1, Duration.ZERO);
        var pipeline = harness.pipeline();

        EventEnvelope e = new EventEnvelope("evt-dup", "agg-x", "T", "hello".getBytes(),
                System.currentTimeMillis());
        pipeline.start();
        var loc1 = pipeline.publish(e);
        var loc2 = pipeline.publish(e); // 同一 eventId 重复投递
        var loc3 = pipeline.publish(new EventEnvelope("evt-dup", "agg-x", "T",
                "hello".getBytes(), System.currentTimeMillis()));
        assertTrue(pipeline.awaitProcessed(5000));
        pipeline.stop();

        assertEquals(loc1, loc2);
        assertEquals(loc1, loc3);
        assertEquals(1, harness.handler().callCount("evt-dup"),
                "duplicate publish must be collapsed");
        assertEquals(1, harness.handler().sideEffectCount("evt-dup"));
    }

    @Test
    void replayAfterRewindDoesNotReapplySideEffect() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(2, 2, 1, Duration.ZERO);
        var pipeline = harness.pipeline();

        pipeline.start();
        EventEnvelope e1 = pipeline.enqueue("agg-r", "T1", null);
        EventEnvelope e2 = pipeline.enqueue("agg-r", "T2", null);
        assertTrue(pipeline.awaitProcessed(5000));

        // 模拟重启后位点回放：回退到 0，重新投递两个已成功事件
        String partition = harness.broker().partitionOf("agg-r");
        pipeline.seek(partition, 0);
        assertTrue(pipeline.awaitProcessed(5000));
        pipeline.stop();

        // 管道层幂等：已成功处理过的 eventId 不再进入业务处理器
        assertEquals(1, harness.handler().callCount(e1.eventId()),
                "redelivery must not re-enter handler for processed event");
        assertEquals(1, harness.handler().callCount(e2.eventId()));
        assertEquals(1, harness.handler().sideEffectCount(e1.eventId()),
                "business side effect applies exactly once");
        assertEquals(1, harness.handler().sideEffectCount(e2.eventId()));
        assertEquals(2, pipeline.committedOffset(partition));
    }

    @Test
    void outOfRangeOffsetsRejectedWithDistinctReasons() {
        TestHarness harness = PipelineFactory.newHarness(2, 1, 1, Duration.ZERO);
        var pipeline = harness.pipeline();
        EventEnvelope e = harness.pipeline().enqueue("agg-o", "T", null);
        String partition = harness.broker().partitionOf("agg-o");
        assertEquals(1, harness.broker().logEndOffset(partition));

        OffsetOutOfRangeException negative = assertThrows(OffsetOutOfRangeException.class,
                () -> pipeline.seek(partition, -1));
        assertSame(OffsetOutOfRangeException.Reason.NEGATIVE_OFFSET, negative.getReason());

        OffsetOutOfRangeException tooHigh = assertThrows(OffsetOutOfRangeException.class,
                () -> pipeline.seek(partition, 42));
        assertSame(OffsetOutOfRangeException.Reason.OFFSET_TOO_HIGH, tooHigh.getReason());

        assertThrows(IllegalArgumentException.class,
                () -> pipeline.seek("no-such-partition", 0));
        assertThrows(IllegalArgumentException.class,
                () -> pipeline.seekToEvent("missing-event-id"));
    }

    @Test
    void seekToDeadLetterOffsetIsRejectedUntilReplay() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(1, 1, 1, Duration.ZERO);
        var pipeline = harness.pipeline();

        pipeline.start();
        EventEnvelope doomed = new EventEnvelope(java.util.UUID.randomUUID().toString(),
                "agg-d", "Bad", new byte[0], System.currentTimeMillis());
        harness.handler().failFirst(doomed.eventId(), 10);
        pipeline.publish(doomed);
        EventEnvelope after = pipeline.enqueue("agg-d", "Good", null);
        assertTrue(pipeline.awaitProcessed(5000));

        var location = pipeline.locate(doomed.eventId());
        // 死信位点明确拒绝，原因可区分
        OffsetInvalidException ex = assertThrows(OffsetInvalidException.class,
                () -> pipeline.seek(location.partition(), location.offset()));
        assertTrue(ex.getMessage().contains(doomed.eventId()));

        // 走正式重放：移除死信 -> 回放位点；但处理仍失败时会再次进入死信
        pipeline.replay(doomed.eventId());
        assertTrue(pipeline.awaitProcessed(5000));
        assertEquals(1, harness.dlq().size(), "still failing -> back in DLQ, not lost");
        assertEquals(2, pipeline.committedOffset(location.partition()));

        // 让重放成功：移除死信 + 处理器不再失败 + 回放
        harness.handler().failFirst(doomed.eventId(), 0);
        pipeline.replay(doomed.eventId());
        assertTrue(pipeline.awaitProcessed(5000));
        pipeline.stop();

        assertEquals(0, harness.dlq().size());
        assertTrue(pipeline.isProcessed(doomed.eventId()));
        assertEquals(1, harness.handler().sideEffectCount(doomed.eventId()),
                "side effect applied exactly once on successful replay");
    }
}
