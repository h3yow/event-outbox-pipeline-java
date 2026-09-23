package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.dlq.DeadLetterRecord;
import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory.TestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 失败重试策略：可重试至上限后成功；持续失败转入可检索死信区，原因可区分，且事件不丢弃。
 */
class RetryAndDeadLetterTests {

    @Test
    void succeedsAfterTransientFailuresWithinRetryBudget() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(2, 2, 4, Duration.ofMillis(10));
        var pipeline = harness.pipeline();

        pipeline.start();
        EventEnvelope flaky = new EventEnvelope(java.util.UUID.randomUUID().toString(),
                "agg-1", "Flaky", new byte[0], System.currentTimeMillis());
        harness.handler().failFirst(flaky.eventId(), 2); // 先注册失败，再发布
        pipeline.publish(flaky);
        EventEnvelope ok = pipeline.enqueue("agg-2", "Fine", null);
        assertTrue(pipeline.awaitProcessed(5000));
        pipeline.stop();

        assertEquals(3, harness.handler().callCount(flaky.eventId()),
                "invoked once initially + twice retried");
        assertTrue(harness.handler().concurrentReentries().isEmpty(),
                "retries must never run concurrently: " + harness.handler().concurrentReentries());
        assertTrue(harness.pipeline().isProcessed(flaky.eventId()));
        assertEquals(1, harness.handler().callCount(ok.eventId()));
        assertEquals(0, harness.dlq().size(), "recovered event must not be dead-lettered");
    }

    @Test
    void exhaustedRetriesMoveToDeadLetterWithReason() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(2, 2, 3, Duration.ofMillis(5));
        var pipeline = harness.pipeline();

        pipeline.start();
        EventEnvelope doomed = new EventEnvelope(java.util.UUID.randomUUID().toString(),
                "agg-doom", "Poison", new byte[0], System.currentTimeMillis());
        harness.handler().failFirst(doomed.eventId(), 100);
        pipeline.publish(doomed);
        EventEnvelope fine = pipeline.enqueue("agg-fine", "Fine", null);
        assertTrue(pipeline.awaitProcessed(5000));
        pipeline.stop();

        assertEquals(3, harness.handler().callCount(doomed.eventId()),
                "attempts capped by maxAttempts");
        assertFalse(pipeline.isProcessed(doomed.eventId()), "poison event never succeeds");
        assertEquals(1, harness.dlq().size());

        Optional<DeadLetterRecord> found = harness.dlq().findByEventId(doomed.eventId());
        assertTrue(found.isPresent(), "dead letter must be retrievable by eventId");
        DeadLetterRecord record = found.get();
        assertEquals(3, record.attempts());
        assertEquals(harness.broker().partitionOf("agg-doom"), record.partition());
        assertTrue(record.reason().startsWith("RETRY_EXHAUSTED"),
                "reason must be distinguishable, got: " + record.reason());
        assertTrue(record.reason().contains("IllegalStateException"),
                "reason must preserve failure cause type");
        assertTrue(record.reason().contains("injected-failure"),
                "reason must preserve failure message");

        // 正常事件仍被处理，位点越过死信事件继续推进，没有任何事件静默丢失
        assertTrue(pipeline.isProcessed(fine.eventId()));
        String pDoom = record.partition();
        assertEquals(harness.broker().logEndOffset(pDoom), pipeline.committedOffset(pDoom),
                "offset advances past the dead-lettered event");
    }

    @Test
    void noRetryPolicyDeadLettersOnFirstFailure() throws Exception {
        // maxAttempts=1 的语义在工厂内不便定制，这里直接组合组件
        var harness = PipelineFactory.newHarness(1, 1, 1, Duration.ZERO);
        var pipeline = harness.pipeline();
        pipeline.start();
        EventEnvelope doomed = new EventEnvelope(java.util.UUID.randomUUID().toString(),
                "agg", "Bad", new byte[0], System.currentTimeMillis());
        harness.handler().failFirst(doomed.eventId(), 5);
        pipeline.publish(doomed);
        assertTrue(pipeline.awaitProcessed(5000));
        pipeline.stop();

        assertEquals(1, harness.handler().callCount(doomed.eventId()));
        assertEquals(1, harness.dlq().size());
    }
}
