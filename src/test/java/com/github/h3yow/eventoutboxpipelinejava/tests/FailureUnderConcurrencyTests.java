package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.dlq.DeadLetterRecord;
import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory.TestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 混合负载（成功 / 瞬时失败重试 / 永久失败死信）在并发消费下的最终一致性：
 * 每个事件要么恰好成功一次，要么恰好在死信区有一条记录，二者绝不重叠。
 */
class FailureUnderConcurrencyTests {

    @Test
    void mixedWorkloadEndsConsistent() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(8, 8, 3, Duration.ofMillis(5));
        var pipeline = harness.pipeline();

        Set<String> transientFail = ConcurrentHashMap.newKeySet();
        Set<String> poison = ConcurrentHashMap.newKeySet();
        Set<String> healthy = ConcurrentHashMap.newKeySet();

        pipeline.start();
        int perClass = 160;
        // 先生成全部事件并注册失败注入，再发布：避免“已入队但失败规则尚未注册”的时序竞态
        List<EventEnvelope> toPublish = new java.util.ArrayList<>();
        for (int i = 0; i < perClass; i++) {
            EventEnvelope ok = new EventEnvelope(java.util.UUID.randomUUID().toString(),
                    "agg-ok-" + (i % 12), "Ok", new byte[0], System.currentTimeMillis());
            healthy.add(ok.eventId());
            toPublish.add(ok);

            EventEnvelope flaky = new EventEnvelope(java.util.UUID.randomUUID().toString(),
                    "agg-flaky-" + (i % 12), "Flaky", new byte[0], System.currentTimeMillis());
            transientFail.add(flaky.eventId());
            harness.handler().failFirst(flaky.eventId(), 2); // 3 次尝试内成功
            toPublish.add(flaky);

            EventEnvelope bad = new EventEnvelope(java.util.UUID.randomUUID().toString(),
                    "agg-bad-" + (i % 12), "Bad", new byte[0], System.currentTimeMillis());
            poison.add(bad.eventId());
            harness.handler().failFirst(bad.eventId(), 1_000); // 必死信
            toPublish.add(bad);
        }
        for (EventEnvelope e : toPublish) {
            pipeline.publish(e);
        }

        assertTrue(pipeline.awaitProcessed(30000), "mixed workload drains");
        pipeline.stop();

        // 位点全部齐平（成功与死信都会推进位点）
        for (String partition : harness.broker().partitions()) {
            assertEquals(harness.broker().logEndOffset(partition),
                    pipeline.committedOffset(partition),
                    "partition " + partition + " fully consumed");
        }

        List<DeadLetterRecord> dead = harness.dlq().findAll();
        Set<String> deadIds = new HashSet<>();
        for (DeadLetterRecord r : dead) {
            assertTrue(deadIds.add(r.event().eventId()), "no duplicated dead-letter entries");
            assertEquals(3, r.attempts(), "attempt count persisted: " + r.event().eventId());
            assertTrue(r.reason().startsWith("RETRY_EXHAUSTED"));
            assertFalse(pipeline.isProcessed(r.event().eventId()),
                    "dead-lettered event must never be marked processed");
        }
        assertEquals(poison, deadIds, "exactly the poison events are dead-lettered");

        // 成功集合与死信集合不相交，且覆盖全部事件
        Set<String> processed = new HashSet<>(harness.state().processedEventIds());
        assertTrue(processed.containsAll(healthy));
        assertTrue(processed.containsAll(transientFail));
            for (String id : transientFail) {
            assertEquals(3, harness.handler().callCount(id),
                    "retried event invoked 3 times but side effect once");
            assertEquals(1, harness.handler().sideEffectCount(id));
        }
        for (String id : healthy) {
            assertEquals(1, harness.handler().callCount(id));
        }
        for (String id : poison) {
            assertFalse(processed.contains(id), "no overlap success/dead-letter");
        }
        assertEquals(perClass * 3, processed.size() + deadIds.size(),
                "nothing silently dropped: every event is processed or dead-lettered");
        assertTrue(harness.handler().concurrentReentries().isEmpty(),
                "retries/partitions never processed concurrently: "
                        + harness.handler().concurrentReentries());
    }
}
