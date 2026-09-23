package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEvent;
import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.outbox.dlq.DeadLetterEntry;
import com.github.h3yow.eventoutboxpipelinejava.outbox.handler.EventHandler;
import com.github.h3yow.eventoutboxpipelinejava.outbox.idempotency.IdempotencyStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 并发消费叠加失败处理的一致性测试：
 * 多工作线程并行处理大量事件，其中部分事件间歇性失败、部分永久失败，
 * 最终状态必须满足：成功事件副作用恰好一次；永久失败事件恰好尝试 maxAttempts 次；
 * 成功态与死信态互斥；位点全部推进到末尾；重试计数并发下不丢失/不串扰。
 */
class ConcurrentFailureConsistencyTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentFailureConsistencyTest.class);

    @Test
    void mixedSuccessTransientAndPoisonUnderConcurrency() {
        int aggregates = 16;
        int perAggregate = 50;
        int maxAttempts = 4;

        ConcurrentMap<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        EventHandler handler = (EventHandler) event -> {
            int n = calls.computeIfAbsent(event.eventId(), k -> new AtomicInteger()).incrementAndGet();
            String id = event.eventId();
            int suffix = Integer.parseInt(id.substring(id.lastIndexOf('-') + 1));
            if (suffix % 10 == 0) {
                // 永久失败：必进死信
                throw new IllegalStateException("poison-" + id);
            }
            if (suffix % 5 == 0) {
                // 间歇失败：前两次失败，之后成功
                if (n <= 2) {
                    throw new IllegalStateException("transient-" + id + "-#" + n);
                }
            }
        };

        try (OutboxEventPipeline pipeline = Pipelines.create(handler, maxAttempts, 5L, 8)) {
            pipeline.start();
            for (int a = 0; a < aggregates; a++) {
                for (int i = 0; i < perAggregate; i++) {
                    pipeline.enqueue("agg-" + a, "agg-" + a + "-" + i, "T", "p");
                }
            }
            pipeline.awaitQuiescence(30000);

            int total = aggregates * perAggregate;
            int expectedPoison = 0;
            int expectedTransient = 0;
            for (int a = 0; a < aggregates; a++) {
                for (int i = 0; i < perAggregate; i++) {
                    if (i % 10 == 0) {
                        expectedPoison++;
                    } else if (i % 5 == 0) {
                        expectedTransient++;
                    }
                }
            }
            int expectedSuccess = total - expectedPoison;

            log.info("[conc-fail] total={}, poison(dead)={}, transient(retried-then-ok)={}, expectedSuccess={}",
                    total, expectedPoison, expectedTransient, expectedSuccess);

            assertEquals(expectedPoison, pipeline.deadLetters().findByState(
                    com.github.h3yow.eventoutboxpipelinejava.outbox.core.DeadLetterState.OPEN).size());

            int poisonAttempts = 0;
            int transientAttempts = 0;
            for (int a = 0; a < aggregates; a++) {
                for (int i = 0; i < perAggregate; i++) {
                    String id = "agg-" + a + "-" + i;
                    int n = calls.get(id).get();
                    IdempotencyStore.Record rec = pipeline.idempotency().find(id).orElseThrow();
                    if (i % 10 == 0) {
                        assertEquals(maxAttempts, n, "死信事件的尝试次数必须恰好等于 maxAttempts: " + id);
                        assertEquals(IdempotencyStore.ResultState.DEAD, rec.state(),
                                "成功与死信不得同时发生: " + id);
                        assertFalse(pipeline.deadLetters().findByEventId(id).isEmpty());
                        poisonAttempts += n;
                    } else if (i % 5 == 0) {
                        assertEquals(3, n, "间歇失败事件应恰好重试到第 3 次成功: " + id);
                        assertEquals(IdempotencyStore.ResultState.PROCESSED, rec.state());
                        transientAttempts += n;
                    } else {
                        assertEquals(1, n, "普通事件应一次成功: " + id);
                        assertEquals(IdempotencyStore.ResultState.PROCESSED, rec.state());
                    }
                }
                assertEquals(perAggregate,
                        pipeline.offsets().committed("test-group", "agg-" + a, 0L),
                        "位点必须连续推进到末尾: agg-" + a);
            }
            log.info("[conc-fail] poisonAttempts={} (each {} tries), transientAttempts={} (each 3 tries), all offsets advanced",
                    poisonAttempts, maxAttempts, transientAttempts);

            // 每条死信都保留可区分的失败原因
            for (DeadLetterEntry e : pipeline.deadLetters().findAll()) {
                assertEquals(IllegalStateException.class.getName(), e.lastFailureType());
                assertEquals("poison-" + e.event().eventId(), e.lastFailureReason());
            }
            log.info("[conc-fail] consistency verified: successSideEffects={}, dlq={}, distinct reasons preserved",
                    total - expectedPoison, expectedPoison);
        }
    }
}
