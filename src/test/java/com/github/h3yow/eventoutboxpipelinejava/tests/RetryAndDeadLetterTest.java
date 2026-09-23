package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.outbox.core.DeadLetterState;
import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEvent;
import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.outbox.dlq.DeadLetterEntry;
import com.github.h3yow.eventoutboxpipelinejava.outbox.handler.EventHandler;
import com.github.h3yow.eventoutboxpipelinejava.outbox.idempotency.IdempotencyStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 失败重试与死信场景：
 * <ol>
 *   <li>暂时失败按策略重试，达到上限才进死信，且重试次数与策略一致；</li>
 *   <li>死信条目保留可区分的失败类型与原因，状态为 OPEN；</li>
 *   <li>进死信后同聚合后续事件继续推进（不卡死），位点正确；</li>
 *   <li>成功与死信不会同时发生（PROCESSED 与 DLQ 互斥）；</li>
 *   <li>死信重放：处理器修复后回放事件可成功完成，副作用生效且仅一次。</li>
 * </ol>
 */
class RetryAndDeadLetterTest {

    private static final Logger log = LoggerFactory.getLogger(RetryAndDeadLetterTest.class);

    @Test
    void transientFailureRetriesThenSucceeds() {
        AtomicInteger calls = new AtomicInteger();
        EventHandler handler = event -> {
            if (event.eventId().equals("flaky") && calls.incrementAndGet() < 3) {
                throw new IllegalStateException("transient error #" + calls.get());
            }
        };
        try (OutboxEventPipeline pipeline = Pipelines.create(handler, 5, 20L, 2)) {
            pipeline.start();
            pipeline.enqueue("A", "flaky", "T", "p");
            pipeline.awaitQuiescence(5000);

            assertEquals(3, calls.get());
            IdempotencyStore.Record rec = pipeline.idempotency().find("flaky").orElseThrow();
            log.info("[retry] eventId=flaky: invocations={}, terminalState={}, dlqSize={}",
                    calls.get(), rec.state(), pipeline.deadLetters().size());
            assertEquals(IdempotencyStore.ResultState.PROCESSED, rec.state());
            assertEquals(0, pipeline.deadLetters().size());
            assertEquals(1L, pipeline.offsets().committed("test-group", "A", 0L));
        }
    }

    @Test
    void permanentFailureGoesToDeadLetterWithReasonAndStreamAdvances() {
        AtomicInteger poisonCalls = new AtomicInteger();
        AtomicInteger okCalls = new AtomicInteger();
        EventHandler handler = event -> {
            if (event.eventId().equals("poison")) {
                poisonCalls.incrementAndGet();
                throw new IllegalArgumentException("permanent poison payload");
            }
            okCalls.incrementAndGet();
        };
        try (OutboxEventPipeline pipeline = Pipelines.create(handler, 3, 10L, 2)) {
            pipeline.start();
            pipeline.enqueue("A", "poison", "T", "fail:x");
            pipeline.enqueue("A", "after-1", "T", "ok");
            pipeline.enqueue("A", "after-2", "T", "ok");
            pipeline.awaitQuiescence(5000);

            log.info("[dlq] poison invocations={} (expect 3 = 首次+2次重试), followingEventsHandled={}",
                    poisonCalls.get(), okCalls.get());
            assertEquals(3, poisonCalls.get(), "总尝试次数必须等于 maxAttempts");
            assertEquals(2, okCalls.get(), "死信事件不得永久阻塞同聚合后续事件");

            DeadLetterEntry entry = pipeline.deadLetters().findByEventId("poison").orElseThrow();
            log.info("[dlq] entry: eventId={}, aggregate={}, offset={}, attempts={}, state={}, reasonType={}, reason='{}'",
                    entry.event().eventId(), entry.event().aggregateId(), entry.event().sequence(),
                    entry.attempts(), entry.state(), entry.lastFailureType(), entry.lastFailureReason());
            assertEquals(3, entry.attempts());
            assertEquals(DeadLetterState.OPEN, entry.state());
            assertEquals(IllegalArgumentException.class.getName(), entry.lastFailureType());
            assertTrue(entry.lastFailureReason().contains("permanent poison payload"));
            assertEquals(3L, pipeline.offsets().committed("test-group", "A", 0L),
                    "三个事件位点全部推进（含被死信跳过的 poison）");

            // 成功与死信互斥：poison 不得同时出现在幂等成功态中
            IdempotencyStore.Record rec = pipeline.idempotency().find("poison").orElseThrow();
            log.info("[dlq] poison terminal idempotency state={} (expect DEAD)", rec.state());
            assertEquals(IdempotencyStore.ResultState.DEAD, rec.state());
        }
    }

    @Test
    void replayDeadLetterSucceedsAfterHandlerFixed() {
        AtomicInteger calls = new AtomicInteger();
        EventHandler alwaysFailing = event -> {
            calls.incrementAndGet();
            throw new IllegalStateException("not fixed yet");
        };
        AtomicInteger replayCalls = new AtomicInteger();
        EventHandler fixed = event -> replayCalls.incrementAndGet();

        // 阶段 1：旧处理器下事件耗尽重试进入死信（沿用同一套持久组件）
        var broker = new com.github.h3yow.eventoutboxpipelinejava.outbox.broker.InMemoryEventBroker();
        var offsets = new com.github.h3yow.eventoutboxpipelinejava.outbox.offset.OffsetStore();
        var dlq = new com.github.h3yow.eventoutboxpipelinejava.outbox.dlq.DeadLetterQueue();
        var idem = new IdempotencyStore();
        var policy = new com.github.h3yow.eventoutboxpipelinejava.outbox.retry.RetryPolicy(
                2, attempt -> 10L);
        OutboxEventPipeline p1 = new OutboxEventPipeline(broker, offsets, dlq, idem,
                alwaysFailing, policy, 2, "test-group", 50L);
        p1.start();
        p1.enqueue("A", "e-replay", "T", "p");
        p1.awaitQuiescence(3000);
        p1.close();
        assertEquals(2, calls.get());
        DeadLetterEntry before = dlq.findByEventId("e-replay").orElseThrow();
        assertEquals(DeadLetterState.OPEN, before.state());

        // 阶段 2：处理器修复，新消费者基于相同状态重放该死信
        OutboxEventPipeline p2 = new OutboxEventPipeline(broker, offsets, dlq, idem,
                fixed, policy, 2, "test-group", 50L);
        p2.start();
        p2.replayDeadLetter("e-replay");
        p2.awaitQuiescence(3000);

        log.info("[replay] fixed-handler invocations={} (expect 1), offset={}",
                replayCalls.get(), offsets.committed("test-group", "A", 0L));
        assertEquals(1, replayCalls.get());
        assertEquals(1L, offsets.committed("test-group", "A", 0L));
        DeadLetterEntry after = dlq.findByEventId("e-replay").orElseThrow();
        assertEquals(DeadLetterState.REPLAYED, after.state());
        assertNotNull(idem.find("e-replay").orElseThrow());
        assertEquals(IdempotencyStore.ResultState.PROCESSED, idem.find("e-replay").orElseThrow().state());
        p2.close();
    }
}
