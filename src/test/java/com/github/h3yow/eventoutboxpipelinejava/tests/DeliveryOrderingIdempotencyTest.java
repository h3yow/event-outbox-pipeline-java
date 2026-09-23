package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEvent;
import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.outbox.handler.RecordingEventHandler;
import com.github.h3yow.eventoutboxpipelinejava.outbox.idempotency.IdempotencyStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 投递、保序与幂等场景：
 * <ol>
 *   <li>同聚合事件按入队稳定顺序被消费；不同聚合互不阻塞；</li>
 *   <li>重复投递（seek 回放 + 模拟重启）时业务副作用只生效一次；</li>
 *   <li>重复 eventId 入队被明确拒绝，绝不静默覆盖。</li>
 * </ol>
 */
class DeliveryOrderingIdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(DeliveryOrderingIdempotencyTest.class);

    /** 记录每个事件被业务处理器看到的顺序，用于断言同聚合严格有序。 */
    private static final class OrderAwareHandler extends RecordingEventHandler {
        private final CopyOnWriteArrayList<String> order = new CopyOnWriteArrayList<>();

        @Override
        public void handle(OutboxEvent event) {
            super.handle(event);
            order.add(event.aggregateId() + "#" + event.sequence());
        }

        List<String> order() {
            return List.copyOf(order);
        }
    }

    @Test
    void sameAggregateConsumedInOrder_differentAggregatesInParallel() {
        OrderAwareHandler handler = new OrderAwareHandler();
        try (OutboxEventPipeline pipeline = Pipelines.create(handler, 1)) {
            pipeline.start();
            for (int i = 0; i < 50; i++) {
                pipeline.enqueue("A", "a-" + i, "T", "p");
                pipeline.enqueue("B", "b-" + i, "T", "p");
            }
            pipeline.awaitQuiescence(5000);

            List<String> expectedA = new ArrayList<>();
            List<String> expectedB = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                expectedA.add("A#" + i);
                expectedB.add("B#" + i);
            }
            assertEquals(expectedA, handler.order().stream().filter(s -> s.startsWith("A#")).toList());
            assertEquals(expectedB, handler.order().stream().filter(s -> s.startsWith("B#")).toList());
            log.info("[ordering] A/B 各 50 事件同聚合严格有序，消费交错序列前10项={}",
                    handler.order().subList(0, Math.min(10, handler.order().size())));
            assertEquals(100, handler.successCount());
        }
    }

    @Test
    void duplicateDeliveryAfterReplayAndRestartAppliesSideEffectOnce() {
        RecordingEventHandler handler = new RecordingEventHandler();
        try (OutboxEventPipeline pipeline = Pipelines.create(handler, 1)) {
            pipeline.start();
            pipeline.enqueue("agg", "evt-1", "T", "hello");
            pipeline.awaitQuiescence(2000);
            assertEquals(1, handler.successCount());
            assertEquals(1L, pipeline.offsets().committed("test-group", "agg", 0L),
                    "事件在 0 号位，成功后位点（下一个待处理下标）推进到 1");

            // 位点回放：同一事件被再次投递
            pipeline.offsets().seek("test-group", "agg", 0L,
                    pipeline.broker().earliestOffset("agg"),
                    pipeline.broker().latestOffset("agg"));
            pipeline.restart();
            pipeline.awaitQuiescence(2000);

            log.info("[idempotency] eventId=evt-1: successCount={} invocationCount={} (副作用只允许生效一次)",
                    handler.successCount(), handler.invocationCount());
            assertEquals(1, handler.successCount(), "重复投递不得重复产生副作用");
            IdempotencyStore.Record rec = pipeline.idempotency().find("evt-1").orElseThrow();
            log.info("[idempotency] terminal record: eventId={} state={} offset={}",
                    rec.eventId(), rec.state(), rec.aggregateOffset());
            assertEquals(IdempotencyStore.ResultState.PROCESSED, rec.state());
        }
    }

    @Test
    void duplicateEventIdIsRejectedNotSilentlyDropped() {
        RecordingEventHandler handler = new RecordingEventHandler();
        try (OutboxEventPipeline pipeline = Pipelines.create(handler, 1)) {
            pipeline.start();
            pipeline.enqueue("agg", "dup", "T", "first");
            pipeline.awaitQuiescence(2000);
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> pipeline.enqueue("agg", "dup", "T", "second"));
            log.info("[idempotency] duplicate eventId rejected: {}", ex.getMessage());
            assertTrue(ex.getMessage().contains("duplicate eventId"));
            assertEquals(1, handler.successCount());
        }
    }
}
