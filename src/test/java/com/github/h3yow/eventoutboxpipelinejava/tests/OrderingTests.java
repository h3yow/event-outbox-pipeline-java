package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory;
import com.github.h3yow.eventoutboxpipelinejava.support.PipelineFactory.TestHarness;
import com.github.h3yow.eventoutboxpipelinejava.support.RecordingHandler;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 同聚合事件必须按稳定顺序消费；位点最终推进到日志末尾，无丢失。
 */
class OrderingTests {

    @Test
    void sameAggregateConsumedInOffsetOrder() throws Exception {
        TestHarness harness = PipelineFactory.newHarness(4, 4, 1, Duration.ZERO);
        var pipeline = harness.pipeline();
        String aggregate = "order-123";
        List<String> enqueued = new ArrayList<>();

        pipeline.start();
        for (int i = 0; i < 20; i++) {
            EventEnvelope e = pipeline.enqueue(aggregate, "OrderChanged", ("v" + i).getBytes());
            enqueued.add(e.eventId());
        }
        assertTrue(pipeline.awaitProcessed(5000), "all events should be processed");
        pipeline.stop();

        RecordingHandler handler = harness.handler();
        // 同一聚合 -> 同一分区 -> 严格按入队顺序
        assertEquals(enqueued, handler.handledOrder(),
                "events of same aggregate must be consumed in enqueue order");
        for (String id : enqueued) {
            assertEquals(1, handler.callCount(id), "no duplicates on normal delivery");
        }
        String partition = harness.broker().partitionOf(aggregate);
        assertEquals(20, harness.broker().size(partition));
        assertEquals(20, pipeline.committedOffset(partition),
                "committed offset reaches log end, nothing silently dropped");
        assertEquals(0, harness.dlq().size());
    }

    @Test
    void aggregateKeysStablyMapToPartitions() {
        TestHarness harness = PipelineFactory.newHarness(4, 1, 1, Duration.ZERO);
        for (String key : List.of("a", "b", "c", "order-1", "order-2")) {
            String first = harness.broker().partitionOf(key);
            for (int i = 0; i < 50; i++) {
                assertEquals(first, harness.broker().partitionOf(key),
                        "partition routing must be stable for " + key);
            }
        }
    }
}
