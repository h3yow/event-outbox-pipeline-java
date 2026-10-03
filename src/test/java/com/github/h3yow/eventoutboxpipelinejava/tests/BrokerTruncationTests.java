package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Broker 分段截断的物理回收边界：整段落入截断区间时必须真正释放，
 * 段边界位点截断后读取/写入仍然正确。
 */
class BrokerTruncationTests {

    private static EventEnvelope event(long id) {
        return new EventEnvelope("e-" + id, "agg", "T", new byte[0], id);
    }

    @Test
    void segmentBoundaryTrimsReclaimFullSegments() {
        InMemoryEventBroker broker = new InMemoryEventBroker(1);
        for (long i = 0; i < 600; i++) {
            broker.publish(event(i));
        }
        // 截断到精确段边界 512：前两段（512 条）整段释放
        broker.trimPrefix("p0", 512);
        assertEquals(512, broker.logStartOffset("p0"));
        assertEquals(600, broker.logEndOffset("p0"));
        assertEquals(88, broker.size("p0"), "retained count is end - start");
        assertFalse(broker.read("p0", 511).isPresent());
        assertTrue(broker.read("p0", 512).isPresent());
        assertTrue(broker.read("p0", 599).isPresent());

        // 截断后继续追加，位点连续、读取正确
        for (long i = 600; i < 800; i++) {
            broker.publish(event(i));
        }
        assertEquals(800, broker.logEndOffset("p0"));
        assertEquals("e-799", broker.read("p0", 799).orElseThrow().eventId());
        broker.trimPrefix("p0", 768); // 再次落在边界：包含 512、768 前的整段全部回收
        assertEquals(768, broker.logStartOffset("p0"));
        assertEquals(32, broker.size("p0"));
        assertFalse(broker.read("p0", 767).isPresent());
        assertTrue(broker.read("p0", 768).isPresent());
    }

    @Test
    void midSegmentTrimsKeepBoundarySegment() {
        InMemoryEventBroker broker = new InMemoryEventBroker(1);
        for (long i = 0; i < 600; i++) {
            broker.publish(event(i));
        }
        broker.trimPrefix("p0", 300);
        assertEquals(300, broker.logStartOffset("p0"));
        assertEquals(300, broker.size("p0"));
        assertFalse(broker.read("p0", 299).isPresent());
        assertEquals("e-300", broker.read("p0", 300).orElseThrow().eventId());
        // 非边界截断幂等
        assertEquals(300, broker.trimPrefix("p0", 300));
    }
}
