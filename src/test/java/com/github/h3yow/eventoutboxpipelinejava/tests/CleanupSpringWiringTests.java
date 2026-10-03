package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.cleanup.EventLogCleaner;
import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.pipeline.EventPipeline;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 后台清理的 Spring 装配：event.outbox.cleanup.enabled=true 时
 * {@link EventLogCleaner} Bean 存在并随管道在后台运行。
 */
@SpringBootTest
@TestPropertySource(properties = {
        "event.outbox.partition-count=2",
        "event.outbox.worker-count=2",
        "event.outbox.cleanup.enabled=true",
        "event.outbox.cleanup.interval=30ms",
        "event.outbox.cleanup.batch-size=1",
        "event.outbox.auto-start=true"
})
class CleanupSpringWiringTests {

    @Autowired EventPipeline pipeline;
    @Autowired InMemoryEventBroker broker;
    @Autowired(required = false) EventLogCleaner cleaner;

    @Test
    void cleanerBeanWiredAndRunsInBackground() throws Exception {
        assertNotNull(cleaner, "cleaner bean must be present when cleanup.enabled=true");

        // 在两个分区各放若干不同聚合事件，全部成功
        for (int i = 0; i < 8; i++) {
            pipeline.enqueue("agg-clean-" + i, "T", null);
        }
        assertTrue(pipeline.awaitProcessed(10000));

        // 后台清理线程 30ms 间隔、每轮每分区删 1 条：最终任一分区起始位点应前移
        long deadline = System.currentTimeMillis() + 5000;
        boolean anyTruncated = false;
        while (System.currentTimeMillis() < deadline && !anyTruncated) {
            for (String p : broker.partitions()) {
                if (broker.logStartOffset(p) > 0) {
                    anyTruncated = true;
                    break;
                }
            }
            Thread.sleep(20);
        }
        assertTrue(anyTruncated, "background cleaner truncates finished prefix without manual call");

        // 清理期间持续入队消费，确认后台清理不阻断管道
        EventEnvelope more = pipeline.enqueue("agg-clean-more", "T", null);
        assertTrue(pipeline.awaitProcessed(5000));
        assertTrue(pipeline.isProcessed(more.eventId()));
        assertTrue(pipeline.currentAggregateState("agg-clean-more").isPresent());
    }
}
