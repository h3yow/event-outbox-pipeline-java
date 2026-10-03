package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.pipeline.EventPipeline;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * event.outbox.retention-interval 绑定：容器启动后台清理线程，
 * 事件全部终态后无需手动调用，logStart 会自动向前推进。
 */
@SpringBootTest
@TestPropertySource(properties = {
        "event.outbox.partition-count=1",
        "event.outbox.worker-count=2",
        "event.outbox.retention-interval=50ms",
        "event.outbox.auto-start=true"
})
class RetentionAutoConfigurationTests {

    static final List<String> SEEN = new CopyOnWriteArrayList<>();

    @TestConfiguration
    static class HandlerConfig {
        @Bean
        com.github.h3yow.eventoutboxpipelinejava.handler.EventHandler handler() {
            return event -> SEEN.add(event.eventId());
        }
    }

    @Autowired
    EventPipeline pipeline;

    @Test
    void backgroundRetentionAdvancesLogStartAutomatically() throws Exception {
        for (int i = 0; i < 10; i++) {
            pipeline.enqueue("spring-retention-agg", "T", null);
        }
        assertTrue(pipeline.awaitProcessed(10_000));

        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline
                && pipeline.logStartOffset("p0") < pipeline.logEndOffset("p0")) {
            Thread.sleep(20);
        }
        assertTrue(pipeline.logStartOffset("p0") >= pipeline.logEndOffset("p0"),
                "background retention reclaimed the fully consumed log: start="
                        + pipeline.logStartOffset("p0") + ", end=" + pipeline.logEndOffset("p0"));
    }
}
