package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.consumer.ConsumerGroupState;
import com.github.h3yow.eventoutboxpipelinejava.dlq.DeadLetterStore;
import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.handler.EventHandler;
import com.github.h3yow.eventoutboxpipelinejava.pipeline.EventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.retry.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spring Boot 自动装配：自定义 EventHandler 优先生效，event.outbox.* 属性绑定，
 * auto-start=true 时容器启动即消费。
 */
@SpringBootTest
@TestPropertySource(properties = {
        "event.outbox.partition-count=3",
        "event.outbox.worker-count=3",
        "event.outbox.max-attempts=2",
        "event.outbox.auto-start=true"
})
class PipelineSpringWiringTests {

    static final List<String> SEEN = new CopyOnWriteArrayList<>();

    @TestConfiguration
    static class CustomHandlerConfig {
        @Bean
        EventHandler customEventHandler() {
            return event -> SEEN.add(event.eventId());
        }
    }

    @Autowired EventPipeline pipeline;
    @Autowired InMemoryEventBroker broker;
    @Autowired ConsumerGroupState state;
    @Autowired DeadLetterStore deadLetters;
    @Autowired RetryPolicy retryPolicy;

    @Test
    void beansWiredAndAutoConsumes() throws Exception {
        assertEquals(2, retryPolicy.maxAttempts());
        assertEquals(3, broker.partitions().size());

        EventEnvelope e = pipeline.enqueue("spring-agg", "SpringEvent", null);
        assertTrue(pipeline.awaitProcessed(10000), "auto-started pipeline consumes without explicit start()");
        assertTrue(SEEN.contains(e.eventId()));
        assertTrue(state.isProcessed(e.eventId()));
    }
}
