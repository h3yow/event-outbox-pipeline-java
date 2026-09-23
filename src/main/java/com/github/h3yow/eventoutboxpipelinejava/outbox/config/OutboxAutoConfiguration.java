package com.github.h3yow.eventoutboxpipelinejava.outbox.config;

import com.github.h3yow.eventoutboxpipelinejava.outbox.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.outbox.dlq.DeadLetterQueue;
import com.github.h3yow.eventoutboxpipelinejava.outbox.handler.EventHandler;
import com.github.h3yow.eventoutboxpipelinejava.outbox.handler.RecordingEventHandler;
import com.github.h3yow.eventoutboxpipelinejava.outbox.idempotency.IdempotencyStore;
import com.github.h3yow.eventoutboxpipelinejava.outbox.offset.OffsetStore;
import com.github.h3yow.eventoutboxpipelinejava.outbox.retry.BackoffStrategy;
import com.github.h3yow.eventoutboxpipelinejava.outbox.retry.RetryPolicy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 本地事件流水线的 Spring 装配：消息代理、位点、死信区、幂等存储均为进程内 Bean。
 *
 * <p>{@link RecordingEventHandler} 同时以具体类型与 {@link EventHandler} 接口暴露：
 * 前者供 Web 层查询生效过的副作用，后者供流水线执行业务。
 */
@Configuration
@EnableConfigurationProperties(PipelineProperties.class)
public class OutboxAutoConfiguration {

    @Bean
    public InMemoryEventBroker inMemoryEventBroker() {
        return new InMemoryEventBroker();
    }

    @Bean
    public OffsetStore offsetStore() {
        return new OffsetStore();
    }

    @Bean
    public DeadLetterQueue deadLetterQueue() {
        return new DeadLetterQueue();
    }

    @Bean
    public IdempotencyStore idempotencyStore() {
        return new IdempotencyStore();
    }

    @Bean
    public RecordingEventHandler recordingEventHandler() {
        return new RecordingEventHandler();
    }

    @Bean
    @org.springframework.context.annotation.Primary
    public EventHandler eventHandler(RecordingEventHandler recordingEventHandler) {
        return recordingEventHandler;
    }

    @Bean
    public RetryPolicy retryPolicy(PipelineProperties props) {
        BackoffStrategy backoff = BackoffStrategy.exponential(
                props.getInitialBackoff(),
                props.getBackoffMultiplier(),
                props.getMaxBackoff());
        return new RetryPolicy(props.getMaxAttempts(), backoff);
    }

    @Bean(destroyMethod = "close")
    public OutboxEventPipeline outboxEventPipeline(InMemoryEventBroker broker,
                                                   OffsetStore offsets,
                                                   DeadLetterQueue deadLetters,
                                                   IdempotencyStore idempotency,
                                                   EventHandler handler,
                                                   RetryPolicy retryPolicy,
                                                   PipelineProperties props) {
        OutboxEventPipeline pipeline = new OutboxEventPipeline(
                broker, offsets, deadLetters, idempotency, handler, retryPolicy,
                props.getWorkerThreads(), props.getConsumerGroup(),
                props.getPollInterval().toMillis());
        pipeline.start();
        return pipeline;
    }
}
