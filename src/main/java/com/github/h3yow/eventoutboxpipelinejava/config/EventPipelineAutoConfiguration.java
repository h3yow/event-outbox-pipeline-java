package com.github.h3yow.eventoutboxpipelinejava.config;

import com.github.h3yow.eventoutboxpipelinejava.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.cleanup.EventLogCleaner;
import com.github.h3yow.eventoutboxpipelinejava.consumer.ConsumerGroupState;
import com.github.h3yow.eventoutboxpipelinejava.dlq.DeadLetterStore;
import com.github.h3yow.eventoutboxpipelinejava.dlq.InMemoryDeadLetterStore;
import com.github.h3yow.eventoutboxpipelinejava.handler.EventHandler;
import com.github.h3yow.eventoutboxpipelinejava.pipeline.EventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.retry.RetryPolicy;
import com.github.h3yow.eventoutboxpipelinejava.state.AggregateStateStore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * 进程内事件管道的 Spring 装配。默认不自动启动消费（event.outbox.auto-start=false），
 * 由应用显式调用 {@link EventPipeline#start()}；置为 true 时 Bean 初始化后自动启动。
 * 容器关闭时统一停止消费与清理线程。
 */
@AutoConfiguration
@EnableConfigurationProperties(EventPipelineProperties.class)
public class EventPipelineAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public InMemoryEventBroker inMemoryEventBroker(EventPipelineProperties props) {
        return new InMemoryEventBroker(props.getPartitionCount());
    }

    @Bean
    @ConditionalOnMissingBean
    public ConsumerGroupState consumerGroupState() {
        return new ConsumerGroupState();
    }

    @Bean
    @ConditionalOnMissingBean
    public AggregateStateStore aggregateStateStore() {
        return new AggregateStateStore();
    }

    @Bean
    @ConditionalOnMissingBean
    public DeadLetterStore deadLetterStore() {
        return new InMemoryDeadLetterStore();
    }

    @Bean
    @ConditionalOnMissingBean
    public RetryPolicy retryPolicy(EventPipelineProperties props) {
        return new RetryPolicy(props.getMaxAttempts(),
                props.getInitialBackoff(), props.getMaxBackoff());
    }

    @Bean
    @ConditionalOnMissingBean(EventHandler.class)
    public EventHandler noopEventHandler() {
        return new NoopEventHandler();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "event.outbox.cleanup", name = "enabled", havingValue = "true")
    public EventLogCleaner eventLogCleaner(InMemoryEventBroker broker,
                                           ConsumerGroupState state,
                                           DeadLetterStore deadLetters,
                                           EventPipelineProperties props) {
        return new EventLogCleaner(broker, state, deadLetters,
                props.getCleanupInterval().toMillis(), props.getCleanupBatchSize());
    }

    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean
    public EventPipeline eventPipeline(InMemoryEventBroker broker,
                                       ConsumerGroupState state,
                                       DeadLetterStore deadLetters,
                                       AggregateStateStore aggregateState,
                                       ObjectProvider<EventLogCleaner> cleaner,
                                       EventHandler handler,
                                       RetryPolicy retryPolicy,
                                       EventPipelineProperties props) {
        EventPipeline pipeline = new EventPipeline(broker, state, deadLetters, aggregateState,
                cleaner.getIfAvailable(), handler, retryPolicy, props.getWorkerCount(), 5L);
        if (props.isAutoStart()) {
            pipeline.start();
        }
        return pipeline;
    }
}
