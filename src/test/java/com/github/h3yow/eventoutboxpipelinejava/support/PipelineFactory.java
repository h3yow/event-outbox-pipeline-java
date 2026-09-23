package com.github.h3yow.eventoutboxpipelinejava.support;

import com.github.h3yow.eventoutboxpipelinejava.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.consumer.ConsumerGroupState;
import com.github.h3yow.eventoutboxpipelinejava.dlq.InMemoryDeadLetterStore;
import com.github.h3yow.eventoutboxpipelinejava.pipeline.EventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.retry.RetryPolicy;

import java.time.Duration;

/**
 * 测试用管道构建工具（轮询间隔短、退避可控）。
 */
public final class PipelineFactory {

    private PipelineFactory() {
    }

    public static TestHarness newHarness(int partitions, int workers,
                                         int maxAttempts, Duration backoff) {
        InMemoryEventBroker broker = new InMemoryEventBroker(partitions);
        ConsumerGroupState state = new ConsumerGroupState();
        InMemoryDeadLetterStore dlq = new InMemoryDeadLetterStore();
        RecordingHandler handler = new RecordingHandler();
        RetryPolicy policy = new RetryPolicy(maxAttempts, backoff, backoff.multipliedBy(4));
        // 轮询间隔 2ms，加速测试
        EventPipeline pipeline = new EventPipeline(broker, state, dlq,
                handler, policy, workers, 2L);
        return new TestHarness(broker, state, dlq, handler, policy, pipeline);
    }

    public record TestHarness(
            InMemoryEventBroker broker,
            ConsumerGroupState state,
            InMemoryDeadLetterStore dlq,
            RecordingHandler handler,
            RetryPolicy policy,
            EventPipeline pipeline) {
    }
}
