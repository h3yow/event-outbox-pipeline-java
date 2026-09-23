package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.outbox.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.outbox.dlq.DeadLetterQueue;
import com.github.h3yow.eventoutboxpipelinejava.outbox.handler.EventHandler;
import com.github.h3yow.eventoutboxpipelinejava.outbox.idempotency.IdempotencyStore;
import com.github.h3yow.eventoutboxpipelinejava.outbox.offset.OffsetStore;
import com.github.h3yow.eventoutboxpipelinejava.outbox.retry.BackoffStrategy;
import com.github.h3yow.eventoutboxpipelinejava.outbox.retry.RetryPolicy;

import java.time.Duration;

/** 测试用流水线构造助手。 */
final class Pipelines {

    private Pipelines() {
    }

    static OutboxEventPipeline create(EventHandler handler, int maxAttempts) {
        return create(handler, maxAttempts, 20L, 4);
    }

    static OutboxEventPipeline create(EventHandler handler, int maxAttempts,
                                      long fixedBackoffMillis, int workerThreads) {
        InMemoryEventBroker broker = new InMemoryEventBroker();
        OffsetStore offsets = new OffsetStore();
        DeadLetterQueue dlq = new DeadLetterQueue();
        IdempotencyStore idempotency = new IdempotencyStore();
        RetryPolicy policy = new RetryPolicy(maxAttempts,
                BackoffStrategy.fixed(Duration.ofMillis(fixedBackoffMillis)));
        return new OutboxEventPipeline(broker, offsets, dlq, idempotency,
                handler, policy, workerThreads, "test-group", 50L);
    }
}
