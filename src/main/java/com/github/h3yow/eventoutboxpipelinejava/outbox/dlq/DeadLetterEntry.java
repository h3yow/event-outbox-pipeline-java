package com.github.h3yow.eventoutboxpipelinejava.outbox.dlq;

import com.github.h3yow.eventoutboxpipelinejava.outbox.core.DeadLetterState;
import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEvent;

import java.time.Instant;

/**
 * 死信区条目：保留原始事件、失败原因、重试痕迹与状态。
 */
public record DeadLetterEntry(
        OutboxEvent event,
        String consumerGroup,
        int attempts,
        String lastFailureReason,
        String lastFailureType,
        Instant failedAt,
        DeadLetterState state) {
}
