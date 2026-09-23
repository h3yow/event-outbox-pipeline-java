package com.github.h3yow.eventoutboxpipelinejava.outbox.core;

/**
 * 不可变的事件信封。
 *
 * <p>{@code sequence} 为同一聚合内的稳定递增序号（投递顺序的依据），
 * {@code eventId} 为全局唯一标识（幂等去重的依据）。
 */
public record OutboxEvent(
        long sequence,
        String aggregateId,
        String eventId,
        String type,
        String payload) {

    public OutboxEvent {
        if (aggregateId == null || aggregateId.isBlank()) {
            throw new IllegalArgumentException("aggregateId must not be blank");
        }
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence must be >= 0");
        }
    }
}
