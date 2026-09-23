package com.github.h3yow.eventoutboxpipelinejava.domain;

/**
 * 不可变事件信封。eventId 全局唯一，用于幂等去重；aggregateKey 决定分区与顺序。
 */
public record EventEnvelope(
        String eventId,
        String aggregateKey,
        String type,
        byte[] payload,
        long enqueueEpochMillis
) {
    public EventEnvelope {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (aggregateKey == null || aggregateKey.isBlank()) {
            throw new IllegalArgumentException("aggregateKey must not be blank");
        }
        payload = payload == null ? new byte[0] : payload.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
