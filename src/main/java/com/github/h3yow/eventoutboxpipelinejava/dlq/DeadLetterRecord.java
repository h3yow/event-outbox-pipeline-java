package com.github.h3yow.eventoutboxpipelinejava.dlq;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;

/**
 * 死信记录：保留事件、最终位点、已尝试次数与可区分的失败原因。
 */
public record DeadLetterRecord(
        EventEnvelope event,
        String partition,
        long offset,
        int attempts,
        String reason,
        long deadLetteredEpochMillis
) {
}
