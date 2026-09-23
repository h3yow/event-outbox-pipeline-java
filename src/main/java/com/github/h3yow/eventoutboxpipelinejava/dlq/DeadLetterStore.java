package com.github.h3yow.eventoutboxpipelinejava.dlq;

import java.util.List;
import java.util.Optional;

/**
 * 可检索的死信区。
 */
public interface DeadLetterStore {

    void put(DeadLetterRecord record);

    Optional<DeadLetterRecord> findByEventId(String eventId);

    List<DeadLetterRecord> findAll();

    void remove(String eventId);

    int size();

    void clear();
}
