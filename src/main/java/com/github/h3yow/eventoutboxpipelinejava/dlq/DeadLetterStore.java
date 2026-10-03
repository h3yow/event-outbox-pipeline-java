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

    /**
     * 返回指定分区中最早（位点最小）的死信位点；无死信时为空。
     * 保留清理据此保证绝不截断到死信事件或越过它。
     */
    java.util.Optional<Long> earliestOffset(String partition);
}
