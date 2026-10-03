package com.github.h3yow.eventoutboxpipelinejava.dlq;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内死信区：线程安全，可按事件 ID 与（分区,位点）检索，记录保留直到显式移除。
 */
public class InMemoryDeadLetterStore implements DeadLetterStore {

    private final ConcurrentHashMap<String, DeadLetterRecord> byEventId = new ConcurrentHashMap<>();

    @Override
    public void put(DeadLetterRecord record) {
        byEventId.put(record.event().eventId(), record);
    }

    @Override
    public Optional<DeadLetterRecord> findByEventId(String eventId) {
        return Optional.ofNullable(byEventId.get(eventId));
    }

    @Override
    public List<DeadLetterRecord> findAll() {
        List<DeadLetterRecord> records = new ArrayList<>(byEventId.values());
        records.sort(Comparator
                .comparing(DeadLetterRecord::partition)
                .thenComparingLong(DeadLetterRecord::offset));
        return List.copyOf(records);
    }

    @Override
    public void remove(String eventId) {
        byEventId.remove(eventId);
    }

    @Override
    public int size() {
        return byEventId.size();
    }

    @Override
    public void clear() {
        byEventId.clear();
    }

    public boolean containsLocation(String partition, long offset) {
        for (DeadLetterRecord record : byEventId.values()) {
            if (record.partition().equals(partition) && record.offset() == offset) {
                return true;
            }
        }
        return false;
    }

    public Optional<DeadLetterRecord> findByLocation(String partition, long offset) {
        for (DeadLetterRecord record : byEventId.values()) {
            if (record.partition().equals(partition) && record.offset() == offset) {
                return Optional.of(record);
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<Long> earliestOffset(String partition) {
        Long earliest = null;
        for (DeadLetterRecord record : byEventId.values()) {
            if (record.partition().equals(partition)
                    && (earliest == null || record.offset() < earliest)) {
                earliest = record.offset();
            }
        }
        return Optional.ofNullable(earliest);
    }
}
