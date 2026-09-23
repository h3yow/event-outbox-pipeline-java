package com.github.h3yow.eventoutboxpipelinejava.broker;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内消息代理模拟：分区追加日志。同一 aggregateKey 稳定映射到同一分区，
 * 分区内严格按位点顺序读取；日志只追加、不删除，事件不会静默丢失。
 */
public class InMemoryEventBroker {

    private static final class PartitionLog {
        private final List<EventEnvelope> events = new ArrayList<>();

        synchronized Location append(EventEnvelope event) {
            events.add(event);
            return new Location("", events.size() - 1L);
        }

        synchronized Optional<EventEnvelope> read(long offset) {
            if (offset < 0 || offset >= events.size()) {
                return Optional.empty();
            }
            return Optional.of(events.get((int) offset));
        }

        synchronized long endOffset() {
            return events.size();
        }

        synchronized int size() {
            return events.size();
        }
    }

    private final int partitionCount;
    private final List<String> partitionNames;
    private final ConcurrentHashMap<String, PartitionLog> logs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Location> eventIndex = new ConcurrentHashMap<>();

    public InMemoryEventBroker(int partitionCount) {
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be >= 1");
        }
        this.partitionCount = partitionCount;
        List<String> names = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            names.add("p" + i);
        }
        this.partitionNames = Collections.unmodifiableList(names);
    }

    public List<String> partitions() {
        return partitionNames;
    }

    public String partitionOf(String aggregateKey) {
        int hash = aggregateKey.hashCode();
        int index = Math.floorMod(hash, partitionCount);
        return partitionNames.get(index);
    }

    /**
     * 追加事件。eventId 幂等：重复入队返回既有位置，不产生重复消息。
     */
    public Location publish(EventEnvelope event) {
        String partition = partitionOf(event.aggregateKey());
        Location existing = eventIndex.get(event.eventId());
        if (existing != null) {
            return existing;
        }
        PartitionLog log = logs.computeIfAbsent(partition, k -> new PartitionLog());
        synchronized (log) {
            Location raced = eventIndex.get(event.eventId());
            if (raced != null) {
                return raced;
            }
            long offset = log.events.size();
            log.events.add(event);
            Location location = new Location(partition, offset);
            eventIndex.put(event.eventId(), location);
            return location;
        }
    }

    public Optional<EventEnvelope> read(String partition, long offset) {
        PartitionLog log = logs.get(partition);
        return log == null ? Optional.empty() : log.read(offset);
    }

    public long logEndOffset(String partition) {
        PartitionLog log = logs.get(partition);
        return log == null ? 0 : log.endOffset();
    }

    public long logStartOffset(String partition) {
        if (!partitionNames.contains(partition)) {
            throw new IllegalArgumentException("unknown partition: " + partition);
        }
        // 日志不做截断，起始位点恒为 0（事件永不丢失）
        return 0;
    }

    public Optional<Location> locate(String eventId) {
        return Optional.ofNullable(eventIndex.get(eventId));
    }

    public int size(String partition) {
        PartitionLog log = logs.get(partition);
        return log == null ? 0 : log.size();
    }

    /**
     * 事件物理位置：分区 + 位点。
     */
    public record Location(String partition, long offset) {
    }
}
