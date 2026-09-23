package com.github.h3yow.eventoutboxpipelinejava.outbox.broker;

import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 进程内消息代理：按聚合（分区）维护顺序日志，同一聚合的事件按入队顺序读取。
 *
 * <p>位点以“日志下标（sequence）”表示；日志可发生截断（{@link #truncateBefore}），
 * 截断后旧下标永久失效，由位点管理以 EXPIRED 原因明确拒绝。
 *
 * <p>线程安全：追加在全局锁下分配连续序号；读取与截断在各自分区的读写锁下进行。
 */
public final class InMemoryEventBroker {

    private static final class Partition {
        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        private final List<OutboxEvent> events = new ArrayList<>();
        private volatile long earliest = 0L;
    }

    private final Map<String, Partition> partitions = new ConcurrentHashMap<>();
    private final Map<String, String> eventIdToAggregate = new ConcurrentHashMap<>();
    /** 序号分配锁：保证每个聚合的 sequence 连续且可见顺序确定。 */
    private final Object appendLock = new Object();

    private Partition partition(String aggregateId) {
        return partitions.computeIfAbsent(aggregateId, k -> new Partition());
    }

    /**
     * 追加事件到指定聚合日志末尾。
     *
     * @throws IllegalArgumentException aggregateId/eventId 为空白
     * @throws IllegalStateException    eventId 已存在（重复入队被明确拒绝）
     */
    public OutboxEvent append(String aggregateId, String eventId, String type, String payload) {
        if (aggregateId == null || aggregateId.isBlank()) {
            throw new IllegalArgumentException("aggregateId must not be blank");
        }
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (eventIdToAggregate.putIfAbsent(eventId, aggregateId) != null) {
            throw new IllegalStateException("duplicate eventId: " + eventId);
        }
        Partition p = partition(aggregateId);
        final long sequence;
        synchronized (appendLock) {
            p.lock.writeLock().lock();
            try {
                sequence = p.earliest + p.events.size();
                OutboxEvent event = new OutboxEvent(sequence, aggregateId, eventId, type, payload);
                p.events.add(event);
                return event;
            } finally {
                p.lock.writeLock().unlock();
            }
        }
    }

    /** 读取下标 {@code offset} 处的事件；为负或已截断抛 {@link IllegalArgumentException}，尚未写入返回 empty。 */
    public Optional<OutboxEvent> fetch(String aggregateId, long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0: " + offset);
        }
        Partition p = partitions.get(aggregateId);
        if (p == null) {
            return Optional.empty();
        }
        p.lock.readLock().lock();
        try {
            if (offset < p.earliest) {
                throw new IllegalArgumentException(
                        "offset " + offset + " expired, earliest=" + p.earliest
                                + ", aggregate=" + aggregateId);
            }
            int index = (int) (offset - p.earliest);
            if (index >= p.events.size()) {
                return Optional.empty();
            }
            return Optional.of(p.events.get(index));
        } finally {
            p.lock.readLock().unlock();
        }
    }

    /** 当前日志最新位点（含），无事件时返回 earliest-1（空分区为 -1）。 */
    public long latestOffset(String aggregateId) {
        Partition p = partitions.get(aggregateId);
        if (p == null) {
            return -1L;
        }
        p.lock.readLock().lock();
        try {
            return p.earliest + p.events.size() - 1;
        } finally {
            p.lock.readLock().unlock();
        }
    }

    /** 当前最早可读位点（截断起始）。 */
    public long earliestOffset(String aggregateId) {
        Partition p = partitions.get(aggregateId);
        return p == null ? 0L : p.earliest;
    }

    /**
     * 截断下标 {@code offset} 之前的日志（保留 offset 及之后）。
     * 截断单调向前，旧位点随即永久失效。
     */
    public void truncateBefore(String aggregateId, long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0: " + offset);
        }
        Partition p = partition(aggregateId);
        p.lock.writeLock().lock();
        try {
            if (offset <= p.earliest) {
                return;
            }
            long latest = p.earliest + p.events.size() - 1;
            long target = Math.min(offset, latest + 1);
            int remove = (int) (target - p.earliest);
            if (remove > 0) {
                p.events.subList(0, Math.min(remove, p.events.size())).clear();
            }
            p.earliest = target;
        } finally {
            p.lock.writeLock().unlock();
        }
    }

    /** 全部聚合标识（快照，顺序不保证）。 */
    public List<String> aggregateIds() {
        return new ArrayList<>(partitions.keySet());
    }

    /** 判断 eventId 是否已经入队过。 */
    public boolean knowsEventId(String eventId) {
        return eventIdToAggregate.containsKey(eventId);
    }
}
