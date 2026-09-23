package com.github.h3yow.eventoutboxpipelinejava.outbox.dlq;

import com.github.h3yow.eventoutboxpipelinejava.outbox.core.DeadLetterState;
import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEvent;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 可检索的死信区：按 eventId 唯一保存，任何情况下事件都不会静默丢弃。
 *
 * <p>条目保留原始事件、消费者组、累计尝试次数、最后一次失败的类型与原因、
 * 发生时间以及生命周期状态（OPEN / REPLAYED / DISCARDED）。
 */
public final class DeadLetterQueue {

    private final ConcurrentMap<String, DeadLetterEntry> entries = new ConcurrentHashMap<>();
    private final Clock clock;

    public DeadLetterQueue() {
        this(Clock.systemUTC());
    }

    DeadLetterQueue(Clock clock) {
        this.clock = clock;
    }

    /**
     * 事件转入死信区。
     *
     * <p>按 eventId 幂等：若该事件已在死信区则保留原条目原样返回（绝不重复登记、
     * 绝不覆盖失败原因），保证“成功与死信不会同时发生”之外，死信登记本身也不重复。
     */
    public DeadLetterEntry send(OutboxEvent event, String consumerGroup, int attempts, Throwable failure) {
        String reason = failure == null ? "unknown"
                : (failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage());
        String failureType = failure == null ? "Unknown" : failure.getClass().getName();
        DeadLetterEntry created = new DeadLetterEntry(
                event, consumerGroup, attempts, reason, failureType,
                Instant.now(clock), DeadLetterState.OPEN);
        DeadLetterEntry existing = entries.putIfAbsent(event.eventId(), created);
        return existing != null ? existing : created;
    }

    public Optional<DeadLetterEntry> findByEventId(String eventId) {
        return Optional.ofNullable(entries.get(eventId));
    }

    public List<DeadLetterEntry> findAll() {
        return entries.values().stream()
                .sorted(Comparator
                        .comparing((DeadLetterEntry e) -> e.event().aggregateId())
                        .thenComparingLong(e -> e.event().sequence()))
                .toList();
    }

    public List<DeadLetterEntry> findByAggregate(String aggregateId) {
        return entries.values().stream()
                .filter(e -> e.event().aggregateId().equals(aggregateId))
                .sorted(Comparator.comparingLong(e -> e.event().sequence()))
                .toList();
    }

    public List<DeadLetterEntry> findByState(DeadLetterState state) {
        return entries.values().stream()
                .filter(e -> e.state() == state)
                .sorted(Comparator
                        .comparing((DeadLetterEntry e) -> e.event().aggregateId())
                        .thenComparingLong(e -> e.event().sequence()))
                .toList();
    }

    /** 更新条目状态（如重放后标记 REPLAYED），返回更新后的条目；不存在则抛异常。 */
    public synchronized DeadLetterEntry markState(String eventId, DeadLetterState state) {
        DeadLetterEntry current = entries.get(eventId);
        if (current == null) {
            throw new IllegalArgumentException("dead letter entry not found: " + eventId);
        }
        if (current.state() == state) {
            return current;
        }
        DeadLetterEntry updated = new DeadLetterEntry(
                current.event(), current.consumerGroup(), current.attempts(),
                current.lastFailureReason(), current.lastFailureType(),
                current.failedAt(), state);
        entries.put(eventId, updated);
        return updated;
    }

    public int size() {
        return entries.size();
    }
}
