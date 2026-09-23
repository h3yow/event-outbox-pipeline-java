package com.github.h3yow.eventoutboxpipelinejava.consumer;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消费组状态（进程内模拟）：每个分区的已提交位点、各事件重试计数、
 * 已成功处理的事件 ID 集合（幂等依据）。所有方法线程安全。
 *
 * <p>位点语义：committedOffset 表示“下一个待处理位点”。初始为 0。
 */
public class ConsumerGroupState {

    private final ConcurrentHashMap<String, AtomicLong> committed = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
    private final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    private static String attemptKey(String partition, long offset) {
        return partition + "#" + offset;
    }

    public long committedOffset(String partition) {
        AtomicLong value = committed.get(partition);
        return value == null ? 0 : value.get();
    }

    /**
     * 位点只能单调推进；回退（回放）必须走 {@link #seek}。
     */
    public void commit(String partition, long nextOffset) {
        committed.compute(partition, (k, current) -> {
            AtomicLong value = current == null ? new AtomicLong(0) : current;
            long prev;
            do {
                prev = value.get();
                if (nextOffset < prev) {
                    throw new IllegalArgumentException(
                            "offset cannot move backwards: partition=" + partition
                                    + ", current=" + prev + ", requested=" + nextOffset);
                }
            } while (!value.compareAndSet(prev, nextOffset));
            return value;
        });
    }

    /**
     * 位点回放：直接回退（越界/死信校验在消费者层完成）。
     */
    public void seek(String partition, long offset) {
        committed.computeIfAbsent(partition, k -> new AtomicLong(0)).set(offset);
    }

    public int getAttempts(String partition, long offset) {
        AtomicInteger value = attempts.get(attemptKey(partition, offset));
        return value == null ? 0 : value.get();
    }

    public int incrementAttempts(String partition, long offset) {
        return attempts.computeIfAbsent(attemptKey(partition, offset),
                k -> new AtomicInteger(0)).incrementAndGet();
    }

    public void resetAttempts(String partition, long offset) {
        attempts.remove(attemptKey(partition, offset));
    }

    /**
     * 位点回放时，重置该分区不小于 fromOffset 的所有重试计数（回放后重新获得完整重试预算）。
     */
    public void resetAttemptsFrom(String partition, long fromOffset) {
        String prefix = partition + "#";
        attempts.keySet().removeIf(key -> {
            if (!key.startsWith(prefix)) {
                return false;
            }
            try {
                return Long.parseLong(key.substring(prefix.length())) >= fromOffset;
            } catch (NumberFormatException e) {
                return false;
            }
        });
    }

    /**
     * 标记事件已成功处理；仅首次返回 true（业务副作用生效一次的依据）。
     * 即使位点回退重投同一事件，也只会返回一次 true。
     */
    public boolean markProcessed(String eventId) {
        return processedEventIds.add(eventId);
    }

    public boolean isProcessed(String eventId) {
        return processedEventIds.contains(eventId);
    }

    public Set<String> processedEventIds() {
        return Collections.unmodifiableSet(processedEventIds);
    }

    public void clear() {
        committed.clear();
        attempts.clear();
        processedEventIds.clear();
    }
}
