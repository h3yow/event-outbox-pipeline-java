package com.github.h3yow.eventoutboxpipelinejava.consumer;

import java.util.Collection;
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
 *
 * <p>同一分区上不同聚合并行推进时，较早位点可能晚于较晚位点结束。
 * 已彻底结束（成功 / 死信 / 重复跳过）的位点先记入 {@code terminalOffsets}，
 * 由消费端在分区锁内把连续前缀折叠进 committedOffset，保证位点依旧齐平。
 *
 * <p>每次显式回放（seek）会使分区 generation 自增；在途任务以派发时的代际号
 * 判定自己是否已过期，回放后不允许旧任务再结束位点。
 */
public class ConsumerGroupState {

    private final ConcurrentHashMap<String, AtomicLong> committed = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
    private final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, Set<Long>> terminalOffsets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> generations = new ConcurrentHashMap<>();

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

    // ---- 终态位点（并行推进后用于折叠连续前缀） ----

    public void markTerminalOffset(String partition, long offset) {
        terminalOffsets.computeIfAbsent(partition, k -> ConcurrentHashMap.newKeySet()).add(offset);
    }

    public boolean isTerminalOffset(String partition, long offset) {
        Set<Long> offsets = terminalOffsets.get(partition);
        return offsets != null && offsets.contains(offset);
    }

    /** 折叠连续前缀时，消费掉单个已终态位点。 */
    public void removeTerminalOffset(String partition, long offset) {
        Set<Long> offsets = terminalOffsets.get(partition);
        if (offsets != null) {
            offsets.remove(offset);
        }
    }

    /**
     * 回放时丢弃不小于 fromOffset 的全部终态标记（这些位点将被重新投递）。
     */
    public void removeTerminalOffsetsFrom(String partition, long fromOffset) {
        Set<Long> offsets = terminalOffsets.get(partition);
        if (offsets != null) {
            offsets.removeIf(o -> o >= fromOffset);
        }
    }

    // ---- 分区代际（回放后旧任务失效） ----

    public long generation(String partition) {
        AtomicLong value = generations.get(partition);
        return value == null ? 0 : value.get();
    }

    public long bumpGeneration(String partition) {
        return generations.computeIfAbsent(partition, k -> new AtomicLong(0)).incrementAndGet();
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

    /**
     * 日志前缀被物理清理后，丢弃对应事件的幂等标记与终态位点。
     * 仅可用于已彻底结束并被截断的事件；未截断区间的标记必须保留
     * （重复投递/回放后副作用依旧只能生效一次）。
     */
    public void pruneProcessed(Collection<String> eventIds) {
        processedEventIds.removeAll(eventIds);
    }

    public void clear() {
        committed.clear();
        attempts.clear();
        processedEventIds.clear();
        terminalOffsets.clear();
        generations.clear();
    }
}
