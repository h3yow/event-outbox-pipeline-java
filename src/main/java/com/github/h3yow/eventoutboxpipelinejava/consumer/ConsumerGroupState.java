package com.github.h3yow.eventoutboxpipelinejava.consumer;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消费组状态（进程内模拟）：每个分区的已提交水位线、各事件重试计数、
 * 已成功处理的事件 ID 集合（幂等依据）。所有方法线程安全。
 *
 * <p>位点语义：committedOffset 是“连续终态前缀”的水位线，即下一个待处理位点。
 * 不同聚合可乱序完成，单个事件终态（成功/死信/重复跳过）先记录到按序集合中，
 * 水位线只沿连续前缀推进——水位线之前的事件一定全部终态，与消费完成顺序无关。
 */
public class ConsumerGroupState {

    private final ConcurrentHashMap<String, AtomicLong> committed = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
    // 已终态但尚未并入连续前缀的位点（乱序完成的空洞之后的部分）；连续前缀在推进时移除
    private final ConcurrentHashMap<String, TreeSet<Long>> terminalOffsets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> partitionMonitors = new ConcurrentHashMap<>();
    private final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    private static String attemptKey(String partition, long offset) {
        return partition + "#" + offset;
    }

    private Object monitor(String partition) {
        return partitionMonitors.computeIfAbsent(partition, k -> new Object());
    }

    public long committedOffset(String partition) {
        AtomicLong value = committed.get(partition);
        return value == null ? 0 : value.get();
    }

    /**
     * 标记某事件已终态（成功处理 / 死信 / 重复跳过）。水位线沿连续终态前缀推进，
     * 支持不同聚合乱序完成。
     */
    public void markTerminal(String partition, long offset) {
        synchronized (monitor(partition)) {
            TreeSet<Long> done = terminalOffsets.computeIfAbsent(partition, k -> new TreeSet<>());
            done.add(offset);
            AtomicLong value = committed.computeIfAbsent(partition, k -> new AtomicLong(0));
            long watermark = value.get();
            while (done.remove(watermark)) {
                watermark++;
            }
            value.set(watermark);
        }
    }

    public boolean isTerminal(String partition, long offset) {
        if (offset < committedOffset(partition)) {
            return true;
        }
        TreeSet<Long> done = terminalOffsets.get(partition);
        return done != null && done.contains(offset);
    }

    /**
     * 位点回放：水位线重置到 offset，并丢弃该分区不小于 offset 的终态标记
     * （旧实现等价语义：回放区间内的事件被重新视为待处理）。
     */
    public void seek(String partition, long offset) {
        synchronized (monitor(partition)) {
            committed.computeIfAbsent(partition, k -> new AtomicLong(0)).set(offset);
            TreeSet<Long> done = terminalOffsets.get(partition);
            if (done != null) {
                done.tailSet(offset, true).clear();
            }
        }
    }

    /**
     * 清理截断区间 [0, newStart) 内遗留的重试计数与终态标记。
     * 水位线必须已经不小于 newStart（调用方负责保证安全点）。
     */
    public void pruneBefore(String partition, long newStart) {
        synchronized (monitor(partition)) {
            TreeSet<Long> done = terminalOffsets.get(partition);
            if (done != null) {
                done.headSet(newStart, false).clear();
            }
        }
        String prefix = partition + "#";
        attempts.keySet().removeIf(key -> {
            if (!key.startsWith(prefix)) {
                return false;
            }
            try {
                return Long.parseLong(key.substring(prefix.length())) < newStart;
            } catch (NumberFormatException e) {
                return false;
            }
        });
    }

    public int getAttempts(String partition, long offset) {
        AtomicInteger value = attempts.get(attemptKey(partition, offset));
        return value == null ? 0 : value.get();
    }

    public int incrementAttempts(String partition, long offset) {
        return attempts.computeIfAbsent(attemptKey(partition, offset),
                k -> new AtomicInteger(0)).incrementAndGet();
    }

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
        terminalOffsets.clear();
        processedEventIds.clear();
    }
}
