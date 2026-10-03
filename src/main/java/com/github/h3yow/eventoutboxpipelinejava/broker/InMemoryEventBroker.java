package com.github.h3yow.eventoutboxpipelinejava.broker;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内消息代理模拟：分区追加日志。同一 aggregateKey 稳定映射到同一分区，
 * 分区内严格按位点顺序读取。
 *
 * <p>日志支持前缀截断（{@link #trimPrefix}）：位点是绝对位点，截断后
 * {@link #logStartOffset} 向前推进，被截断区间的事件物理删除，但
 * {@code eventIndex} 保留（重复入队折叠、按 eventId 定位仍可工作），
 * 以较小的索引内存换取“长时间运行内存有界 + 幂等语义不丢”。
 */
public class InMemoryEventBroker {

    /**
     * 分段日志：每段固定大小，截断以整段为单位回收，读取 O(1)。
     * 非边界截断时，包含 logStart 的段作为部分段保留（每分区至多残留一个部分段的空槽）。
     */
    private static final class PartitionLog {

        private static final int SEGMENT_SIZE = 256;

        // segments.get(0) 对应当前最早未整段回收的段
        private final List<EventEnvelope[]> segments = new ArrayList<>();
        private long startOffset;
        private long endOffset;

        private synchronized long append(EventEnvelope event) {
            int within = (int) (endOffset % SEGMENT_SIZE);
            if (within == 0) {
                segments.add(new EventEnvelope[SEGMENT_SIZE]);
            }
            segments.get(segments.size() - 1)[within] = event;
            return endOffset++;
        }

        private synchronized Optional<EventEnvelope> read(long offset) {
            if (offset < startOffset || offset >= endOffset) {
                return Optional.empty();
            }
            long firstSegmentIndex = startOffset / SEGMENT_SIZE;
            long segmentIndex = offset / SEGMENT_SIZE - firstSegmentIndex;
            int within = (int) (offset % SEGMENT_SIZE);
            EventEnvelope[] segment = segments.get((int) segmentIndex);
            return Optional.ofNullable(segment[within]);
        }

        /**
         * 截断到 newStart：物理回收所有完全位于 newStart 之前的段。
         * 仅允许单调向前推进，且不得越过日志末尾。
         *
         * @return 截断后的实际起始位点
         */
        private synchronized long trimTo(long newStart) {
            if (newStart < startOffset) {
                throw new IllegalArgumentException(
                        "trim cannot move backwards: current=" + startOffset + ", requested=" + newStart);
            }
            if (newStart > endOffset) {
                throw new IllegalArgumentException(
                        "trim beyond log end: end=" + endOffset + ", requested=" + newStart);
            }
            if (newStart == startOffset) {
                return startOffset;
            }
            long firstSegmentIndex = startOffset / SEGMENT_SIZE;
            // 段编号 < newStart 所在段的整段全部落在 newStart 之前，可回收；
            // 当 newStart 恰好是段边界时，其所在段整段丢弃（drop 计数天然正确）。
            long keepFromSegment = newStart / SEGMENT_SIZE;
            int dropSegments = (int) (keepFromSegment - firstSegmentIndex);
            if (dropSegments > 0) {
                segments.subList(0, Math.min(dropSegments, segments.size())).clear();
            }
            startOffset = newStart;
            return startOffset;
        }

        private synchronized long endOffset() {
            return endOffset;
        }

        private synchronized long startOffset() {
            return startOffset;
        }

        /** 当前物理保留的事件条数（仅用于可观测/测试）。 */
        private synchronized int retainedSize() {
            if (segments.isEmpty()) {
                return 0;
            }
            long absolute = endOffset - startOffset;
            return (int) absolute;
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
            long offset = log.append(event);
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
        PartitionLog log = logs.get(partition);
        return log == null ? 0 : log.startOffset();
    }

    /**
     * 将分区日志前缀截断到 newStart：[logStart, newStart) 区间的事件被物理回收。
     *
     * <p>调用方必须保证 newStart 之前的事件都已彻底结束（成功处理或进入死信），
     * 且 newStart 不得越过任何死信事件（见消费者侧保留策略的安全点计算）。
     * 位点单调、不允许向后回退，也不允许越过日志末尾。
     *
     * @return 截断后的实际起始位点
     */
    public long trimPrefix(String partition, long newStart) {
        if (!partitionNames.contains(partition)) {
            throw new IllegalArgumentException("unknown partition: " + partition);
        }
        PartitionLog log = logs.computeIfAbsent(partition, k -> new PartitionLog());
        return log.trimTo(newStart);
    }

    public Optional<Location> locate(String eventId) {
        return Optional.ofNullable(eventIndex.get(eventId));
    }

    /** 物理保留的事件条数（= logEndOffset - logStartOffset；用于可观测/测试）。 */
    public int size(String partition) {
        PartitionLog log = logs.get(partition);
        return log == null ? 0 : log.retainedSize();
    }

    /**
     * 事件物理位置：分区 + 位点。
     */
    public record Location(String partition, long offset) {
    }
}
