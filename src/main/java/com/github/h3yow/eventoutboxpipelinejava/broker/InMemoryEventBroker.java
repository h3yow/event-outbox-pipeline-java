package com.github.h3yow.eventoutboxpipelinejava.broker;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内消息代理模拟：分区追加日志。同一 aggregateKey 稳定映射到同一分区，
 * 分区内严格按位点顺序读取。
 *
 * <p>日志默认只追加；后台清理（{@link #truncatePrefix}）只删除已彻底结束
 * （已成功处理或已离开死信区）的连续前缀，截断后 {@code logStartOffset} 前移，
 * 但 {@code eventId -> 位置} 索引保留：因此旧位点可以被明确判定为
 * “已截断”，而不是与“事件不存在”混淆。
 */
public class InMemoryEventBroker {

    private static final Logger log = LoggerFactory.getLogger(InMemoryEventBroker.class);

    private static final class PartitionLog {
        // index 0 对应的物理位点为 startOffset；截断通过 subList(0,n).clear() 实现
        private final List<EventEnvelope> events = new ArrayList<>();
        private long startOffset = 0;

        synchronized long append(EventEnvelope event) {
            long offset = startOffset + events.size();
            events.add(event);
            return offset;
        }

        synchronized Optional<EventEnvelope> read(long offset) {
            int index = (int) (offset - startOffset);
            if (index < 0 || index >= events.size()) {
                return Optional.empty();
            }
            return Optional.of(events.get(index));
        }

        synchronized long startOffset() {
            return startOffset;
        }

        synchronized long endOffset() {
            return startOffset + events.size();
        }

        synchronized int retainedSize() {
            return events.size();
        }

        /**
         * 删除位点严格小于 newStartOffset 的前缀，最多删除 {@code maxBatch} 条。
         *
         * @return 实际删除的事件；newStartOffset 不大于当前起始位点时返回空列表
         */
        synchronized List<EventEnvelope> truncatePrefix(long newStartOffset, int maxBatch) {
            long removable = newStartOffset - startOffset;
            int removeCount = 0;
            if (removable > 0 && !events.isEmpty()) {
                removeCount = (int) Math.min(Math.min(removable, events.size()), Math.max(1, maxBatch));
            }
            if (removeCount == 0) {
                return List.of();
            }
            List<EventEnvelope> removed = new ArrayList<>(events.subList(0, removeCount));
            events.subList(0, removeCount).clear();
            startOffset += removeCount;
            return removed;
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
     * 后台清理：把分区中位点严格小于 {@code newStartOffset} 的事件前缀物理删除，
     * 单次最多删除 {@code maxBatch} 条，避免大批量拷贝阻塞入队/消费。
     *
     * <p>调用方必须保证被删区间内的事件都已彻底结束（成功处理或已离开死信区）；
     * 本方法只负责按边界截断。截断不影响 {@code eventId -> 位置} 索引
     * （因此截断后仍能区分“位点已被清理”与“事件从未存在”）。
     *
     * @return 本次实际删除的事件
     */
    public List<EventEnvelope> truncatePrefix(String partition, long newStartOffset, int maxBatch) {
        if (!partitionNames.contains(partition)) {
            throw new IllegalArgumentException("unknown partition: " + partition);
        }
        PartitionLog log = logs.get(partition);
        if (log == null) {
            return List.of();
        }
        List<EventEnvelope> removed = log.truncatePrefix(newStartOffset, maxBatch);
        if (!removed.isEmpty()) {
            InMemoryEventBroker.log.info(
                    "decision=LOG_TRUNCATED partition={} removed={} newLogStart={} logEnd={} basis='safe-watermark'",
                    partition, removed.size(), log.startOffset(), log.endOffset());
        }
        return removed;
    }

    public Optional<Location> locate(String eventId) {
        return Optional.ofNullable(eventIndex.get(eventId));
    }

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
