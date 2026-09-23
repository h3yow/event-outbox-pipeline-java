package com.github.h3yow.eventoutboxpipelinejava.outbox.offset;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 消费位点存储：以“消费者组 + 聚合”为键维护已提交位点。
 *
 * <p>位点语义为“下一个待处理事件的下标”：提交 {@code n} 表示 [0,n) 已处理完。
 * 提交（{@link #commit}）与回放（{@link #seek}）都会校验合法性，越界/失效位点
 * 以 {@link InvalidOffsetException} 明确拒绝并给出 {@link OffsetRejectReason}：
 * <ul>
 *     <li>{@link OffsetRejectReason#NEGATIVE}：负数位点；</li>
 *     <li>{@link OffsetRejectReason#EXPIRED}：位点为正但对应日志已被截断，永久失效；</li>
 *     <li>{@link OffsetRejectReason#BEYOND_LATEST}：位点超出当前最新位点 + 1（尚无对应事件）；</li>
 *     <li>{@link OffsetRejectReason#UNKNOWN_PARTITION}：位点属于未知聚合分区。</li>
 * </ul>
 */
public final class OffsetStore {

    private final ConcurrentMap<String, ConcurrentMap<String, Long>> byGroup = new ConcurrentHashMap<>();

    public OffsetStore() {
    }

    private ConcurrentMap<String, Long> group(String consumerGroup) {
        return byGroup.computeIfAbsent(consumerGroup, k -> new ConcurrentHashMap<>());
    }

    /** 返回当前已提交位点；从无提交记录时返回 {@code defaultOffset}。 */
    public long committed(String consumerGroup, String aggregateId, long defaultOffset) {
        return group(consumerGroup).getOrDefault(aggregateId, defaultOffset);
    }

    /**
     * 提交位点（表示该位点之前的事件均已处理完成）。
     *
     * <p>合法区间为 {@code [earliest, latest+1]}；{@code latest} 为 -1（分区无事件）时仅允许
     * {@code earliest}（即 0）。
     *
     * @param earliest 当前日志最早可读位点
     * @param latest   当前日志最新位点（无事件时为 -1）
     */
    public void commit(String consumerGroup, String aggregateId, long offset,
                       long earliest, long latest) {
        validate(consumerGroup, aggregateId, offset, earliest, latest);
        group(consumerGroup).put(aggregateId, offset);
    }

    /** 回放：把位点重置为 {@code offset}，校验规则与 {@link #commit} 相同。 */
    public void seek(String consumerGroup, String aggregateId, long offset,
                     long earliest, long latest) {
        validate(consumerGroup, aggregateId, offset, earliest, latest);
        group(consumerGroup).put(aggregateId, offset);
    }

    private void validate(String consumerGroup, String aggregateId, long offset,
                          long earliest, long latest) {
        if (offset < 0) {
            throw new InvalidOffsetException(OffsetRejectReason.NEGATIVE,
                    consumerGroup, aggregateId, offset,
                    "offset must be >= 0 but got " + offset);
        }
        if (offset < earliest) {
            throw new InvalidOffsetException(OffsetRejectReason.EXPIRED,
                    consumerGroup, aggregateId, offset,
                    "offset " + offset + " has expired due to log truncation; earliest=" + earliest);
        }
        long upperBound = Math.max(earliest, latest + 1);
        if (offset > upperBound) {
            throw new InvalidOffsetException(OffsetRejectReason.BEYOND_LATEST,
                    consumerGroup, aggregateId, offset,
                    "offset " + offset + " is beyond latest+1=" + upperBound
                            + " (latest=" + latest + "); event does not exist yet");
        }
    }

    /** 某消费者组下所有聚合的当前位点快照（未初始化者取 defaultOffset）。 */
    public Map<String, Long> snapshot(String consumerGroup,
                                      Iterable<String> aggregateIds,
                                      long defaultOffset) {
        ConcurrentMap<String, Long> g = byGroup.getOrDefault(consumerGroup, new ConcurrentHashMap<>());
        Map<String, Long> result = new LinkedHashMap<>();
        for (String aggregateId : aggregateIds) {
            result.put(aggregateId, g.getOrDefault(aggregateId, defaultOffset));
        }
        return result;
    }
}
