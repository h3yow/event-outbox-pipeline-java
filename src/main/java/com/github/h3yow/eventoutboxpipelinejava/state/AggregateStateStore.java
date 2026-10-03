package com.github.h3yow.eventoutboxpipelinejava.state;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 聚合当前状态：保留每个 aggregateKey 最后一个成功处理完成的事件。
 *
 * <p>该投影独立于分区日志存在：日志前缀被后台清理后，读不到旧物理消息，
 * 但聚合“当前状态该读到的还得读到”。只有业务处理成功（而非死信）才更新。
 * 同一聚合内事件严格保序地更新，后成功的高位点事件必然覆盖低位点事件。
 */
public class AggregateStateStore {

    public record Snapshot(EventEnvelope event, String partition, long offset) {
    }

    private final ConcurrentHashMap<String, Snapshot> latestByAggregate = new ConcurrentHashMap<>();

    /**
     * 记录一次成功处理。同一聚合按位点顺序串行成功，直接覆盖即可；
     * 为防御乱序调用，仅接受位点不小于既有快照的更新。
     */
    public void recordSuccess(String aggregateKey, EventEnvelope event, String partition, long offset) {
        latestByAggregate.merge(aggregateKey, new Snapshot(event, partition, offset),
                (existing, incoming) -> incoming.offset() >= existing.offset() ? incoming : existing);
    }

    public Optional<Snapshot> currentState(String aggregateKey) {
        return Optional.ofNullable(latestByAggregate.get(aggregateKey));
    }

    public int size() {
        return latestByAggregate.size();
    }

    public void clear() {
        latestByAggregate.clear();
    }
}
