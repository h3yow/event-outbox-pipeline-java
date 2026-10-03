package com.github.h3yow.eventoutboxpipelinejava.pipeline;

import com.github.h3yow.eventoutboxpipelinejava.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.cleanup.EventLogCleaner;
import com.github.h3yow.eventoutboxpipelinejava.consumer.ConsumerGroupState;
import com.github.h3yow.eventoutboxpipelinejava.consumer.EventConsumer;
import com.github.h3yow.eventoutboxpipelinejava.dlq.DeadLetterRecord;
import com.github.h3yow.eventoutboxpipelinejava.dlq.DeadLetterStore;
import com.github.h3yow.eventoutboxpipelinejava.dlq.InMemoryDeadLetterStore;
import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.handler.EventHandler;
import com.github.h3yow.eventoutboxpipelinejava.offset.OffsetInvalidException;
import com.github.h3yow.eventoutboxpipelinejava.retry.RetryPolicy;
import com.github.h3yow.eventoutboxpipelinejava.state.AggregateStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 管道门面：事件入队、位点回放、死信检索与重放、聚合当前状态、日志清理、生命周期管理。
 * 线程安全，所有能力均可在本地运行验证。
 */
public class EventPipeline {

    private static final Logger log = LoggerFactory.getLogger(EventPipeline.class);

    private final InMemoryEventBroker broker;
    private final ConsumerGroupState state;
    private final InMemoryDeadLetterStore deadLetters;
    private final AggregateStateStore aggregateState;
    private final EventConsumer consumer;
    private final EventLogCleaner cleaner;

    public EventPipeline(InMemoryEventBroker broker,
                         ConsumerGroupState state,
                         DeadLetterStore deadLetters,
                         EventHandler handler,
                         RetryPolicy retryPolicy,
                         int workerCount) {
        this(broker, state, deadLetters, handler, retryPolicy, workerCount, 5L);
    }

    public EventPipeline(InMemoryEventBroker broker,
                         ConsumerGroupState state,
                         DeadLetterStore deadLetters,
                         EventHandler handler,
                         RetryPolicy retryPolicy,
                         int workerCount,
                         long pollIntervalMillis) {
        this(broker, state, deadLetters, new AggregateStateStore(), null, handler,
                retryPolicy, workerCount, pollIntervalMillis);
    }

    public EventPipeline(InMemoryEventBroker broker,
                         ConsumerGroupState state,
                         DeadLetterStore deadLetters,
                         EventHandler handler,
                         RetryPolicy retryPolicy,
                         int workerCount,
                         long pollIntervalMillis,
                         EventLogCleaner cleaner) {
        this(broker, state, deadLetters, new AggregateStateStore(), cleaner, handler,
                retryPolicy, workerCount, pollIntervalMillis);
    }

    public EventPipeline(InMemoryEventBroker broker,
                         ConsumerGroupState state,
                         DeadLetterStore deadLetters,
                         AggregateStateStore aggregateState,
                         EventLogCleaner cleaner,
                         EventHandler handler,
                         RetryPolicy retryPolicy,
                         int workerCount,
                         long pollIntervalMillis) {
        if (!(deadLetters instanceof InMemoryDeadLetterStore store)) {
            throw new IllegalArgumentException("deadLetters must be InMemoryDeadLetterStore");
        }
        this.broker = broker;
        this.state = state;
        this.deadLetters = store;
        this.aggregateState = aggregateState;
        this.consumer = new EventConsumer(broker, state, store, aggregateState, handler,
                retryPolicy, workerCount, pollIntervalMillis);
        this.cleaner = cleaner;
    }

    /**
     * 构造并投递一个新事件，返回事件信封。
     */
    public EventEnvelope enqueue(String aggregateKey, String type, byte[] payload) {
        EventEnvelope event = new EventEnvelope(
                UUID.randomUUID().toString(), aggregateKey, type, payload, System.currentTimeMillis());
        publish(event);
        return event;
    }

    /**
     * 投递既有事件。eventId 幂等：重复投递返回同一物理位置，不产生重复消息。
     */
    public InMemoryEventBroker.Location publish(EventEnvelope event) {
        InMemoryEventBroker.Location location = broker.publish(event);
        log.info("eventId={} aggregateKey={} partition={} offset={} decision=ENQUEUED",
                event.eventId(), event.aggregateKey(), location.partition(), location.offset());
        return location;
    }

    public void start() {
        consumer.start();
        if (cleaner != null) {
            cleaner.start();
        }
    }

    public void stop() {
        if (cleaner != null) {
            cleaner.stop();
        }
        consumer.stop();
    }

    public boolean awaitProcessed(long timeoutMillis) throws InterruptedException {
        return consumer.awaitIdle(timeoutMillis);
    }

    /**
     * 手动触发一轮后台清理（无清理器时直接返回 0）。返回本轮删除事件数。
     * 清理只动已彻底结束的前缀，不阻断入队与消费。
     */
    public int cleanupOnce() {
        return cleaner == null ? 0 : cleaner.cleanupOnce();
    }

    /**
     * 位点回放（模拟消费者重启/位点重放）。
     *
     * @throws com.github.h3yow.eventoutboxpipelinejava.offset.OffsetOutOfRangeException 位点越界（可区分 NEGATIVE / TRUNCATED / TOO_HIGH）
     * @throws OffsetInvalidException 位点指向已死信事件
     */
    public void seek(String partition, long offset) {
        consumer.rewind(partition, offset);
    }

    /**
     * 按 eventId 定位并回放；事件不存在时抛 IllegalArgumentException。
     */
    public void seekToEvent(String eventId) {
        InMemoryEventBroker.Location location = broker.locate(eventId)
                .orElseThrow(() -> new IllegalArgumentException("unknown eventId: " + eventId));
        consumer.rewind(location.partition(), location.offset());
    }

    /**
     * 重放死信：必须先从死信区移除（解除位点锁定），再回放位点并重新获得完整重试预算。
     * 若该事件此前从未成功处理过，其业务副作用将在重放成功时生效一次。
     */
    public void replay(String eventId) {
        DeadLetterRecord record = deadLetters.findByEventId(eventId)
                .orElseThrow(() -> new IllegalArgumentException("not a dead letter: " + eventId));
        deadLetters.remove(eventId);
        log.info("eventId={} partition={} offset={} decision=DLQ_RELEASED",
                eventId, record.partition(), record.offset());
        consumer.rewind(record.partition(), record.offset());
    }

    public Optional<DeadLetterRecord> deadLetter(String eventId) {
        return deadLetters.findByEventId(eventId);
    }

    public List<DeadLetterRecord> deadLetters() {
        return deadLetters.findAll();
    }

    public long committedOffset(String partition) {
        return consumer.committedOffset(partition);
    }

    public boolean isProcessed(String eventId) {
        return state.isProcessed(eventId);
    }

    /**
     * 聚合当前状态（最后一个成功处理的事件）。独立于分区日志：
     * 即使该事件所在日志前缀已被后台清理，这里依旧可读。
     */
    public Optional<AggregateStateStore.Snapshot> currentAggregateState(String aggregateKey) {
        return aggregateState.currentState(aggregateKey);
    }

    public InMemoryEventBroker.Location locate(String eventId) {
        return broker.locate(eventId)
                .orElseThrow(() -> new IllegalArgumentException("unknown eventId: " + eventId));
    }
}
