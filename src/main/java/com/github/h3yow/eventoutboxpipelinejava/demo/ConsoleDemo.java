package com.github.h3yow.eventoutboxpipelinejava.demo;

import com.github.h3yow.eventoutboxpipelinejava.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.cleanup.EventLogCleaner;
import com.github.h3yow.eventoutboxpipelinejava.consumer.ConsumerGroupState;
import com.github.h3yow.eventoutboxpipelinejava.dlq.DeadLetterRecord;
import com.github.h3yow.eventoutboxpipelinejava.dlq.InMemoryDeadLetterStore;
import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.offset.OffsetOutOfRangeException;
import com.github.h3yow.eventoutboxpipelinejava.pipeline.EventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.retry.RetryPolicy;
import com.github.h3yow.eventoutboxpipelinejava.state.AggregateStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 纯 main 方法的本地演示，不启动 Spring / Web 容器：
 * 正常事件、瞬时失败后重试成功、永久失败转死信，随后重放死信。
 *
 * <p>运行：mvn -q compile exec:java -Dexec.mainClass=com.github.h3yow.eventoutboxpipelinejava.demo.ConsoleDemo
 */
public final class ConsoleDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsoleDemo.class);

    private ConsoleDemo() {
    }

    public static void main(String[] args) throws Exception {
        // 单分区 + 多工作线程：大量聚合挤在同一条物理通道上，演示聚合级隔离
        InMemoryEventBroker broker = new InMemoryEventBroker(1);
        ConsumerGroupState state = new ConsumerGroupState();
        InMemoryDeadLetterStore dlq = new InMemoryDeadLetterStore();
        AggregateStateStore aggregateState = new AggregateStateStore();
        Map<String, Integer> failTimes = new ConcurrentHashMap<>(Map.of("evt-flaky", 2));
        Set<String> applied = ConcurrentHashMap.newKeySet();

        RetryPolicy policy = new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(1));
        EventLogCleaner cleaner = new EventLogCleaner(broker, state, dlq,
                Duration.ofSeconds(1).toMillis(), 100);
        EventPipeline pipeline = new EventPipeline(broker, state, dlq, aggregateState, cleaner,
                event -> {
                    Integer remaining = failTimes.get(event.eventId());
                    if (remaining != null && remaining > 0) {
                        failTimes.put(event.eventId(), remaining - 1);
                        throw new IllegalStateException("simulated transient failure: " + event.eventId());
                    }
                    applied.add(event.eventId());
                }, policy, 4, 5L);

        pipeline.start();
        // 同一个分区上的三个互不相干聚合
        EventEnvelope fine = pipeline.enqueue("order-A", "OrderPaid", null);
        EventEnvelope flaky = new EventEnvelope("evt-flaky", "order-A", "OrderUpdated",
                new byte[0], System.currentTimeMillis());
        pipeline.publish(flaky);
        EventEnvelope poison = new EventEnvelope("evt-poison", "order-B", "OrderClosed",
                new byte[0], System.currentTimeMillis());
        pipeline.publish(poison);
        failTimes.put("evt-poison", Integer.MAX_VALUE);
        pipeline.enqueue("order-C", "OrderCreated", null); // 不受 order-B 死信影响

        pipeline.awaitProcessed(10_000);

        log.info("---- after first pass ----");
        log.info("sideEffectsApplied={}", applied);
        log.info("aggregateState(order-A)={}", pipeline.currentAggregateState("order-A")
                .map(s -> s.event().eventId()).orElse("-"));
        for (DeadLetterRecord r : pipeline.deadLetters()) {
            log.info("DLQ eventId={} partition={} offset={} attempts={} reason='{}'",
                    r.event().eventId(), r.partition(), r.offset(), r.attempts(), r.reason());
        }

        // 手动清理一轮：存活死信 order-B 是硬水位，它之前没有可删前缀
        int removed = pipeline.cleanupOnce();
        log.info("manual cleanup removed={} while a live dead letter pins the watermark", removed);

        // 位点四类拒绝演示
        try {
            pipeline.seek("p0", 999);
        } catch (OffsetOutOfRangeException ex) {
            log.info("seek rejected: reason={} msg={}", ex.getReason(), ex.getMessage());
        }

        // 修复后重放死信，再清理：水位放开，已结束前缀被物理删除
        failTimes.remove("evt-poison");
        pipeline.replay("evt-poison");
        pipeline.awaitProcessed(10_000);
        removed = pipeline.cleanupOnce();
        log.info("cleanup after DLQ replay removed={}, logStartOffset(p0)={}",
                removed, broker.logStartOffset("p0"));

        // 旧位点落入已清理区间：与负数/超末尾/死信相互区分
        try {
            pipeline.seek("p0", 0);
        } catch (OffsetOutOfRangeException ex) {
            log.info("seek rejected: reason={} msg={}", ex.getReason(), ex.getMessage());
        }

        // 物理消息已删，但聚合当前状态依旧可读
        log.info("aggregateState(order-A) after cleanup={}", pipeline.currentAggregateState("order-A")
                .map(s -> s.event().eventId()).orElseThrow());

        // 此时整段日志已清理，旧位点回放一律 OFFSET_TRUNCATED；
        // 若要观察“仍存活区间内重复投递副作用不重复”，可在清理前 seek（见单元测试）
        try {
            pipeline.seek("p0", broker.logEndOffset("p0") - 1);
        } catch (OffsetOutOfRangeException ex) {
            log.info("seek after full cleanup rejected: reason={}", ex.getReason());
        }

        log.info("---- final ----");
        log.info("sideEffectsApplied={} (each once)", applied);
        log.info("dlqSize={}", pipeline.deadLetters().size());
        pipeline.stop();
    }
}
