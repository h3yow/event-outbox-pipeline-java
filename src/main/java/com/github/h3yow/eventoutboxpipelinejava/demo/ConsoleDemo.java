package com.github.h3yow.eventoutboxpipelinejava.demo;

import com.github.h3yow.eventoutboxpipelinejava.broker.InMemoryEventBroker;
import com.github.h3yow.eventoutboxpipelinejava.consumer.ConsumerGroupState;
import com.github.h3yow.eventoutboxpipelinejava.dlq.DeadLetterRecord;
import com.github.h3yow.eventoutboxpipelinejava.dlq.InMemoryDeadLetterStore;
import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.pipeline.EventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.retry.RetryPolicy;
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
        InMemoryEventBroker broker = new InMemoryEventBroker(2);
        ConsumerGroupState state = new ConsumerGroupState();
        InMemoryDeadLetterStore dlq = new InMemoryDeadLetterStore();
        Map<String, Integer> failTimes = new ConcurrentHashMap<>(Map.of("evt-flaky", 2));
        Set<String> applied = ConcurrentHashMap.newKeySet();

        RetryPolicy policy = new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(1));
        EventPipeline pipeline = new EventPipeline(broker, state, dlq,
                event -> {
                    Integer remaining = failTimes.get(event.eventId());
                    if (remaining != null && remaining > 0) {
                        failTimes.put(event.eventId(), remaining - 1);
                        throw new IllegalStateException("simulated transient failure: " + event.eventId());
                    }
                    applied.add(event.eventId());
                }, policy, 3, 5L);

        pipeline.start();
        EventEnvelope fine = pipeline.enqueue("order-A", "OrderPaid", null);
        // 固定 eventId，便于演示重复投递与失败注入
        EventEnvelope flaky = new EventEnvelope("evt-flaky", "order-A", "OrderUpdated",
                new byte[0], System.currentTimeMillis());
        pipeline.publish(flaky);
        EventEnvelope poison = new EventEnvelope("evt-poison", "order-B", "OrderClosed",
                new byte[0], System.currentTimeMillis());
        pipeline.publish(poison);
        failTimes.put("evt-poison", Integer.MAX_VALUE);

        pipeline.awaitProcessed(10_000);

        log.info("---- after first pass ----");
        log.info("sideEffectsApplied={}", applied);
        for (DeadLetterRecord r : pipeline.deadLetters()) {
            log.info("DLQ eventId={} partition={} offset={} attempts={} reason='{}'",
                    r.event().eventId(), r.partition(), r.offset(), r.attempts(), r.reason());
        }

        // 位点越界/死信拒绝演示
        try {
            pipeline.seek("p0", 999);
        } catch (RuntimeException ex) {
            log.info("seek rejected as expected: {}: {}", ex.getClass().getSimpleName(), ex.getMessage());
        }

        // 修复后重放死信
        failTimes.remove("evt-poison");
        pipeline.replay("evt-poison");
        pipeline.awaitProcessed(10_000);

        // 重复投递已成功事件：副作用不重复
        pipeline.seekToEvent(flaky.eventId());
        pipeline.awaitProcessed(10_000);

        log.info("---- final ----");
        log.info("sideEffectsApplied={} (each once)", applied);
        log.info("dlqSize={}", pipeline.deadLetters().size());
        pipeline.stop();
    }
}
