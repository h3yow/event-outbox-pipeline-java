package com.github.h3yow.eventoutboxpipelinejava.support;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.handler.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试处理器：记录每次调用，并可对指定 eventId 注入前 N 次失败。
 */
public class RecordingHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(RecordingHandler.class);

    private final List<String> handledOrder = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, AtomicInteger> calls = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> failuresRemaining = new ConcurrentHashMap<>();
    private final Set<String> sideEffects = ConcurrentHashMap.newKeySet();
    // 当前正处于 handle 调用中的事件：若同一 eventId 重入，说明发生并发重复处理
    private final Set<String> inProgress = ConcurrentHashMap.newKeySet();
    private final Set<String> concurrentReentries = ConcurrentHashMap.newKeySet();

    public void failFirst(String eventId, int times) {
        failuresRemaining.put(eventId, new AtomicInteger(times));
    }

    public int callCount(String eventId) {
        AtomicInteger value = calls.get(eventId);
        return value == null ? 0 : value.get();
    }

    public Set<String> concurrentReentries() {
        return Set.copyOf(concurrentReentries);
    }

    public List<String> handledOrder() {
        return List.copyOf(handledOrder);
    }

    /** 真正生效的业务副作用（每次成功调用记录）。 */
    public Set<String> sideEffects() {
        return Set.copyOf(sideEffects);
    }

    public int sideEffectCount(String eventId) {
        return sideEffects.contains(eventId) ? 1 : 0;
    }

    @Override
    public void handle(EventEnvelope event) {
        if (!inProgress.add(event.eventId())) {
            concurrentReentries.add(event.eventId());
        }
        try {
            int n = calls.computeIfAbsent(event.eventId(), k -> new AtomicInteger()).incrementAndGet();
            log.info("handler invoked eventId={} aggregateKey={} invocation={}",
                    event.eventId(), event.aggregateKey(), n);
            AtomicInteger remaining = failuresRemaining.get(event.eventId());
            if (remaining != null && remaining.getAndDecrement() > 0) {
                throw new IllegalStateException("injected-failure for " + event.eventId()
                        + " invocation=" + n);
            }
            sideEffects.add(event.eventId());
            handledOrder.add(event.eventId());
        } finally {
            inProgress.remove(event.eventId());
        }
    }
}
