package com.github.h3yow.eventoutboxpipelinejava.outbox.handler;

import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEvent;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 默认演示用处理器：记录每次生效的副作用（便于本地验证与测试断言）。
 *
 * <p>约定 payload 以 {@code fail:} 开头时抛出 {@link IllegalStateException}，
 * 用于演示重试与死信流转；其余 payload 视为处理成功。
 *
 * <p>注意：本类自身不做去重，“只生效一次”由外层流水线的幂等存储保证；
 * 因此 {@link #processedEventIds()} 在正确配置下每个 eventId 只会出现一次。
 */
public class RecordingEventHandler implements EventHandler {

    /** payload 此前缀表示本事件应处理失败（用于本地演示重试/死信）。 */
    public static final String FAIL_PREFIX = "fail:";

    private final CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();
    private final java.util.concurrent.atomic.AtomicInteger invocations =
            new java.util.concurrent.atomic.AtomicInteger();

    @Override
    public void handle(OutboxEvent event) {
        invocations.incrementAndGet();
        if (event.payload() != null && event.payload().startsWith(FAIL_PREFIX)) {
            throw new IllegalStateException(
                    "simulated business failure for event " + event.eventId()
                            + ": " + event.payload().substring(FAIL_PREFIX.length()));
        }
        processed.add(event.eventId());
    }

    /** 实际生效过的事件标识（每个 eventId 只会出现一次）。 */
    public List<String> processedEventIds() {
        return List.copyOf(processed);
    }

    public int successCount() {
        return processed.size();
    }

    /** 处理器被调用的总次数（含失败调用）。 */
    public int invocationCount() {
        return invocations.get();
    }
}
