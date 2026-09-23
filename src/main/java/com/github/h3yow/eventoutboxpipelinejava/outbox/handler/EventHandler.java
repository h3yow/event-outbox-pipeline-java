package com.github.h3yow.eventoutboxpipelinejava.outbox.handler;

import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEvent;

/**
 * 业务事件处理器。
 *
 * <p>实现方只需表达业务副作用；幂等、重试、死信、顺序由外层流水线保证。
 * 抛出任意异常即视为本次处理失败。
 */
@FunctionalInterface
public interface EventHandler {
    void handle(OutboxEvent event) throws Exception;
}
