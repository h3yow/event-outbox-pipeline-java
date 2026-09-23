package com.github.h3yow.eventoutboxpipelinejava.handler;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;

/**
 * 业务处理器接口。抛出异常视为处理失败，触发重试/死信流程。
 */
@FunctionalInterface
public interface EventHandler {
    void handle(EventEnvelope event) throws Exception;
}
