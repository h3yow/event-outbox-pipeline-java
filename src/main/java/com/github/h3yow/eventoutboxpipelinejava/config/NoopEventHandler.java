package com.github.h3yow.eventoutboxpipelinejava.config;

import com.github.h3yow.eventoutboxpipelinejava.domain.EventEnvelope;
import com.github.h3yow.eventoutboxpipelinejava.handler.EventHandler;

/**
 * 未提供自定义处理器时使用的空处理器（保证 Spring 装配可启动）。
 */
public class NoopEventHandler implements EventHandler {
    @Override
    public void handle(EventEnvelope event) {
    }
}
