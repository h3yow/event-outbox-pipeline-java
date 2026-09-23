package com.github.h3yow.eventoutboxpipelinejava.outbox.web;

/**
 * 入队请求体。
 */
public record EnqueueRequest(String aggregateId, String eventId, String type, String payload) {
}
