package com.github.h3yow.eventoutboxpipelinejava.outbox.web;

import com.github.h3yow.eventoutboxpipelinejava.outbox.core.DeadLetterState;
import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEvent;
import com.github.h3yow.eventoutboxpipelinejava.outbox.core.OutboxEventPipeline;
import com.github.h3yow.eventoutboxpipelinejava.outbox.dlq.DeadLetterEntry;
import com.github.h3yow.eventoutboxpipelinejava.outbox.handler.RecordingEventHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 本地验证用 HTTP 接口：入队、查询死信/幂等副作用/消费位点。
 */
@RestController
@RequestMapping("/api/outbox")
public class OutboxController {

    private final OutboxEventPipeline pipeline;
    private final RecordingEventHandler recorder;

    public OutboxController(OutboxEventPipeline pipeline, RecordingEventHandler recorder) {
        this.pipeline = pipeline;
        this.recorder = recorder;
    }

    /** 入队事件；payload 以 "fail:" 开头可本地演示失败重试与死信。 */
    @PostMapping("/events")
    public OutboxEvent enqueue(@RequestBody EnqueueRequest request) {
        return pipeline.enqueue(request.aggregateId(), request.eventId(),
                request.type(), request.payload());
    }

    /** 列出死信；可按状态过滤（OPEN / REPLAYED / DISCARDED）。 */
    @GetMapping("/dead-letters")
    public List<DeadLetterEntry> listDeadLetters(@RequestParam(required = false) String state) {
        if (state == null || state.isBlank()) {
            return pipeline.deadLetters().findAll();
        }
        return pipeline.deadLetters().findByState(DeadLetterState.valueOf(state));
    }

    @GetMapping("/dead-letters/{eventId}")
    public DeadLetterEntry getDeadLetter(@PathVariable String eventId) {
        return pipeline.deadLetters().findByEventId(eventId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "dead letter entry not found: " + eventId));
    }

    /** 已生效业务副作用的事件标识（去重后，每个仅出现一次）。 */
    @GetMapping("/processed")
    public Map<String, Object> listProcessed() {
        return Map.of(
                "successCount", recorder.successCount(),
                "invocationCount", recorder.invocationCount(),
                "processedEventIds", recorder.processedEventIds());
    }

    /** 各聚合在当前消费者组下的消费位点。 */
    @GetMapping("/offsets")
    public Map<String, Long> offsets() {
        return pipeline.offsets().snapshot(
                pipeline.consumerGroup(),
                pipeline.broker().aggregateIds(),
                0L);
    }
}
