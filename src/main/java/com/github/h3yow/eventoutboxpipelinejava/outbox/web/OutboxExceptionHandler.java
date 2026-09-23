package com.github.h3yow.eventoutboxpipelinejava.outbox.web;

import com.github.h3yow.eventoutboxpipelinejava.outbox.offset.InvalidOffsetException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 把本地流水线的典型错误映射为可区分的 HTTP 响应，便于 curl 验证。
 */
@RestControllerAdvice
public class OutboxExceptionHandler {

    /** 越界/失效位点：422，并返回可区分的 reason（NEGATIVE/EXPIRED/BEYOND_LATEST/...）。 */
    @ExceptionHandler(InvalidOffsetException.class)
    public ResponseEntity<Map<String, Object>> handleInvalidOffset(InvalidOffsetException ex) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", "INVALID_OFFSET");
        body.put("reason", ex.reason());
        body.put("consumerGroup", ex.consumerGroup());
        body.put("aggregateId", ex.aggregateId());
        body.put("offset", ex.offset());
        body.put("message", ex.getMessage());
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(body);
    }

    /** 重复 eventId 等非法状态：409。 */
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalState(IllegalStateException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("error", "ILLEGAL_STATE", "message", String.valueOf(ex.getMessage())));
    }

    /** 参数非法：400。 */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException ex) {
        return ResponseEntity.badRequest()
                .body(Map.of("error", "INVALID_ARGUMENT", "message", String.valueOf(ex.getMessage())));
    }
}
