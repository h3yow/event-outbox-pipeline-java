package com.github.h3yow.eventoutboxpipelinejava.retry;

import java.time.Duration;

/**
 * 可配置的重试策略：maxAttempts 为最大处理次数（含首次），超过后进入死信区。
 */
public record RetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        initialBackoff = initialBackoff == null ? Duration.ZERO : initialBackoff;
        maxBackoff = maxBackoff == null ? initialBackoff : maxBackoff;
        if (maxBackoff.compareTo(initialBackoff) < 0) {
            throw new IllegalArgumentException("maxBackoff must be >= initialBackoff");
        }
    }

    public static RetryPolicy noRetry() {
        return new RetryPolicy(1, Duration.ZERO, Duration.ZERO);
    }

    /**
     * 计算第 failedAttempt 次失败后的退避时长（指数退避，上限 maxBackoff）。
     */
    public Duration backoffAfter(int failedAttempt) {
        if (failedAttempt <= 0) {
            return Duration.ZERO;
        }
        long millis = initialBackoff.toMillis() << Math.min(failedAttempt - 1, 20);
        return Duration.ofMillis(Math.min(millis, maxBackoff.toMillis()));
    }

    /**
     * 已失败 failedAttempt 次后是否还应重试（总处理次数不超过 maxAttempts）。
     */
    public boolean shouldRetry(int failedAttempt) {
        return failedAttempt >= 0 && failedAttempt < maxAttempts;
    }
}
