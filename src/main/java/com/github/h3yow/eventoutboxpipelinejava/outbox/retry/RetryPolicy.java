package com.github.h3yow.eventoutboxpipelinejava.outbox.retry;

import java.time.Duration;
import java.util.Objects;

/**
 * 可配置的失败重试策略。
 *
 * <p>处理失败后按 {@link BackoffStrategy} 计算退避时间重试；
 * 当已尝试次数达到 {@code maxAttempts} 仍失败，则事件转入死信区。
 *
 * <p>{@code maxAttempts} 表示“总尝试次数（含首次）”：
 * 为 1 时不重试，首次失败即进死信区；为 3 时最多处理 3 次（首次 + 2 次重试）。
 */
public final class RetryPolicy {

    private final int maxAttempts;
    private final BackoffStrategy backoff;

    public RetryPolicy(int maxAttempts, BackoffStrategy backoff) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.maxAttempts = maxAttempts;
        this.backoff = Objects.requireNonNull(backoff, "backoff");
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * 已失败 {@code failedAttempt} 次后，是否还应继续重试。
     *
     * <p>failedAttempt=1 表示首次处理刚失败；当 failedAttempt &lt; maxAttempts 时尚有重试额度。
     */
    public boolean shouldRetry(int failedAttempt) {
        return failedAttempt < maxAttempts;
    }

    /** 第 {@code failedAttempt} 次失败（从 1 开始）后、下次重试前的退避时间。 */
    public Duration backoffAfter(int failedAttempt) {
        if (failedAttempt < 1) {
            throw new IllegalArgumentException("failedAttempt must be >= 1");
        }
        long millis = backoff.nextDelayMillis(failedAttempt);
        if (millis < 0) {
            throw new IllegalStateException("backoff must not be negative: " + millis);
        }
        return Duration.ofMillis(millis);
    }
}
