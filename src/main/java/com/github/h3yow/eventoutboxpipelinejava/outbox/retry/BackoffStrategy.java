package com.github.h3yow.eventoutboxpipelinejava.outbox.retry;

import java.time.Duration;
import java.util.Objects;

/**
 * 退避策略：给定第 n 次失败（attempt 从 1 开始），返回下次重试前的等待毫秒数。
 */
@FunctionalInterface
public interface BackoffStrategy {

    /**
     * 第 {@code attempt} 次处理失败后的退避时间（attempt 从 1 开始）。
     *
     * @return 非负的等待毫秒数
     */
    long nextDelayMillis(int attempt);

    /** 固定退避。 */
    static BackoffStrategy fixed(Duration delay) {
        Objects.requireNonNull(delay, "delay");
        long millis = delay.toMillis();
        if (millis < 0) {
            throw new IllegalArgumentException("delay must be >= 0");
        }
        return attempt -> millis;
    }

    /**
     * 指数退避：{@code initial * multiplier^(attempt-1)}，并以 {@code max} 封顶。
     */
    static BackoffStrategy exponential(Duration initial, double multiplier, Duration max) {
        Objects.requireNonNull(initial, "initial");
        Objects.requireNonNull(max, "max");
        long initialMillis = initial.toMillis();
        long maxMillis = max.toMillis();
        if (initialMillis < 0 || maxMillis < 0 || initialMillis > maxMillis) {
            throw new IllegalArgumentException("require 0 <= initial <= max");
        }
        if (multiplier < 1.0d) {
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        }
        return attempt -> {
            double value = initialMillis * Math.pow(multiplier, Math.max(0, attempt - 1));
            if (Double.isNaN(value) || value >= maxMillis || value <= 0.0d) {
                return maxMillis;
            }
            return Math.max(0L, (long) value);
        };
    }
}
