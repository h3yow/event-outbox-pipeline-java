package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.outbox.retry.BackoffStrategy;
import com.github.h3yow.eventoutboxpipelinejava.outbox.retry.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重试策略单测：验证重试判定依据（attempt vs maxAttempts）与退避序列。
 */
class RetryPolicyTest {

    private static final Logger log = LoggerFactory.getLogger(RetryPolicyTest.class);

    @Test
    void maxAttemptsOneMeansNoRetry() {
        RetryPolicy policy = new RetryPolicy(1, BackoffStrategy.fixed(Duration.ofMillis(10)));
        log.info("[retry-policy] maxAttempts=1: shouldRetry(attempt=1)={} (expect false, 首次失败即死信)",
                policy.shouldRetry(1));
        assertFalse(policy.shouldRetry(1));
    }

    @Test
    void threeAttemptsAllowTwoRetries() {
        RetryPolicy policy = new RetryPolicy(3, BackoffStrategy.fixed(Duration.ofMillis(10)));
        log.info("[retry-policy] maxAttempts=3: shouldRetry(1)={} shouldRetry(2)={} shouldRetry(3)={}",
                policy.shouldRetry(1), policy.shouldRetry(2), policy.shouldRetry(3));
        assertTrue(policy.shouldRetry(1));
        assertTrue(policy.shouldRetry(2));
        assertFalse(policy.shouldRetry(3), "第 3 次失败时重试额度耗尽，应转入死信区");
    }

    @Test
    void exponentialBackoffGrowsAndIsCapped() {
        RetryPolicy policy = new RetryPolicy(5,
                BackoffStrategy.exponential(Duration.ofMillis(100), 2.0, Duration.ofMillis(1000)));
        long d1 = policy.backoffAfter(1).toMillis();
        long d2 = policy.backoffAfter(2).toMillis();
        long d4 = policy.backoffAfter(4).toMillis();
        long d5 = policy.backoffAfter(5).toMillis();
        log.info("[retry-policy] exponential backoff: attempt1={}ms attempt2={}ms attempt4={}ms attempt5={}ms (capped at 1000ms)",
                d1, d2, d4, d5);
        assertEquals(100, d1);
        assertEquals(200, d2);
        assertEquals(800, d4);
        assertEquals(1000, d5, "退避必须被 maxBackoff 封顶");
    }

    @Test
    void invalidConfigIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicy(0, BackoffStrategy.fixed(Duration.ZERO)));
        assertThrows(IllegalArgumentException.class,
                () -> BackoffStrategy.exponential(Duration.ofMillis(-1), 2.0, Duration.ofMillis(10)));
        log.info("[retry-policy] invalid configuration rejected as expected");
    }
}
