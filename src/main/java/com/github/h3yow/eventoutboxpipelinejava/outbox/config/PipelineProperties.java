package com.github.h3yow.eventoutboxpipelinejava.outbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 流水线可配置项，前缀 {@code outbox.pipeline}。
 */
@ConfigurationProperties(prefix = "outbox.pipeline")
public class PipelineProperties {

    /** 默认消费者组。 */
    private String consumerGroup = "default-group";
    /** 最大处理尝试次数（含首次）；超过后转入死信区。 */
    private int maxAttempts = 3;
    /** 初始退避。 */
    private Duration initialBackoff = Duration.ofMillis(100);
    /** 退避倍数。 */
    private double backoffMultiplier = 2.0;
    /** 退避上限。 */
    private Duration maxBackoff = Duration.ofSeconds(5);
    /** 工作线程数（并发消费）。 */
    private int workerThreads = 4;
    /** 兜底扫描周期（正常情况下入队/重试完成会立即唤醒，不等待该周期）。 */
    private Duration pollInterval = Duration.ofMillis(200);

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Duration getInitialBackoff() {
        return initialBackoff;
    }

    public void setInitialBackoff(Duration initialBackoff) {
        this.initialBackoff = initialBackoff;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public Duration getMaxBackoff() {
        return maxBackoff;
    }

    public void setMaxBackoff(Duration maxBackoff) {
        this.maxBackoff = maxBackoff;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }
}
