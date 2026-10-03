package com.github.h3yow.eventoutboxpipelinejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * event.outbox.* 配置。
 */
@ConfigurationProperties(prefix = "event.outbox")
public class EventPipelineProperties {

    private int partitionCount = 4;
    private int workerCount = 4;
    private int maxAttempts = 3;
    private Duration initialBackoff = Duration.ofMillis(50);
    private Duration maxBackoff = Duration.ofSeconds(2);
    private boolean autoStart = false;
    /**
     * 后台安全清理的周期；0 或负数表示不启动后台清理（仍可手动 compactLog()）。
     */
    private Duration retentionInterval = Duration.ZERO;

    public int getPartitionCount() { return partitionCount; }
    public void setPartitionCount(int partitionCount) { this.partitionCount = partitionCount; }
    public int getWorkerCount() { return workerCount; }
    public void setWorkerCount(int workerCount) { this.workerCount = workerCount; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public Duration getInitialBackoff() { return initialBackoff; }
    public void setInitialBackoff(Duration initialBackoff) { this.initialBackoff = initialBackoff; }
    public Duration getMaxBackoff() { return maxBackoff; }
    public void setMaxBackoff(Duration maxBackoff) { this.maxBackoff = maxBackoff; }
    public boolean isAutoStart() { return autoStart; }
    public void setAutoStart(boolean autoStart) { this.autoStart = autoStart; }
    public Duration getRetentionInterval() { return retentionInterval; }
    public void setRetentionInterval(Duration retentionInterval) { this.retentionInterval = retentionInterval; }
}
