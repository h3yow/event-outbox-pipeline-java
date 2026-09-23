package com.github.h3yow.eventoutboxpipelinejava.outbox.offset;

/**
 * 位点被拒绝的可区分原因。
 */
public enum OffsetRejectReason {
    /** 负数位点，任何情况下都不合法。 */
    NEGATIVE,
    /** 位点为正但对应日志已被截断，永久失效。 */
    EXPIRED,
    /** 位点超出当前已写入的最新位点（尚无对应事件）。 */
    BEYOND_LATEST,
    /** 位点属于未知的消费者组/聚合。 */
    UNKNOWN_PARTITION
}
