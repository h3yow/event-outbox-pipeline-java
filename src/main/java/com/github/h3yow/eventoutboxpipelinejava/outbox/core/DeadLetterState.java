package com.github.h3yow.eventoutboxpipelinejava.outbox.core;

/**
 * 死信区条目的生命周期状态。
 */
public enum DeadLetterState {
    /** 刚转入死信区，等待人工/外部介入。 */
    OPEN,
    /** 已被重放（重新进入消费流程）。 */
    REPLAYED,
    /** 已被明确丢弃（有审计记录，非静默丢弃）。 */
    DISCARDED
}
