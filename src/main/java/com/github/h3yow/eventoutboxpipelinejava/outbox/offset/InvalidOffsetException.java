package com.github.h3yow.eventoutboxpipelinejava.outbox.offset;

/**
 * 越界或已失效的位点被提交/回放时抛出，携带可区分的 {@link OffsetRejectReason}。
 */
public class InvalidOffsetException extends RuntimeException {

    private final OffsetRejectReason reason;
    private final String consumerGroup;
    private final String aggregateId;
    private final long offset;

    public InvalidOffsetException(OffsetRejectReason reason,
                                  String consumerGroup,
                                  String aggregateId,
                                  long offset,
                                  String message) {
        super(message);
        this.reason = reason;
        this.consumerGroup = consumerGroup;
        this.aggregateId = aggregateId;
        this.offset = offset;
    }

    public OffsetRejectReason reason() {
        return reason;
    }

    public String consumerGroup() {
        return consumerGroup;
    }

    public String aggregateId() {
        return aggregateId;
    }

    public long offset() {
        return offset;
    }
}
