package com.github.h3yow.eventoutboxpipelinejava.offset;

/**
 * 位点越界：超过分区已知最大位点（含原因码，便于调用方区分）。
 */
public class OffsetOutOfRangeException extends RuntimeException {

    public enum Reason {
        NEGATIVE_OFFSET,
        OFFSET_TRUNCATED,
        OFFSET_TOO_HIGH
    }

    private final Reason reason;

    public OffsetOutOfRangeException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }
}
