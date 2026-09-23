package com.github.h3yow.eventoutboxpipelinejava.offset;

/**
 * 位点合法但当前不可用：对应事件已经进入死信区，位点回放被明确拒绝。
 */
public class OffsetInvalidException extends RuntimeException {

    public OffsetInvalidException(String message) {
        super(message);
    }
}
