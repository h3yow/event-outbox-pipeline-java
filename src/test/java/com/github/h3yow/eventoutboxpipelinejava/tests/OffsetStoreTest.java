package com.github.h3yow.eventoutboxpipelinejava.tests;

import com.github.h3yow.eventoutboxpipelinejava.outbox.offset.InvalidOffsetException;
import com.github.h3yow.eventoutboxpipelinejava.outbox.offset.OffsetRejectReason;
import com.github.h3yow.eventoutboxpipelinejava.outbox.offset.OffsetStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 位点管理单测：越界/失效位点必须被明确拒绝并给出可区分原因。
 */
class OffsetStoreTest {

    private static final Logger log = LoggerFactory.getLogger(OffsetStoreTest.class);
    private static final String GROUP = "g1";
    private static final String AGG = "agg-1";

    private InvalidOffsetException expectReject(OffsetStore store, long offset, long earliest, long latest) {
        return assertThrows(InvalidOffsetException.class,
                () -> store.seek(GROUP, AGG, offset, earliest, latest));
    }

    @Test
    void rejectReasonsAreDistinguishable() {
        OffsetStore store = new OffsetStore();

        InvalidOffsetException negative = expectReject(store, -1, 0, 10);
        log.info("[offset] seek offset=-1 -> rejectReason={} msg='{}'",
                negative.reason(), negative.getMessage());
        assertEquals(OffsetRejectReason.NEGATIVE, negative.reason());

        // 日志已截断 [0,5)，合法区间 [5,11]
        InvalidOffsetException expired = expectReject(store, 4, 5, 10);
        log.info("[offset] seek offset=4 with earliest=5 -> rejectReason={} msg='{}'",
                expired.reason(), expired.getMessage());
        assertEquals(OffsetRejectReason.EXPIRED, expired.reason());

        InvalidOffsetException beyond = expectReject(store, 12, 5, 10);
        log.info("[offset] seek offset=12 with latest=10 -> rejectReason={} msg='{}'",
                beyond.reason(), beyond.getMessage());
        assertEquals(OffsetRejectReason.BEYOND_LATEST, beyond.reason());
    }

    @Test
    void commitAdvancesWithinValidWindow() {
        OffsetStore store = new OffsetStore();
        store.commit(GROUP, AGG, 0, 0, -1);
        store.commit(GROUP, AGG, 3, 0, 5);
        long committed = store.committed(GROUP, AGG, 0);
        log.info("[offset] committed offset={} for aggregate={} group={}", committed, AGG, GROUP);
        assertEquals(3, committed);

        // 截断后旧位点回放必须被拒绝（消费者重启后试图回放到已失效位置）
        InvalidOffsetException ex = assertThrows(InvalidOffsetException.class,
                () -> store.seek(GROUP, AGG, 2, 3, 5));
        log.info("[offset] replay to truncated offset=2 after restart -> rejectReason={}", ex.reason());
        assertEquals(OffsetRejectReason.EXPIRED, ex.reason());
    }

    @Test
    void emptyPartitionOnlyAcceptsEarliest() {
        OffsetStore store = new OffsetStore();
        store.commit(GROUP, AGG, 0, 0, -1);
        InvalidOffsetException ex = expectReject(store, 1, 0, -1);
        log.info("[offset] commit offset=1 on empty partition -> rejectReason={}", ex.reason());
        assertEquals(OffsetRejectReason.BEYOND_LATEST, ex.reason());
    }
}
