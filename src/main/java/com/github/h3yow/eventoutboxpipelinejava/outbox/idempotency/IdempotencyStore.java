package com.github.h3yow.eventoutboxpipelinejava.outbox.idempotency;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 幂等记录：保证同一 eventId 的业务副作用只生效一次。
 *
 * <p>以 eventId 为键记录处理终态：
 * <ul>
 *     <li>{@link ResultState#PROCESSED}：业务处理成功，副作用已生效；</li>
 *     <li>{@link ResultState#DEAD}：重试耗尽已转入死信区（业务从未生效）。</li>
 * </ul>
 *
 * <p>每次处理尝试先经 {@link #acquire} 原子占坑。终态一旦形成便不可逆，
 * 消费者重启或位点回放后再次读到同一事件时，只会拿到终态而跳过业务，
 * 杜绝重复副作用；处理失败且仍有重试额度时必须调用 {@link #release}
 * 让出占坑，使下一次重试能够重新获取。
 */
public final class IdempotencyStore {

    /** 单个事件的处理终态。 */
    public enum ResultState {
        /** 业务处理已成功，副作用已生效。 */
        PROCESSED,
        /** 重试耗尽，事件已转入死信区。 */
        DEAD
    }

    public record Record(String eventId, ResultState state, long aggregateOffset, Instant recordedAt) {
    }

    /** 占坑结果：获准执行业务，或事件已有终态。 */
    public sealed interface AcquireResult permits AcquireResult.Acquired, AcquireResult.AlreadyTerminal {

        /** 此前无标记，本次调用原子占坑成功，调用方应当执行业务。 */
        record Acquired(String eventId) implements AcquireResult {
        }

        /** 事件已有终态，调用方不得再执行业务；依据 state 区分“已成功/已死信”。 */
        record AlreadyTerminal(Record record) implements AcquireResult {
        }
    }

    /** 占坑标记（非终态，对查询不可见）。 */
    private record InFlight(String eventId, long aggregateOffset, Instant startedAt) {
    }

    private final ConcurrentMap<String, Object> table = new ConcurrentHashMap<>();
    private final Clock clock;

    public IdempotencyStore() {
        this(Clock.systemUTC());
    }

    IdempotencyStore(Clock clock) {
        this.clock = clock;
    }

    /**
     * 原子地占坑或读取终态。
     *
     * @throws IllegalStateException 同一事件正被另一线程处理（正常由同聚合串行保证不会发生，
     *                               一旦发生即说明上层并发约束被破坏，立即显式失败）
     */
    public AcquireResult acquire(String eventId, long aggregateOffset) {
        Object existing = table.putIfAbsent(
                eventId, new InFlight(eventId, aggregateOffset, Instant.now(clock)));
        if (existing == null) {
            return new AcquireResult.Acquired(eventId);
        }
        if (existing instanceof Record r) {
            return new AcquireResult.AlreadyTerminal(r);
        }
        throw new IllegalStateException(
                "event " + eventId + " is already being processed by another worker");
    }

    /**
     * 处理失败且仍将重试时释放占坑；仅当当前标记仍是本次的在途标记时才移除，
     * 绝不覆盖终态。
     */
    public void release(String eventId) {
        Object marker = table.get(eventId);
        if (marker instanceof InFlight) {
            table.remove(eventId, marker);
        }
    }

    /** 清除终态记录（用于死信重放：清除后事件可被重新执行）。 */
    public void clear(String eventId) {
        table.remove(eventId);
    }

    /** 登记处理成功（终态）。必须由占坑成功的一方调用；终态不可逆。 */
    public void markProcessed(String eventId, long aggregateOffset) {
        table.put(eventId, new Record(eventId, ResultState.PROCESSED, aggregateOffset,
                Instant.now(clock)));
    }

    /** 登记进入死信区（终态）。必须由占坑成功的一方调用；终态不可逆。 */
    public void markDead(String eventId, long aggregateOffset) {
        table.put(eventId, new Record(eventId, ResultState.DEAD, aggregateOffset,
                Instant.now(clock)));
    }

    /** 查询终态；无记录或仍在途（尚未得出结果）时返回 empty。 */
    public Optional<Record> find(String eventId) {
        Object marker = table.get(eventId);
        return marker instanceof Record r ? Optional.of(r) : Optional.empty();
    }
}
