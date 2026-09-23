package com.github.h3yow.eventoutboxpipelinejava.outbox.concurrent;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 归属执行器：同一 key 的任务严格串行（FIFO），不同 key 的任务可并行。
 *
 * <p>实现模型：每个 key 维护一条非阻塞任务队列，一个任务在工作线程上运行时，
 * 该队列后续任务只是排队等待，<b>不占用额外线程</b>；工作线程池被所有 key 共享。
 * 因此单个 key 上的慢事件只会阻塞该 key 自身，无关 key 的事件可立即被其他线程领取推进。
 *
 * <p>{@link #submit} 时若该 key 尚无在运行任务，则把整条队列“注册”到调度器并由一个
 * 工作线程领取；任务运行结束后该工作线程继续执行同 key 的下一个任务，直到队列排空。
 */
public final class AffiliatedExecutor implements AutoCloseable {

    /** 单条 key 队列：FIFO 任务链，{@code running} 表示是否已有工作者负责执行它。 */
    private static final class Lane {
        private final Deque<Runnable> tasks = new ConcurrentLinkedDeque<>();
        private volatile boolean running;
    }

    private final Map<String, Lane> lanes = new HashMap<>();
    private final ReentrantLock lanesLock = new ReentrantLock();
    private final ExecutorService pool;

    public AffiliatedExecutor(int workerThreads) {
        if (workerThreads < 1) {
            throw new IllegalArgumentException("workerThreads must be >= 1");
        }
        AtomicInteger idx = new AtomicInteger();
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "outbox-worker-" + idx.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        this.pool = Executors.newFixedThreadPool(workerThreads, tf);
    }

    /** 提交归属 {@code key} 的任务；同 key 任务按提交顺序串行执行，不同 key 可并行。 */
    public void submit(String key, Runnable task) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        Lane lane;
        boolean shouldSchedule = false;
        lanesLock.lock();
        try {
            lane = lanes.computeIfAbsent(key, k -> new Lane());
            lane.tasks.addLast(task);
            if (!lane.running) {
                lane.running = true;
                shouldSchedule = true;
            }
        } finally {
            lanesLock.unlock();
        }
        if (shouldSchedule) {
            scheduleLane(lane);
        }
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private void scheduleLane(Lane lane) {
        pool.execute(() -> runLane(lane));
    }

    /** 连续排空单个 lane；队列为空时释放 running 标记，期间有新任务则继续负责。 */
    private void runLane(Lane lane) {
        while (true) {
            Runnable task = lane.tasks.pollFirst();
            if (task == null) {
                lanesLock.lock();
                try {
                    if (lane.tasks.isEmpty()) {
                        lane.running = false;
                        return;
                    }
                    // 释放标记前又有任务入队：继续由本工作者处理。
                } finally {
                    lanesLock.unlock();
                }
                continue;
            }
            try {
                task.run();
            } catch (Throwable t) {
                // 任务内部必须自行处理异常；这里兜底以免工作线程死亡。
                Thread.currentThread().getUncaughtExceptionHandler()
                        .uncaughtException(Thread.currentThread(), t);
            }
        }
    }

    public void shutdown() {
        pool.shutdown();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return pool.awaitTermination(timeout, unit);
    }

    @Override
    public void close() {
        pool.shutdownNow();
    }
}
