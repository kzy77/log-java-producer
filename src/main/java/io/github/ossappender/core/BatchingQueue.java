package io.github.ossappender.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 批处理内存队列：支持条数/字节阈值与定时刷新，将日志事件聚合成批次。
 * 线程安全，单消费者模型；生产者可配置为阻塞或丢弃。
 */
public class BatchingQueue {

    /** 单条日志的封装结构 */
    public static class LogEvent {
        public final byte[] payload;
        public final long timestampMs;

        /**
         * 构造一条日志事件。
         * @param payload 已编码的日志字节
         * @param timestampMs 事件时间戳（毫秒）
         */
        public LogEvent(byte[] payload, long timestampMs) {
            this.payload = payload;
            this.timestampMs = timestampMs;
        }
    }

    /** 批次回调接口 */
    public interface BatchConsumer {
        /**
         * 当达到阈值或超时触发时调用，返回是否已成功接收处理该批次。
         */
        boolean onBatch(List<LogEvent> events, int totalBytes);
    }

    private final BlockingQueue<LogEvent> queue;
    private final int batchMaxMessages;
    private final int batchMaxBytes;
    private final long flushIntervalMs;
    private final boolean blockOnFull;
    private final BatchConsumer consumer;

    private final ScheduledExecutorService scheduler;
    private volatile boolean started = false;
    private volatile boolean closed = false;

    /**
     * 创建批处理队列实例。
     * @param capacity 队列容量（条）
     * @param batchMaxMessages 批处理最大条数
     * @param batchMaxBytes 批处理最大字节数
     * @param flushIntervalMs 定时刷新间隔（毫秒）
     * @param blockOnFull 队列满时是否阻塞生产者，否则丢弃
     * @param consumer 批次消费回调
     */
    public BatchingQueue(int capacity,
                         int batchMaxMessages,
                         int batchMaxBytes,
                         long flushIntervalMs,
                         boolean blockOnFull,
                         BatchConsumer consumer) {
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.batchMaxMessages = Math.max(1, batchMaxMessages);
        this.batchMaxBytes = Math.max(1, batchMaxBytes);
        this.flushIntervalMs = Math.max(1, flushIntervalMs);
        this.blockOnFull = blockOnFull;
        this.consumer = consumer;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "oss-appender-batch-flusher");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动定时刷新任务。
     */
    public synchronized void start() {
        if (started) return;
        started = true;
        scheduler.scheduleAtFixedRate(this::flushIfNeeded, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 关闭队列，尽力刷新剩余数据。
     */
    public synchronized void close() {
        if (closed) return;
        closed = true;
        scheduler.shutdown();
        flushAll();
    }

    /**
     * 放入一条日志。
     * @param payload 已编码日志字节
     * @return 是否成功入队
     */
    public boolean offer(byte[] payload) {
        if (closed) return false;
        LogEvent ev = new LogEvent(payload, System.currentTimeMillis());
        if (blockOnFull) {
            try {
                queue.put(ev);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        } else {
            return queue.offer(ev);
        }
    }

    private void flushIfNeeded() {
        drainAndConsume(false);
    }

    private void flushAll() {
        drainAndConsume(true);
    }

    /**
     * 将队列数据按阈值聚合，并回调消费。
     */
    private void drainAndConsume(boolean drainAll) {
        if (queue.isEmpty()) return;

        List<LogEvent> buffer = new ArrayList<>(batchMaxMessages);
        int bytes = 0;

        while (!queue.isEmpty()) {
            LogEvent ev = queue.poll();
            if (ev == null) break;
            int size = ev.payload == null ? 0 : ev.payload.length;

            boolean willExceedCount = buffer.size() + 1 > batchMaxMessages;
            boolean willExceedBytes = bytes + size > batchMaxBytes;

            if (willExceedCount || willExceedBytes) {
                if (!buffer.isEmpty()) {
                    consumer.onBatch(Collections.unmodifiableList(buffer), bytes);
                    buffer.clear();
                    bytes = 0;
                }
            }

            buffer.add(ev);
            bytes += size;

            if (!drainAll && (buffer.size() >= batchMaxMessages || bytes >= batchMaxBytes)) {
                consumer.onBatch(Collections.unmodifiableList(buffer), bytes);
                buffer.clear();
                bytes = 0;
            }
        }

        if (!buffer.isEmpty()) {
            consumer.onBatch(Collections.unmodifiableList(buffer), bytes);
        }
    }
}


