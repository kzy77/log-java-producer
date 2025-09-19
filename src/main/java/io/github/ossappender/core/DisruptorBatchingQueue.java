package io.github.ossappender.core;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
/**
 * 基于 LMAX Disruptor 的批处理队列实现：低 GC、低延迟。
 * 与 BatchingQueue 保持相同回调语义。
 */
public final class DisruptorBatchingQueue {
    /**
     * 事件载体，避免频繁分配。
     */
    public static final class LogEventHolder {
        public byte[] payload;
        public long timestampMs;
        void set(byte[] p, long ts) { this.payload = p; this.timestampMs = ts; }
        void clear() {
            this.payload = null;
            this.timestampMs = 0L;
        }
    }
    private final Disruptor<LogEventHolder> disruptor;
    private final RingBuffer<LogEventHolder> ringBuffer;
    private final int batchMaxMessages;
    private final int batchMaxBytes;
    private final long flushIntervalMs;
    // 提示：满时可自旋或丢弃
    private final boolean blockOnFull;
    private final BatchingQueue.BatchConsumer consumer;
    private volatile boolean started = false;
    /**
     * 构造 Disruptor 队列。
     * @param capacity 环形缓冲大小（2 的幂）
     * @param batchMaxMessages 批处理最大条数
     * @param batchMaxBytes 批处理最大字节数
     * @param flushIntervalMs 定时刷新（消费者内部基于时间窗口）
     * @param blockOnFull 满时是否自旋等待（否则直接丢弃）
     * @param multiProducer 是否多生产者
     * @param consumer 批次消费回调
     */
    public DisruptorBatchingQueue(int capacity,
                                  int batchMaxMessages,
                                  int batchMaxBytes,
                                  long flushIntervalMs,
                                  boolean blockOnFull,
                                  boolean multiProducer,
                                  BatchingQueue.BatchConsumer consumer) {
        this.batchMaxMessages = Math.max(1, batchMaxMessages);
        this.batchMaxBytes = Math.max(1, batchMaxBytes);
        this.flushIntervalMs = Math.max(1, flushIntervalMs);
        this.blockOnFull = blockOnFull;
        this.consumer = consumer;
        EventFactory<LogEventHolder> factory = LogEventHolder::new;
        ProducerType type = multiProducer ? ProducerType.MULTI : ProducerType.SINGLE;
        this.disruptor = new Disruptor<>(factory, capacity, Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oss-disruptor-consumer");
            t.setDaemon(true);
            return t;
        }), type, new BlockingWaitStrategy());
        this.disruptor.handleEventsWith(new EventHandler<LogEventHolder>() {
            private List<BatchingQueue.LogEvent> buffer = new ArrayList<>(DisruptorBatchingQueue.this.batchMaxMessages);
            private int bytes = 0;
            private long lastFlush = System.currentTimeMillis();
            @Override
            public void onEvent(LogEventHolder ev, long sequence, boolean endOfBatch) {
                if (ev.payload != null) {
                    int size = ev.payload.length;
                    boolean willExceedCount = buffer.size() + 1 > batchMaxMessages;
                    boolean willExceedBytes = bytes + size > batchMaxBytes;
                    if (willExceedCount || willExceedBytes) {
                        if (!buffer.isEmpty()) {
                            consumer.onBatch(Collections.unmodifiableList(buffer), bytes);
                            buffer.clear();
                            bytes = 0;
                        }
                    }
                    buffer.add(new BatchingQueue.LogEvent(ev.payload, ev.timestampMs));
                    bytes += size;
                    ev.clear();
                }
                long now = System.currentTimeMillis();
                boolean timeUp = (now - lastFlush) >= flushIntervalMs;
                if (endOfBatch || timeUp || buffer.size() >= batchMaxMessages || bytes >= batchMaxBytes) {
                    if (!buffer.isEmpty()) {
                        consumer.onBatch(Collections.unmodifiableList(buffer), bytes);
                        buffer.clear();
                        bytes = 0;
                    }
                    lastFlush = now;
                }
            }
        });
        this.ringBuffer = disruptor.getRingBuffer();
    }
    /** 启动消费者线程 */
    public synchronized void start() {
        if (started) {
            return;
        }
        disruptor.start();
        started = true;
    }
    /** 关闭队列 */
    public synchronized void close() {
        if (!started) {
            return;
        }
        disruptor.shutdown();
        started = false;
    }
    /**
     * 提交事件。
     * @param payload 已编码日志字节
     * @return 是否成功写入环形缓冲
     */
    public boolean offer(byte[] payload) {
        if (!started) {
            return false;
        }
        long ts = System.currentTimeMillis();
        while (true) {
            if (ringBuffer.hasAvailableCapacity(1)) {
                long seq = ringBuffer.next();
                try {
                    LogEventHolder slot = ringBuffer.get(seq);
                    slot.set(payload, ts);
                } finally {
                    ringBuffer.publish(seq);
                }
                return true;
            }
            if (!blockOnFull) {
                // 丢弃
                return false;
            }
            // 自旋等待空间可用（JDK8 兼容）并退避，避免占用CPU过高
            try {
                Thread.sleep(1L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }
}
