package io.github.ossappender.core;

/**
 * 上传与队列事件的钩子回调接口。
 * 实现方可用于监控指标、报警或自定义容错策略。
 */
public interface UploadHooks {

    /**
     * 当日志被丢弃时触发（例如队列满且未启用阻塞）。
     * @param droppedBytes 被丢弃日志字节数
     * @param currentQueueSize 当前队列条数（近似）
     */
    void onDropped(int droppedBytes, int currentQueueSize);

    /**
     * 单次对象上传成功。
     */
    void onUploadSuccess(String objectKey, int originalBytes, int compressedBytes);

    /**
     * 上传失败且将进行重试。
     */
    void onUploadRetry(String objectKey, int attempt, long backoffMillis, Exception error);

    /**
     * 最终失败。
     */
    void onUploadFailure(String objectKey, Exception error);

    /**
     * 返回一个无操作实现。
     */
    static UploadHooks noop() {
        return new UploadHooks() {
            @Override public void onDropped(int droppedBytes, int currentQueueSize) {}
            @Override public void onUploadSuccess(String objectKey, int originalBytes, int compressedBytes) {}
            @Override public void onUploadRetry(String objectKey, int attempt, long backoffMillis, Exception error) {}
            @Override public void onUploadFailure(String objectKey, Exception error) {}
        };
    }
}


