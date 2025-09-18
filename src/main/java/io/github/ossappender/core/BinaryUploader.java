package io.github.ossappender.core;

/**
 * 通用二进制上传接口：用于将编码后的批量日志以对象存储形式上传。
 */
public interface BinaryUploader {

    /**
     * 执行上传。
     * @param objectKey 目标对象Key（例如OSS路径）
     * @param contentBytes 内容字节
     * @param contentType 媒体类型（可为 null）
     * @param contentEncoding 内容编码（如 gzip，可为 null）
     * @throws Exception 上传失败时抛出
     */
    void upload(String objectKey, byte[] contentBytes, String contentType, String contentEncoding) throws Exception;
}


