package io.github.ossappender.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.ObjectMetadata;
import io.github.ossappender.core.BinaryUploader;

import java.util.Objects;

/**
 * Aliyun OSS 实现的 BinaryUploader。
 * 支持连接复用，线程安全，适用于高吞吐上传。
 */
public final class AliyunOssUploader implements BinaryUploader, AutoCloseable {
    private final OSS client;
    private final String bucket;

    /**
     * 构造上传器。
     * @param endpoint OSS Endpoint，如 https://oss-cn-hangzhou.aliyuncs.com
     * @param accessKeyId AK
     * @param accessKeySecret SK
     * @param bucket 目标桶
     */
    public AliyunOssUploader(String endpoint, String accessKeyId, String accessKeySecret, String bucket) {
        this.client = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
        this.bucket = Objects.requireNonNull(bucket, "bucket");
    }

    /**
     * 执行上传。
     */
    @Override
    public void upload(String objectKey, byte[] contentBytes, String contentType, String contentEncoding) {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(contentBytes.length);
        if (contentType != null) meta.setContentType(contentType);
        if (contentEncoding != null) meta.setContentEncoding(contentEncoding);
        client.putObject(bucket, objectKey, new java.io.ByteArrayInputStream(contentBytes), meta);
    }

    /**
     * 关闭底层客户端。
     */
    @Override
    public void close() {
        client.shutdown();
    }
}


