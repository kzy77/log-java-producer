package io.github.ossappender.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

/**
 * S3兼容对象存储上传器：支持AWS S3、阿里云OSS、腾讯云COS、MinIO等所有S3兼容存储。
 * 基于AWS SDK v2构建，提供统一的对象存储接口。
 */
public final class S3CompatibleUploader implements AutoCloseable {

    private static final DateTimeFormatter KEY_TS = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mmssSSS")
            .withZone(ZoneOffset.UTC);

    private final S3Client s3Client;
    private final String bucket;
    private final String keyPrefix;
    private final String keyPrefixWithSlash;
    private final int maxRetries;
    private final long baseBackoffMs;
    private final long maxBackoffMs;

    /**
     * 构造S3兼容上传器。
     *
     * @param endpoint 存储服务端点，如 https://oss-cn-hangzhou.aliyuncs.com 或 https://s3.amazonaws.com
     * @param region 区域，如 cn-hangzhou 或 us-east-1
     * @param accessKeyId 访问密钥ID
     * @param accessKeySecret 访问密钥Secret
     * @param bucket 存储桶名称
     * @param keyPrefix 对象键前缀，默认 "logs/"
     * @param maxRetries 最大重试次数
     * @param baseBackoffMs 基础退避时间（毫秒）
     * @param maxBackoffMs 最大退避时间（毫秒）
     * @param forcePathStyle 是否强制路径风格，MinIO等需要设置为true
     */
    public S3CompatibleUploader(String endpoint,
                               String region,
                               String accessKeyId,
                               String accessKeySecret,
                               String bucket,
                               String keyPrefix,
                               int maxRetries,
                               long baseBackoffMs,
                               long maxBackoffMs,
                               boolean forcePathStyle) {

        this.bucket = bucket;
        this.keyPrefix = keyPrefix != null ? keyPrefix.replaceAll("^/+|/+$", "") : "logs";
        this.keyPrefixWithSlash = this.keyPrefix.isEmpty() ? "" : (this.keyPrefix + "/");
        this.maxRetries = Math.max(0, maxRetries);
        this.baseBackoffMs = Math.max(100L, baseBackoffMs);
        this.maxBackoffMs = Math.max(this.baseBackoffMs, maxBackoffMs);

        // 构建S3客户端
        S3ClientBuilder clientBuilder = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKeyId, accessKeySecret)))
                .region(Region.of(region != null ? region : "us-east-1"));

        // 自定义端点（非AWS S3）
        if (endpoint != null && !endpoint.trim().isEmpty()) {
            clientBuilder.endpointOverride(URI.create(endpoint));
        }

        // 路径风格访问（MinIO等需要）
        if (forcePathStyle) {
            clientBuilder.forcePathStyle(true);
        }

        this.s3Client = clientBuilder.build();
    }

    /**
     * 简化构造函数：使用默认参数
     */
    public S3CompatibleUploader(String endpoint,
                               String region,
                               String accessKeyId,
                               String accessKeySecret,
                               String bucket) {
        this(endpoint, region, accessKeyId, accessKeySecret, bucket,
             "logs", 5, 200L, 10000L, false);
    }

    /**
     * 上传内容到S3兼容存储
     */
    public void upload(String objectKey, byte[] contentBytes, String contentType, String contentEncoding) throws Exception {
        // 如果未提供objectKey，自动生成
        String finalObjectKey = objectKey != null ? objectKey : buildObjectKey(contentEncoding);

        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(bucket)
                .key(finalObjectKey)
                .contentLength((long) contentBytes.length);

        if (contentType != null) {
            requestBuilder.contentType(contentType);
        }

        if (contentEncoding != null) {
            requestBuilder.contentEncoding(contentEncoding);
        }

        PutObjectRequest putRequest = requestBuilder.build();
        RequestBody requestBody = RequestBody.fromBytes(contentBytes);

        // 重试逻辑
        Exception lastException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                s3Client.putObject(putRequest, requestBody);
                return; // 成功上传
            } catch (S3Exception e) {
                lastException = e;
                if (attempt >= maxRetries) {
                    break; // 最后一次重试失败
                }

                // 指数退避
                long backoff = computeBackoff(attempt);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Upload interrupted", ie);
                }
            }
        }

        throw new RuntimeException("Failed to upload after " + (maxRetries + 1) + " attempts", lastException);
    }

    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    /**
     * 构建对象键，包含UTC时间戳和随机后缀
     */
    private String buildObjectKey(String contentEncoding) {
        long now = System.currentTimeMillis();
        String ts = KEY_TS.format(Instant.ofEpochMilli(now));
        int rnd = ThreadLocalRandom.current().nextInt(100000, 999999);
        String suffix = ".ndjson";
        if ("gzip".equals(contentEncoding)) {
            suffix = ".ndjson.gz";
        }
        return keyPrefixWithSlash + ts + "-" + rnd + suffix;
    }

    /**
     * 计算指数退避时间（含抖动）
     */
    private long computeBackoff(int attempt) {
        long exp = Math.min(maxBackoffMs, (long) (baseBackoffMs * Math.pow(2, attempt)));
        long jitter = ThreadLocalRandom.current().nextLong(0, exp / 3 + 1);
        return Math.min(maxBackoffMs, exp + jitter);
    }

    /**
     * 创建S3兼容上传器的便捷方法（使用自动配置检测）
     */
    public static S3CompatibleUploader create(String endpoint,
                                            String region,
                                            String accessKeyId,
                                            String accessKeySecret,
                                            String bucket,
                                            String keyPrefix) {
        S3CompatibleConfig.Config config = S3CompatibleConfig.detectConfig(endpoint, region);
        return new S3CompatibleUploader(
                config.normalizedEndpoint,
                config.region,
                accessKeyId,
                accessKeySecret,
                bucket,
                keyPrefix,
                5, // maxRetries
                200L, // baseBackoffMs
                10000L, // maxBackoffMs
                config.forcePathStyle
        );
    }
}