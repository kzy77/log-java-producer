package io.github.ossappender.oss;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.ObjectMetadata;
import io.github.ossappender.core.BinaryUploader;
import io.github.ossappender.core.UploadHooks;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

public class AliyunOssUploader implements BinaryUploader {
    private final OSS oss;
    private final String bucket;
    private final String keyPrefix;
    private final String keyPrefixWithSlash;
    private final int maxRetries;
    private final long baseBackoffMs;
    private final long maxBackoffMs;
    private final UploadHooks hooks;

    private static final DateTimeFormatter KEY_TS = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mmssSSS")
            .withZone(ZoneId.of("UTC"));

    public AliyunOssUploader(String endpoint,
                             String accessKeyId,
                             String accessKeySecret,
                             String bucket,
                             String keyPrefix,
                             int maxRetries,
                             long baseBackoffMs,
                             long maxBackoffMs,
                             UploadHooks hooks) {
        ClientBuilderConfiguration conf = new ClientBuilderConfiguration();
        conf.setConnectionTimeout(10_000);
        conf.setSocketTimeout(30_000);
        conf.setMaxConnections(64);
        this.oss = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret, conf);
        this.bucket = bucket;
        this.keyPrefix = keyPrefix != null ? keyPrefix.replaceAll("^/+|/+$", "") : "logs";
        this.keyPrefixWithSlash = this.keyPrefix.isEmpty() ? "" : (this.keyPrefix + "/");
        this.maxRetries = Math.max(0, maxRetries);
        this.baseBackoffMs = Math.max(100L, baseBackoffMs);
        this.maxBackoffMs = Math.max(this.baseBackoffMs, maxBackoffMs);
        this.hooks = hooks == null ? UploadHooks.noop() : hooks;
    }

    @Override
    public void upload(String objectKey, byte[] data, String contentType, String contentEncoding) throws Exception {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(data.length);
        if (contentType != null) {
            meta.setContentType(contentType);
        }
        if (contentEncoding != null) {
            meta.setContentEncoding(contentEncoding);
        }

        int attempt = 0;
        Exception last = null;
        while (attempt <= maxRetries) {
            try {
                oss.putObject(bucket, objectKey, new ByteArrayInputStream(data), meta);
                hooks.onUploadSuccess(objectKey, data.length, data.length);
                return;
            } catch (Exception e) {
                last = e;
                if (attempt >= maxRetries) {
                    break;
                }
                long backoff = computeBackoff(attempt);
                hooks.onUploadRetry(objectKey, attempt + 1, backoff, e);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
                attempt++;
            }
        }
        if (last != null) {
            throw last;
        }
    }

    public void close() {
        if (oss != null) {
            oss.shutdown();
        }
    }

    private String buildObjectKey(String contentEncoding) {
        long now = System.currentTimeMillis();
        String ts = KEY_TS.format(Instant.ofEpochMilli(now));
        int rnd = ThreadLocalRandom.current().nextInt(100000, 999999);
        String suffix = ".log";
        if ("gzip".equals(contentEncoding)) {
            suffix = ".log.gz";
        }
        return keyPrefixWithSlash + ts + "-" + rnd + suffix;
    }

    private long computeBackoff(int attempt) {
        long exp = Math.min(maxBackoffMs, (long) (baseBackoffMs * Math.pow(2, attempt)));
        long jitter = ThreadLocalRandom.current().nextLong(0, exp / 3 + 1);
        return Math.min(maxBackoffMs, exp + jitter);
    }
}
