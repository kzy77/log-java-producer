package io.github.ossappender.core;
import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.GZIPOutputStream;

/**
 * OSS 上传器：负责将批次日志压缩并上传到指定 bucket/key 前缀。
 * 支持可配置 gzip、重试与指数退避。
 */
public class OssUploader {
    private static final Logger logger = LoggerFactory.getLogger(OssUploader.class);
    private final OSS oss;
    private final String bucket;
    private final String keyPrefix;
    private final String keyPrefixWithSlash;
    private final boolean gzipEnabled;
    private final int maxRetries;
    private final long baseBackoffMs;
    private final long maxBackoffMs;
    private final UploadHooks hooks;
    private static final DateTimeFormatter KEY_TS = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mmssSSS")
            .withZone(ZoneId.of("UTC"));
    /**
     * 构造上传器。
     */
    public OssUploader(String endpoint,
                       String accessKeyId,
                       String accessKeySecret,
                       String bucket,
                       String keyPrefix,
                       boolean gzipEnabled,
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
        this.keyPrefix = keyPrefix != null ? keyPrefix.replaceAll("^/+|/$", "") : "logs";
        this.keyPrefixWithSlash = this.keyPrefix.isEmpty() ? "" : (this.keyPrefix + "/");
        this.gzipEnabled = gzipEnabled;
        this.maxRetries = Math.max(0, maxRetries);
        this.baseBackoffMs = Math.max(100L, baseBackoffMs);
        this.maxBackoffMs = Math.max(this.baseBackoffMs, maxBackoffMs);
        this.hooks = hooks == null ? UploadHooks.noop() : hooks;
    }
    /** 释放底层客户端。 */
    public void close() {
        try { 
            oss.shutdown(); 
        } catch (Throwable throwable) {
            logger.warn("Failed to shutdown OSS client", throwable);
        }
    }
    /**
     * 将一批日志编码为 NDJSON 文本并上传到 OSS。
     */
    public void uploadBatch(List<BatchingQueue.LogEvent> events, int totalBytes) {
        byte[] ndjson = encodeNdjson(events);
        byte[] toUpload;
        boolean compressed = false;
        try {
            if (gzipEnabled) {
                byte[] gz = gzip(ndjson);
                toUpload = gz;
                compressed = true;
            } else {
                toUpload = ndjson;
            }
        } catch (IOException ioException) {
            // 回退到未压缩上传
            logger.warn("GZIP compression failed, fallback to plain NDJSON", ioException);
            toUpload = ndjson;
            compressed = false;
        }
        String objectKey = buildObjectKey();
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(toUpload.length);
        meta.setContentType("application/x-ndjson; charset=utf-8");
        if (compressed) {
            meta.setContentEncoding("gzip");
        }
        int attempt = 0;
        Exception last = null;
        while (attempt <= maxRetries) {
            try {
                oss.putObject(bucket, objectKey, new ByteArrayInputStream(toUpload), meta);
                hooks.onUploadSuccess(objectKey, ndjson.length, toUpload.length);
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
        hooks.onUploadFailure(objectKey, last);
    }
    /** 将事件数组编码为 NDJSON（每行一个 UTF-8 文本）。 */
    private byte[] encodeNdjson(List<BatchingQueue.LogEvent> events) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(256, events.size() * 128))) {
            for (BatchingQueue.LogEvent ev : events) {
                try {
                    out.write(ev.payload);
                    out.write('\n');
                } catch (IOException ignored) { } 
            }
            return out.toByteArray();
        } catch (IOException e) {
            // ByteArrayOutputStream 理论上不会抛出，但为一致性记录
            logger.warn("encodeNdjson unexpected error", e);
            return new byte[0];
        }
    }
    /** GZIP 压缩 */
    private byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(Math.max(256, data.length / 2));
        try (GZIPOutputStream gos = new GZIPOutputStream(bos)) {
            gos.write(data);
        }
        return bos.toByteArray();
    }
    /** 构建对象 key，包含 UTC 时间与随机后缀 */
    private String buildObjectKey() {
        long now = System.currentTimeMillis();
        String ts = KEY_TS.format(Instant.ofEpochMilli(now));
        int rnd = ThreadLocalRandom.current().nextInt(100000, 999999);
        return keyPrefixWithSlash + ts + "-" + rnd + ".ndjson" + (gzipEnabled ? ".gz" : "");
    }
    /** 指数退避（含抖动） */
    private long computeBackoff(int attempt) {
        long exp = Math.min(maxBackoffMs, (long) (baseBackoffMs * Math.pow(2, attempt)));
        long jitter = ThreadLocalRandom.current().nextLong(0, exp / 3 + 1);
        return Math.min(maxBackoffMs, exp + jitter);
    }
}
