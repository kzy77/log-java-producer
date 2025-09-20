package io.github.ossappender.s3;

/**
 * S3兼容存储配置工厂：根据endpoint自动检测云厂商并提供最佳配置。
 */
public final class S3CompatibleConfig {

    /**
     * 云存储提供商枚举
     */
    public enum Provider {
        AWS_S3,
        ALIBABA_OSS,
        TENCENT_COS,
        MINIO,
        CLOUDFLARE_R2,
        GENERIC_S3
    }

    /**
     * 配置信息
     */
    public static class Config {
        public final Provider provider;
        public final String region;
        public final boolean forcePathStyle;
        public final String normalizedEndpoint;

        public Config(Provider provider, String region, boolean forcePathStyle, String normalizedEndpoint) {
            this.provider = provider;
            this.region = region;
            this.forcePathStyle = forcePathStyle;
            this.normalizedEndpoint = normalizedEndpoint;
        }
    }

    /**
     * 根据endpoint自动检测配置
     */
    public static Config detectConfig(String endpoint, String region) {
        if (endpoint == null || endpoint.trim().isEmpty()) {
            // 默认AWS S3
            return new Config(Provider.AWS_S3, region != null ? region : "us-east-1", false, null);
        }

        String lowerEndpoint = endpoint.toLowerCase().trim();

        // 阿里云OSS
        if (lowerEndpoint.contains("aliyuncs.com")) {
            String detectedRegion = extractOssRegion(lowerEndpoint, region);
            return new Config(Provider.ALIBABA_OSS, detectedRegion, false, endpoint);
        }

        // 腾讯云COS
        if (lowerEndpoint.contains("myqcloud.com")) {
            String detectedRegion = extractCosRegion(lowerEndpoint, region);
            return new Config(Provider.TENCENT_COS, detectedRegion, false, endpoint);
        }

        // MinIO (检测本地地址或包含minio的域名)
        if (lowerEndpoint.contains("localhost") ||
            lowerEndpoint.contains("127.0.0.1") ||
            lowerEndpoint.contains("minio") ||
            lowerEndpoint.matches(".*:\\d+.*")) { // 包含端口号
            return new Config(Provider.MINIO, "us-east-1", true, endpoint);
        }

        // Cloudflare R2
        if (lowerEndpoint.contains("r2.cloudflarestorage.com")) {
            return new Config(Provider.CLOUDFLARE_R2, "auto", false, endpoint);
        }

        // AWS S3 (包括s3.amazonaws.com及其区域变体)
        if (lowerEndpoint.contains("amazonaws.com")) {
            String detectedRegion = extractAwsRegion(lowerEndpoint, region);
            return new Config(Provider.AWS_S3, detectedRegion, false, endpoint);
        }

        // 通用S3兼容存储
        return new Config(Provider.GENERIC_S3, region != null ? region : "us-east-1", false, endpoint);
    }

    /**
     * 从阿里云OSS endpoint提取区域
     */
    private static String extractOssRegion(String endpoint, String fallback) {
        // 格式: https://oss-cn-hangzhou.aliyuncs.com
        if (endpoint.contains("oss-")) {
            int start = endpoint.indexOf("oss-") + 4;
            int end = endpoint.indexOf(".", start);
            if (end > start) {
                return endpoint.substring(start, end);
            }
        }
        return fallback != null ? fallback : "cn-hangzhou";
    }

    /**
     * 从腾讯云COS endpoint提取区域
     */
    private static String extractCosRegion(String endpoint, String fallback) {
        // 格式: https://cos.ap-beijing.myqcloud.com
        if (endpoint.contains("cos.")) {
            int start = endpoint.indexOf("cos.") + 4;
            int end = endpoint.indexOf(".", start);
            if (end > start) {
                return endpoint.substring(start, end);
            }
        }
        return fallback != null ? fallback : "ap-beijing";
    }

    /**
     * 从AWS S3 endpoint提取区域
     */
    private static String extractAwsRegion(String endpoint, String fallback) {
        // 格式: https://s3.us-west-2.amazonaws.com 或 https://s3-us-west-2.amazonaws.com
        if (endpoint.contains("s3.") || endpoint.contains("s3-")) {
            String[] parts = endpoint.split("\\.");
            for (String part : parts) {
                if (part.startsWith("s3-") && part.length() > 3) {
                    return part.substring(3);
                }
                if (isAwsRegion(part)) {
                    return part;
                }
            }
        }
        return fallback != null ? fallback : "us-east-1";
    }

    /**
     * 检查是否为有效的AWS区域格式
     */
    private static boolean isAwsRegion(String region) {
        return region.matches("^[a-z]{2}-[a-z]+-\\d+$");
    }
}