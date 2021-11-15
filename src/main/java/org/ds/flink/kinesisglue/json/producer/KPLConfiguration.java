package org.ds.flink.kinesisglue.json.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//See https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer-sample/default_config.properties
public class KPLConfiguration {
    private final static Logger logger = LoggerFactory.getLogger(KPLConfiguration.class);

    public static boolean getFailIfThrottled() {
        boolean failIfThrottled = false; //Default value if false
        String envSetting = System.getenv("FAIL_IF_THROTTLED");
        if (envSetting != null) {
            failIfThrottled = Boolean.valueOf(envSetting);
        }

        logger.info("FailIfThrottled: {}", failIfThrottled);
        return failIfThrottled;
    }

    public static long longFromEnv(String envName, String configName, long defaultVal) {
        long val = defaultVal;
        String envSetting = System.getenv(envName);
        if(envSetting != null) {
            val = Long.valueOf(envSetting);
        }
        logger.info("{}: {}", configName, val);
        return val;
    }

    public static long getRateLimit() {
        return longFromEnv("RATE_LIMIT", "RateLimit", 150);
    }

    public static long getRecordMaxBufferedTime() {
        return longFromEnv("RECORD_MAX_BUFFERED_TIME", "RecordMaxBufferedTime", 100);
    }

    public static long getRecordTtl() {
        return longFromEnv("RECORD_TTL", "RecordTtl",30000);
    }

    public static long getBackPressureBufferThreshold() {
        return longFromEnv("BACKPRESSURE_BUFFER_THRESHOLD", "BackPressureBufferThreshold", 99999999999999L);
    }
}
