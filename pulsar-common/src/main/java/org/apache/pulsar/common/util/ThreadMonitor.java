package org.apache.pulsar.common.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ThreadMonitor {
    public static final ConcurrentMap<String, Long> THREAD_LAST_ACTIVE_TIMESTAMP =
            new ConcurrentHashMap<>(Runtime.getRuntime().availableProcessors(),
                    0.75f, 1024);

    public static final String THREAD_ACTIVE_TIMESTAMP_GAUGE_NAME = "pulsar_thread_last_active_timestamp_ms";

    public static class Ping implements Runnable {
        @Override
        public void run() {
            THREAD_LAST_ACTIVE_TIMESTAMP.put(Thread.currentThread().getName(), System.currentTimeMillis());
        }
    }

    public static Runnable ping() {
        return new Ping();
    }
}
