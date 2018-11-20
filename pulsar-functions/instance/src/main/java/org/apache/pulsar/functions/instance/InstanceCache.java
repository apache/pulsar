package org.apache.pulsar.functions.instance;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class InstanceCache {

    private static InstanceCache instance;

    public final ScheduledExecutorService executor;

    private InstanceCache() {
        executor = Executors.newSingleThreadScheduledExecutor();;
    }

    public static InstanceCache getInstanceCache() {
        synchronized (InstanceCache.class) {
            if (instance == null) {
                instance = new InstanceCache();
            }
        }
        return instance;
    }

    public static void shutdown() {
        synchronized (InstanceCache.class) {
            if (instance != null) {
                instance.executor.shutdown();
            }
        }

    }
}
