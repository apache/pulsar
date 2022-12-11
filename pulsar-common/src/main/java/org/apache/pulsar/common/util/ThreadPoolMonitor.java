package org.apache.pulsar.common.util;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class ThreadPoolMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadPoolMonitor.class);

    private static final ScheduledExecutorService monitorExecutor =
            Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("thread-pool-monitor", true));

    private static final Set<ExecutorService> registered = ConcurrentHashMap.newKeySet();

    public static void register(long monitorInterval, TimeUnit unit, ExecutorService... executorService) {
        for (ExecutorService service : executorService) {
            if (service != null && !registered.contains(service)) {
                monitorExecutor.scheduleWithFixedDelay(() -> {
                    try {
                        if (!service.isShutdown() && !service.isTerminated()) {
                            service.submit(ThreadMonitor.ping());
                        }

                    } catch (Throwable t) {
                        LOGGER.error("Unexpected throwable caught ", t);
                    }

                }, monitorInterval, monitorInterval, unit);

                registered.add(service);
            }
        }
    }

    public static void stop() {
        monitorExecutor.shutdownNow();
    }
}
