package org.apache.pulsar.common.util;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


public class ThreadPoolMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadPoolMonitor.class);

    private static final AtomicBoolean ENABLED = new AtomicBoolean(false);
    private static final AtomicLong CHECK_INTERVAL_MS = new AtomicLong(10 * 1000); // 10 seconds

    public static final String PULSAR_ENABLE_THREAD_MONITOR = "pulsar.thread-pool.monitor.enabled";
    public static final String PULSAR_THREAD_CHECK_INTERVAL_MS = "pulsar.thread-pool.monitor.check.ms";

    private static final ScheduledExecutorService monitorExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("pulsar-thread-pool-monitor", true));

    private static final Set<ExecutorService> registered = ConcurrentHashMap.newKeySet();
    private static final Map<ExecutorService, ScheduledFuture<?>> submitted = new ConcurrentHashMap<>();

    static {
        checkThreadMonitorEnabled();
        monitorExecutor.scheduleWithFixedDelay(ThreadPoolMonitor::checkThreadMonitorEnabled,
                1,
                10,
                TimeUnit.SECONDS);
    }

    private static void checkThreadMonitorEnabled() {
        boolean enableSysProp;
        long checkIntervalMs = 10000;

        try {
            enableSysProp = Boolean.parseBoolean(System.getProperty(PULSAR_ENABLE_THREAD_MONITOR, "false"));
            checkIntervalMs = Integer.parseInt(System.getProperty(PULSAR_THREAD_CHECK_INTERVAL_MS, "1000"));
        } catch (Exception e) {
            enableSysProp = false;
        }

        boolean lastTimeEnabled = ENABLED.getAndSet(enableSysProp);
        long lastTimeCheckIntervalMs = CHECK_INTERVAL_MS.get();
        if (checkIntervalMs > 0) {
            CHECK_INTERVAL_MS.set(checkIntervalMs);
        }

        boolean currentSetEnabled = ENABLED.get();

        boolean checkIntervalChanged = lastTimeCheckIntervalMs != checkIntervalMs;
        boolean monitorStateChanged = lastTimeEnabled ^ currentSetEnabled;
        boolean hasNewRegisteredThreadPool = registered.size() != submitted.size();

        // state changed or thread-pool registered or checkIntervalChanged
        // cancel first
        if (monitorStateChanged || checkIntervalChanged || (currentSetEnabled && hasNewRegisteredThreadPool)) {
            cancelAllMonitorTasks();
            LOGGER.info("pulsar thread monitor all task canceled " +
                            "monitorStateChanged={} checkIntervalChanged={} hasNewRegisterThreadPool={}.",
                    monitorStateChanged,
                    checkIntervalChanged,
                    hasNewRegisteredThreadPool);

            // reschedule
            if (currentSetEnabled) {
                submitMonitorTasks();
                LOGGER.info("pulsar thread monitor start monitor task, checkIntervalMs={} " +
                                "current register threadPool number={} submitted number={}.",
                        CHECK_INTERVAL_MS.get(),
                        registered.size(),
                        submitted.size());
            }
        }
    }

    public static void register(ExecutorService... executorService) {
        registered.addAll(Arrays.asList(executorService));
    }

    public static synchronized void submitMonitorTasks() {
        if (!ENABLED.get()) {
            return;
        }

        for (ExecutorService service : registered) {
            if (service != null && !submitted.containsKey(service)) {
                boolean canSubmitTask = ENABLED.get() && !service.isShutdown() && !service.isTerminated();

                if (!monitorExecutor.isShutdown() && !monitorExecutor.isTerminated()) {
                    ScheduledFuture<?> future = monitorExecutor.scheduleWithFixedDelay(() -> {
                        try {
                            if (canSubmitTask) {
                                service.submit(ThreadMonitor.ping());
                            }
                        } catch (Throwable t) {
                            LOGGER.error("Unexpected throwable caught ", t);
                        }

                    }, CHECK_INTERVAL_MS.get(), CHECK_INTERVAL_MS.get(), TimeUnit.MILLISECONDS);
                    submitted.put(service, future);
                } else {
                    break;
                }
            }
        }
    }

    private static synchronized void cancelAllMonitorTasks() {
        submitted.values().forEach((future) -> future.cancel(false));
        submitted.clear();

        ThreadMonitor.THREAD_LAST_ACTIVE_TIMESTAMP.clear();
    }

    public static final String THREAD_POOL_MONITOR_ENABLED_GAUGE_NAME = "pulsar_thread_monitor_enabled";
    public static final String THREAD_POOL_MONITOR_CHECK_INTERVAL_MS_GAUGE_NAME = "pulsar_thread_monitor_check_interval_ms";
    public static final String THREAD_POOL_MONITOR_REGISTERED_POOL_NUMBER_GAUGE_NAME = "pulsar_thread_monitor_register_pool_number";
    public static final String THREAD_POOL_MONITOR_SUBMITTED_POOL_NUMBER_GAUGE_NAME = "pulsar_thread_monitor_submitted_monitor_task_number";

    public static boolean isEnabled() {
        return ENABLED.get();
    }

    public static long checkIntervalMs() {
        return CHECK_INTERVAL_MS.get();
    }

    public static int registeredThreadPool() {
        return registered.size();
    }

    public static int submittedThreadPool() {
        return submitted.size();
    }

    public static void stop() {
        monitorExecutor.shutdownNow();
    }
}
