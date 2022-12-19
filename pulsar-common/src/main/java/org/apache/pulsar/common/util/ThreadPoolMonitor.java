/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.util;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ThreadPoolMonitor
 * A utility to schedule `ping` task periodically to registered thread's task queue.
 * If the `ping` task is executed thread active timestamp will be refreshed,
 * if not, the active timestamp will remain the same for a long time, which perhaps
 * thread is blocking.
 * <p>
 * Use System property to enable or disable this function.
 * Support dynamic configuration to enable/disable or change check interval.
 * <p>
 * Prefer registerSingleThreadExecutor single thread pool like
 * `Executors.newSingleThreadExecutor`.
 * <p>
 * If a thread pool has more threads, the active timestamp for **each** thread in pool
 * may not be refreshed in time.
 */
public class ThreadPoolMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadPoolMonitor.class);

    private static final AtomicBoolean ENABLED = new AtomicBoolean(false);
    private static final AtomicLong CHECK_INTERVAL_MS = new AtomicLong(10 * 1000); // 10 seconds

    private static final AtomicReference<ThreadPoolMonitorConfig> monitorConfig = new AtomicReference<>();

    public static final String PULSAR_ENABLE_THREAD_MONITOR =
            "pulsar.thread-pool.monitor.enabled";
    public static final String PULSAR_THREAD_CHECK_INTERVAL_MS =
            "pulsar.thread-pool.monitor.check.ms";

    public static final String PULSAR_BLOCK_THREAD_DUMP =
            "pulsar.thread-pool.monitor.blocking.dump.enabled";

    public static final String PULSAR_BLOCK_THREAD_CHECK_INTERVAL_MS =
            "pulsar.thread-pool.monitor.blocking.dump.check.ms";

    public static final String PULSAR_BLOCK_THREAD_THRESHOLD_MS =
            "pulsar-thread-pool.monitor.blocking.dump.threshold.ms";

    public static final String PULSAR_BLOCK_THREAD_STACK_MAX_DEPTH =
            "pulsar-thread-pool.monitor.blocking.dump.stack-max-depth";

    private static final ScheduledExecutorService monitorExecutor = Executors.newSingleThreadScheduledExecutor(
            new ExecutorProvider.ExtendedThreadFactory("pulsar-thread-pool-monitor", true));

    private static final Set<ExecutorService> registered = ConcurrentHashMap.newKeySet(100);
    private static final Map<ExecutorService, Integer> multiThreadPool = new ConcurrentHashMap<>();
    private static final Map<ExecutorService, ScheduledFuture<?>> submitted = new ConcurrentHashMap<>();

    public static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    static {
        init();
    }

    private static volatile ScheduledFuture<?> blockThreadDumpTask = null;

    private static void init() {
        monitorExecutor.scheduleWithFixedDelay(ThreadPoolMonitor::checkThreadMonitorEnabled,
                0, 10, TimeUnit.SECONDS);

        monitorExecutor.scheduleWithFixedDelay(ThreadPoolMonitor::cleanExitThread,
                60, 60, TimeUnit.SECONDS);
    }

    @Value
    @AllArgsConstructor
    private static class ThreadPoolMonitorConfig {
        boolean monitorThreadEnabled;
        long monitorThreadIntervalMs;

        boolean dumpBlockingThreadEnabled;
        long blockingThreadCheckIntervalMs;
        long blockingThreadThresholdMs;
        int blockingThreadMaxDepth;
    }

    private static ThreadPoolMonitorConfig readSysProp() {
        boolean monitorThreadEnabled;
        long monitorThreadIntervalMs = 10000;

        boolean dumpBlockingThreadEnabled;
        long blockingThreadCheckIntervalMs = 30000;
        long blockingThreadThresholdMs = 30000;
        int blockingThreadMaxDepth = 32;


        try {
            monitorThreadEnabled = Boolean.parseBoolean(
                    System.getProperty(PULSAR_ENABLE_THREAD_MONITOR, "false"));
            monitorThreadIntervalMs = Integer.parseInt(
                    System.getProperty(PULSAR_THREAD_CHECK_INTERVAL_MS, "10000"));

            dumpBlockingThreadEnabled = Boolean.parseBoolean(
                    System.getProperty(PULSAR_BLOCK_THREAD_DUMP, "false"));
            blockingThreadCheckIntervalMs = Integer.parseInt(
                    System.getProperty(PULSAR_BLOCK_THREAD_CHECK_INTERVAL_MS, "30000"));
            blockingThreadThresholdMs = Integer.parseInt(
                    System.getProperty(PULSAR_BLOCK_THREAD_THRESHOLD_MS, "30000"));
            blockingThreadMaxDepth = Integer.parseInt(
                    System.getProperty(PULSAR_BLOCK_THREAD_STACK_MAX_DEPTH, "32"));
        } catch (Exception e) {
            LOGGER.error("error when read system property. cancel all thread monitor task.", e);
            monitorThreadEnabled = false;
            dumpBlockingThreadEnabled = false;
        }

        if (monitorThreadIntervalMs <= 0) {
            monitorThreadIntervalMs = 10000;
        }

        if (blockingThreadCheckIntervalMs <= 0) {
            blockingThreadCheckIntervalMs = 30000;
        }

        if (blockingThreadThresholdMs <= 0) {
            blockingThreadThresholdMs = 30000;
        }

        if (blockingThreadMaxDepth < 0) {
            blockingThreadMaxDepth = 32;
        }

        return new ThreadPoolMonitorConfig(monitorThreadEnabled,
                monitorThreadIntervalMs,
                dumpBlockingThreadEnabled,
                blockingThreadCheckIntervalMs,
                blockingThreadThresholdMs,
                blockingThreadMaxDepth);
    }

    private static void checkThreadMonitorEnabled() {
        ThreadPoolMonitorConfig config = readSysProp();

        boolean enableSysProp = config.isMonitorThreadEnabled();
        long monitorThreadIntervalMs = config.getMonitorThreadIntervalMs();

        boolean lastTimeEnabled = ENABLED.getAndSet(enableSysProp);
        long lastTimeCheckIntervalMs = CHECK_INTERVAL_MS.get();
        if (monitorThreadIntervalMs > 0) {
            CHECK_INTERVAL_MS.set(monitorThreadIntervalMs);
        }

        boolean currentSetEnabled = ENABLED.get();

        boolean checkIntervalChanged = lastTimeCheckIntervalMs != monitorThreadIntervalMs;
        boolean monitorStateChanged = lastTimeEnabled ^ currentSetEnabled;
        boolean hasNewRegisteredThreadPool = registered.size() != submitted.size();

        // state changed or thread-pool registered or checkIntervalChanged
        // cancel first
        if (monitorStateChanged || checkIntervalChanged || (currentSetEnabled && hasNewRegisteredThreadPool)) {
            cancelAllMonitorTasks();
            LOGGER.info("pulsar thread monitor all task canceled "
                            + "monitorStateChanged={} checkIntervalChanged={} hasNewRegisterThreadPool={}.",
                    monitorStateChanged,
                    checkIntervalChanged,
                    hasNewRegisteredThreadPool);

            // reschedule
            if (currentSetEnabled) {
                submitMonitorTasks();
                LOGGER.info("pulsar thread monitor start monitor task, checkIntervalMs={} "
                                + "current registeredThreadPool={} submittedThreadPool={}.",
                        CHECK_INTERVAL_MS.get(),
                        registered.size(),
                        submitted.size());
            }

            switchThreadContentionEnabled(currentSetEnabled);
        }

        checkThreadBlockingDump(config);

        // set current monitor config.
        monitorConfig.set(config);
    }

    private static void checkThreadBlockingDump(ThreadPoolMonitorConfig currentConfig) {
        ThreadPoolMonitorConfig lastTimeConfig = monitorConfig.get();

        if (!currentConfig.isDumpBlockingThreadEnabled()) {
            if (blockThreadDumpTask != null) {
                blockThreadDumpTask.cancel(false);
                blockThreadDumpTask = null;
            }

            return;
        }

        if (lastTimeConfig.getBlockingThreadCheckIntervalMs()
                != currentConfig.getBlockingThreadCheckIntervalMs()) {

            long checkInterval = currentConfig.getBlockingThreadCheckIntervalMs();

            if (blockThreadDumpTask != null) {
                blockThreadDumpTask.cancel(false);
                blockThreadDumpTask = null;
            }

            blockThreadDumpTask = monitorExecutor.scheduleWithFixedDelay(
                    ThreadPoolMonitor::dumpBlockingThread,
                    checkInterval, checkInterval, TimeUnit.MILLISECONDS);
        }

    }

    private static void switchThreadContentionEnabled(boolean enabled) {
        if (THREAD_MX_BEAN.isThreadContentionMonitoringSupported()) {
            THREAD_MX_BEAN.setThreadContentionMonitoringEnabled(enabled);
        }
    }

    private static void cleanExitThread() {
        // skip check if all thread empty
        if (ThreadMonitor.THREAD_ID_TO_STATE.isEmpty()) {
            return;
        }

        Set<Long> monitorThreads = new HashSet<>(ThreadMonitor.THREAD_ID_TO_STATE.keySet());

        for (long threadId : THREAD_MX_BEAN.getAllThreadIds()) {
            monitorThreads.remove(threadId);
        }

        for (long exitThreadId : monitorThreads) {
            ThreadMonitor.THREAD_ID_TO_STATE.remove(exitThreadId);
        }
    }

    private static void dumpBlockingThread() {
        ThreadPoolMonitorConfig config = monitorConfig.get();
        long blockThresholdMs = config.getBlockingThreadThresholdMs();

        Map<Long, ThreadMonitor.ThreadMonitorState> threadMonitorState =
                new HashMap<>(ThreadMonitor.THREAD_ID_TO_STATE);
        Map<Long, Long> maybeBlockThread = new HashMap<>();

        threadMonitorState.forEach((threadId, monitorState) -> {
            boolean runningTask = monitorState.isRunningTask();
            long timeMs = (System.currentTimeMillis() - monitorState.getLastActiveTimestamp());
            if (timeMs > blockThresholdMs && runningTask) {
                maybeBlockThread.put(threadId, timeMs);
            }
        });

        long[] threadIds = new long[maybeBlockThread.size()];
        int i = 0;
        for (Long id : maybeBlockThread.keySet()) {
            threadIds[i] = id;
            i++;
        }

        boolean lockedSynchronizers = false;
        boolean lockedMonitors = false;

        if (THREAD_MX_BEAN.isSynchronizerUsageSupported()) {
            lockedSynchronizers = true;
        }

        if (THREAD_MX_BEAN.isObjectMonitorUsageSupported()) {
            lockedMonitors = true;
        }

        try {
            ThreadInfo[] threadInfos = THREAD_MX_BEAN.getThreadInfo(threadIds,
                    lockedMonitors,
                    lockedSynchronizers);

            for (ThreadInfo info : threadInfos) {
                if (info != null) {
                    LOGGER.info("Possible blocking thread: \n"
                                    + "  duration={}ms, blockTime={}, blockCount={}, waitTime={}, waitCount={} \n"
                                    + "  threadInfo: \n {}",
                            maybeBlockThread.get(info.getThreadId()),
                            info.getBlockedTime(), info.getBlockedCount(), info.getWaitedTime(), info.getWaitedCount(),
                            info);
                }
            }

        } catch (Exception e) {
            LOGGER.error("error when get thread info threadIds={}, lockedMonitors={}, lockedSynchronizers={}",
                    Arrays.toString(threadIds), lockedMonitors, lockedSynchronizers, e);
        }
    }


    public static void registerSingleThreadExecutor(ExecutorService executorService) {
        if (executorService != null) {
            registered.add(executorService);
        }
    }

    public static void registerMultiThreadExecutor(ExecutorService executorService, int threadNumber) {
        if (executorService != null) {
            multiThreadPool.put(executorService, threadNumber);
            registered.add(executorService);
        }
    }

    private static boolean canSubmitTask(ExecutorService service) {
        return ENABLED.get()
                && !service.isShutdown() && !service.isTerminated()
                && !monitorExecutor.isShutdown() && !monitorExecutor.isTerminated();
    }

    private static void scheduleMonitorTask(ExecutorService service, IntSupplier taskNumber) {
        if (!submitted.containsKey(service) && canSubmitTask(service)) {

            ScheduledFuture<?> future = monitorExecutor.scheduleWithFixedDelay(() -> {

                int number = taskNumber.getAsInt();
                for (int i = 0; i < number; i++) {
                    service.execute(ThreadMonitor.ping());
                }

            }, CHECK_INTERVAL_MS.get(), CHECK_INTERVAL_MS.get(), TimeUnit.MILLISECONDS);

            submitted.put(service, future);
        }
    }

    private static synchronized void submitMonitorTasks() {
        if (!ENABLED.get()) {
            return;
        }

        try {
            for (ExecutorService service : registered) {
                int number = multiThreadPool.getOrDefault(service, 1);

                IntSupplier taskNumber;
                if (service == ForkJoinPool.commonPool()) {
                    taskNumber = () -> ForkJoinPool.commonPool().getParallelism();
                } else {
                    taskNumber = () -> number;
                }
                scheduleMonitorTask(service, taskNumber);
            }

        } catch (Exception e) {
            LOGGER.error("Unexpected throwable caught ", e);
        }
    }

    private static synchronized void cancelAllMonitorTasks() {
        submitted.values().forEach((future) -> future.cancel(false));
        submitted.clear();

        ThreadMonitor.THREAD_ID_TO_STATE.clear();
    }

    public static final String THREAD_POOL_MONITOR_ENABLED_GAUGE_NAME =
            "pulsar_thread_monitor_enabled";
    public static final String THREAD_POOL_MONITOR_CHECK_INTERVAL_MS_GAUGE_NAME =
            "pulsar_thread_monitor_check_interval_ms";
    public static final String THREAD_POOL_MONITOR_REGISTERED_POOL_NUMBER_GAUGE_NAME =
            "pulsar_thread_monitor_register_pool_number";
    public static final String THREAD_POOL_MONITOR_SUBMITTED_POOL_NUMBER_GAUGE_NAME =
            "pulsar_thread_monitor_submitted_monitor_task_number";

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
        monitorExecutor.shutdown();
    }
}
