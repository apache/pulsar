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
package org.apache.pulsar.functions.instance.stats;

import com.google.common.collect.EvictingQueue;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.RateLimiter;
import org.apache.pulsar.functions.proto.InstanceCommunication;

/**
 * Function stats.
 */
@Slf4j
@Getter
@Setter
public class FunctionStatsManager extends ComponentStatsManager {

    public static final String PULSAR_FUNCTION_METRICS_PREFIX = "pulsar_function_";

    /** Declare metric names. **/
    public static final String PROCESSED_SUCCESSFULLY_TOTAL = "processed_successfully_total";
    public static final String SYSTEM_EXCEPTIONS_TOTAL = "system_exceptions_total";
    public static final String USER_EXCEPTIONS_TOTAL = "user_exceptions_total";
    public static final String SOURCE_EXCEPTIONS_TOTAL = "source_exceptions_total";
    public static final String SINK_EXCEPTIONS_TOTAL = "sink_exceptions_total";
    public static final String PROCESS_LATENCY_MS = "process_latency_ms";
    public static final String LAST_INVOCATION = "last_invocation";
    public static final String RECEIVED_TOTAL = "received_total";

    public static final String PROCESSED_SUCCESSFULLY_TOTAL_1min = "processed_successfully_1min";
    public static final String SYSTEM_EXCEPTIONS_TOTAL_1min = "system_exceptions_1min";
    public static final String USER_EXCEPTIONS_TOTAL_1min = "user_exceptions_1min";
    public static final String SOURCE_EXCEPTIONS_TOTAL_1min = "source_exceptions_1min";
    public static final String SINK_EXCEPTIONS_TOTAL_1min = "sink_exceptions_1min";
    public static final String PROCESS_LATENCY_MS_1min = "process_latency_ms_1min";
    public static final String RECEIVED_TOTAL_1min = "received_1min";

    /** Declare Prometheus stats. **/

    final Counter statTotalProcessedSuccessfully;

    final Counter statTotalSysExceptions;

    final Counter statTotalUserExceptions;

    final Summary statProcessLatency;

    final Gauge statlastInvocation;

    final Counter statTotalRecordsReceived;

    // windowed metrics

    final Counter statTotalProcessedSuccessfully1min;

    final Counter statTotalSysExceptions1min;

    final Counter statTotalUserExceptions1min;

    final Summary statProcessLatency1min;

    final Counter statTotalRecordsReceived1min;

    // exceptions

    final Gauge userExceptions;

    final Gauge sysExceptions;

    final Gauge sourceExceptions;

    final Gauge sinkExceptions;

    // As an optimization
    private final Counter.Child statTotalProcessedSuccessfullyChild;
    private final Counter.Child statTotalSysExceptionsChild;
    private final Counter.Child statTotalUserExceptionsChild;
    private final Summary.Child statProcessLatencyChild;
    private final Gauge.Child statlastInvocationChild;
    private final Counter.Child statTotalRecordsReceivedChild;
    private Counter.Child statTotalProcessedSuccessfully1minChild;
    private Counter.Child statTotalSysExceptions1minChild;
    private Counter.Child statTotalUserExceptions1minChild;
    private Summary.Child statProcessLatency1minChild;
    private Counter.Child statTotalRecordsReceivedChild1min;

    @Getter
    private EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> latestUserExceptions =
            EvictingQueue.create(10);
    @Getter
    private EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> latestSystemExceptions =
            EvictingQueue.create(10);

    private final RateLimiter userExceptionRateLimiter;

    private final RateLimiter sysExceptionRateLimiter;

    public FunctionStatsManager(FunctionCollectorRegistry collectorRegistry,
                                String[] metricsLabels,
                                ScheduledExecutorService scheduledExecutorService) {
        super(collectorRegistry, metricsLabels, scheduledExecutorService);

        statTotalProcessedSuccessfully = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + PROCESSED_SUCCESSFULLY_TOTAL,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + PROCESSED_SUCCESSFULLY_TOTAL)
                .help("Total number of messages processed successfully.")
                .labelNames(METRICS_LABEL_NAMES)
                .create());
        statTotalProcessedSuccessfullyChild = statTotalProcessedSuccessfully.labels(metricsLabels);

        statTotalSysExceptions = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL)
                .help("Total number of system exceptions.")
                .labelNames(METRICS_LABEL_NAMES)
                .create());
        statTotalSysExceptionsChild = statTotalSysExceptions.labels(metricsLabels);

        statTotalUserExceptions = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + USER_EXCEPTIONS_TOTAL,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + USER_EXCEPTIONS_TOTAL)
                .help("Total number of user exceptions.")
                .labelNames(METRICS_LABEL_NAMES)
                .create());
        statTotalUserExceptionsChild = statTotalUserExceptions.labels(metricsLabels);

        statProcessLatency = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS,
                Summary.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS)
                .help("Process latency in milliseconds.")
                .quantile(0.5, 0.01)
                .quantile(0.9, 0.01)
                .quantile(0.99, 0.01)
                .quantile(0.999, 0.01)
                .labelNames(METRICS_LABEL_NAMES)
                .create());
        statProcessLatencyChild = statProcessLatency.labels(metricsLabels);

        statlastInvocation = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + LAST_INVOCATION,
                Gauge.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + LAST_INVOCATION)
                .help("The timestamp of the last invocation of the function.")
                .labelNames(METRICS_LABEL_NAMES)
                .create());
        statlastInvocationChild = statlastInvocation.labels(metricsLabels);

        statTotalRecordsReceived = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + RECEIVED_TOTAL,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + RECEIVED_TOTAL)
                .help("Total number of messages received from source.")
                .labelNames(METRICS_LABEL_NAMES)
                .create());
        statTotalRecordsReceivedChild = statTotalRecordsReceived.labels(metricsLabels);

        statTotalProcessedSuccessfully1min = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + PROCESSED_SUCCESSFULLY_TOTAL_1min,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + PROCESSED_SUCCESSFULLY_TOTAL_1min)
                .help("Total number of messages processed successfully in the last 1 minute.")
                .labelNames(METRICS_LABEL_NAMES)
                .create());
        statTotalProcessedSuccessfully1minChild = statTotalProcessedSuccessfully1min.labels(metricsLabels);

        statTotalSysExceptions1min = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL_1min,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL_1min)
                .help("Total number of system exceptions in the last 1 minute.")
                .labelNames(METRICS_LABEL_NAMES)
                .create());
        statTotalSysExceptions1minChild = statTotalSysExceptions1min.labels(metricsLabels);

        statTotalUserExceptions1min = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + USER_EXCEPTIONS_TOTAL_1min,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + USER_EXCEPTIONS_TOTAL_1min)
                .help("Total number of user exceptions in the last 1 minute.")
                .labelNames(METRICS_LABEL_NAMES)
                .create());
        statTotalUserExceptions1minChild = statTotalUserExceptions1min.labels(metricsLabels);

        statProcessLatency1min = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS_1min,
                Summary.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS_1min)
                .help("Process latency in milliseconds in the last 1 minute.")
                .quantile(0.5, 0.01)
                .quantile(0.9, 0.01)
                .quantile(0.99, 0.01)
                .quantile(0.999, 0.01)
                .labelNames(METRICS_LABEL_NAMES)
                .create());
        statProcessLatency1minChild = statProcessLatency1min.labels(metricsLabels);

        statTotalRecordsReceived1min = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + RECEIVED_TOTAL_1min,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + RECEIVED_TOTAL_1min)
                .help("Total number of messages received from source in the last 1 minute.")
                .labelNames(METRICS_LABEL_NAMES)
                .create());
        statTotalRecordsReceivedChild1min = statTotalRecordsReceived1min.labels(metricsLabels);

        userExceptions = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + "user_exception",
                Gauge.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + "user_exception")
                .labelNames(EXCEPTION_METRICS_LABEL_NAMES)
                .help("Exception from user code.")
                .create());
        sysExceptions = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + "system_exception",
                Gauge.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + "system_exception")
                .labelNames(EXCEPTION_METRICS_LABEL_NAMES)
                .help("Exception from system code.")
                .create());

        sourceExceptions = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + "source_exception",
                Gauge.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + "source_exception")
                .labelNames(EXCEPTION_METRICS_LABEL_NAMES)
                .help("Exception from source.")
                .create());

        sinkExceptions = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + "sink_exception",
                Gauge.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + "sink_exception")
                .labelNames(EXCEPTION_METRICS_LABEL_NAMES)
                .help("Exception from sink.")
                .create());

        userExceptionRateLimiter = RateLimiter.builder()
                .scheduledExecutorService(scheduledExecutorService)
                .permits(5)
                .rateTime(1)
                .timeUnit(TimeUnit.MINUTES)
                .build();
        sysExceptionRateLimiter = RateLimiter.builder()
                .scheduledExecutorService(scheduledExecutorService)
                .permits(5)
                .rateTime(1)
                .timeUnit(TimeUnit.MINUTES)
                .build();
    }

    public void addUserException(Throwable ex) {
        long ts = System.currentTimeMillis();
        InstanceCommunication.FunctionStatus.ExceptionInformation info = getExceptionInfo(ex, ts);
        latestUserExceptions.add(info);

        // report exception throw prometheus
        if (userExceptionRateLimiter.tryAcquire()) {
            String[] exceptionMetricsLabels = getExceptionMetricsLabels(ex);
            userExceptions.labels(exceptionMetricsLabels).set(1.0);
        }
    }

    public void addSystemException(Throwable ex) {
        long ts = System.currentTimeMillis();
        InstanceCommunication.FunctionStatus.ExceptionInformation info = getExceptionInfo(ex, ts);
        latestSystemExceptions.add(info);

        // report exception throw prometheus
        if (sysExceptionRateLimiter.tryAcquire()) {
            String[] exceptionMetricsLabels = getExceptionMetricsLabels(ex);
            sysExceptions.labels(exceptionMetricsLabels).set(1.0);
        }
    }

    private String[] getExceptionMetricsLabels(Throwable ex) {
        String[] exceptionMetricsLabels = Arrays.copyOf(metricsLabels, metricsLabels.length + 1);
        exceptionMetricsLabels[exceptionMetricsLabels.length - 1] = ex.getMessage() != null ? ex.getMessage() : "";
        return exceptionMetricsLabels;
    }

    @Override
    public void incrTotalReceived() {
        statTotalRecordsReceivedChild.inc();
        statTotalRecordsReceivedChild1min.inc();
    }

    @Override
    public void incrTotalProcessedSuccessfully() {
        statTotalProcessedSuccessfullyChild.inc();
        statTotalProcessedSuccessfully1minChild.inc();
    }

    @Override
    public void incrSysExceptions(Throwable sysException) {
        statTotalSysExceptionsChild.inc();
        statTotalSysExceptions1minChild.inc();
        addSystemException(sysException);
    }

    @Override
    public void incrUserExceptions(Throwable userException) {
        statTotalUserExceptionsChild.inc();
        statTotalUserExceptions1minChild.inc();
        addUserException(userException);
    }

    @Override
    public void incrSourceExceptions(Throwable ex) {
        incrSysExceptions(ex);
    }

    @Override
    public void incrSinkExceptions(Throwable ex) {
        incrSysExceptions(ex);
    }

    @Override
    public void setLastInvocation(long ts) {
        statlastInvocationChild.set(ts);
    }

    private Long processTimeStart;

    @Override
    public void processTimeStart() {
        processTimeStart = System.nanoTime();
    }

    @Override
    public void processTimeEnd() {
        if (processTimeStart != null) {
            double endTimeMs = ((double) System.nanoTime() - processTimeStart) / 1.0E6D;
            statProcessLatencyChild.observe(endTimeMs);
            statProcessLatency1minChild.observe(endTimeMs);
        }
    }

    @Override
    public double getTotalProcessedSuccessfully() {
        return statTotalProcessedSuccessfullyChild.get();
    }

    @Override
    public double getTotalRecordsReceived() {
        return statTotalRecordsReceivedChild.get();
    }

    @Override
    public double getTotalSysExceptions() {
        return statTotalSysExceptionsChild.get();
    }

    @Override
    public double getTotalUserExceptions() {
        return statTotalUserExceptionsChild.get();
    }

    @Override
    public double getLastInvocation() {
        return statlastInvocationChild.get();
    }

    public double getAvgProcessLatency() {
        return statProcessLatencyChild.get().count <= 0.0
                ? 0 : statProcessLatencyChild.get().sum / statProcessLatencyChild.get().count;
    }

    public double getProcessLatency50P() {
        return statProcessLatencyChild.get().quantiles.get(0.5);
    }

    public double getProcessLatency90P() {
        return statProcessLatencyChild.get().quantiles.get(0.9);
    }

    public double getProcessLatency99P() {
        return statProcessLatencyChild.get().quantiles.get(0.99);
    }

    public double getProcessLatency99_9P() {
        return statProcessLatencyChild.get().quantiles.get(0.999);
    }

    @Override
    public double getTotalProcessedSuccessfully1min() {
        return statTotalProcessedSuccessfully1minChild.get();
    }

    @Override
    public double getTotalRecordsReceived1min() {
        return statTotalRecordsReceivedChild1min.get();
    }

    @Override
    public double getTotalSysExceptions1min() {
        return statTotalSysExceptions1minChild.get();
    }

    @Override
    public double getTotalUserExceptions1min() {
        return statTotalUserExceptions1minChild.get();
    }

    @Override
    public double getAvgProcessLatency1min() {
        return statProcessLatency1minChild.get().count <= 0.0
                ? 0 : statProcessLatency1minChild.get().sum / statProcessLatency1minChild.get().count;
    }

    @Override
    public EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> getLatestSourceExceptions() {
        return emptyQueue;
    }

    @Override
    public EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> getLatestSinkExceptions() {
        return emptyQueue;
    }

    public double getProcessLatency50P1min() {
        return statProcessLatency1minChild.get().quantiles.get(0.5);
    }

    public double getProcessLatency90P1min() {
        return statProcessLatency1minChild.get().quantiles.get(0.9);
    }

    public double getProcessLatency99P1min() {
        return statProcessLatency1minChild.get().quantiles.get(0.99);
    }

    public double getProcessLatency99_9P1min() {
        return statProcessLatency1minChild.get().quantiles.get(0.999);
    }

    @Override
    public void reset() {
        statTotalProcessedSuccessfully1min.clear();
        statTotalProcessedSuccessfully1minChild = statTotalProcessedSuccessfully1min.labels(metricsLabels);

        statTotalSysExceptions1min.clear();
        statTotalSysExceptions1minChild = statTotalSysExceptions1min.labels(metricsLabels);

        statTotalUserExceptions1min.clear();
        statTotalUserExceptions1minChild = statTotalUserExceptions1min.labels(metricsLabels);

        statProcessLatency1min.clear();
        statProcessLatency1minChild = statProcessLatency1min.labels(metricsLabels);

        statTotalRecordsReceived1min.clear();
        statTotalRecordsReceivedChild1min = statTotalRecordsReceived1min.labels(metricsLabels);
    }
}
