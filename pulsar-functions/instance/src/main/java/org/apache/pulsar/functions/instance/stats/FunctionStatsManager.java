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
import com.google.common.util.concurrent.RateLimiter;
import io.prometheus.metrics.core.datapoints.CounterDataPoint;
import io.prometheus.metrics.core.datapoints.GaugeDataPoint;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Summary;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.pulsar.functions.proto.FunctionStatus;

/**
 * Function stats.
 */
@CustomLog
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

    /** The simpleclient default; the 1.x default is 5 minutes. */
    private static final long QUANTILE_WINDOW_SECONDS = TimeUnit.MINUTES.toSeconds(10);

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
    private final CounterDataPoint statTotalProcessedSuccessfullyChild;
    private final CounterDataPoint statTotalSysExceptionsChild;
    private final CounterDataPoint statTotalUserExceptionsChild;
    private final Summary.DataPoint statProcessLatencyChild;
    private final GaugeDataPoint statlastInvocationChild;
    private final CounterDataPoint statTotalRecordsReceivedChild;
    private CounterDataPoint statTotalProcessedSuccessfully1minChild;
    private CounterDataPoint statTotalSysExceptions1minChild;
    private CounterDataPoint statTotalUserExceptions1minChild;
    private Summary.DataPoint statProcessLatency1minChild;
    private CounterDataPoint statTotalRecordsReceivedChild1min;

    @Getter
    private EvictingQueue<FunctionStatus.ExceptionInformation> latestUserExceptions =
            EvictingQueue.create(10);
    @Getter
    private EvictingQueue<FunctionStatus.ExceptionInformation> latestSystemExceptions =
            EvictingQueue.create(10);

    private final RateLimiter userExceptionRateLimiter;

    private final RateLimiter sysExceptionRateLimiter;

    public FunctionStatsManager(FunctionCollectorRegistry collectorRegistry,
                                String[] metricsLabels,
                                ScheduledExecutorService scheduledExecutorService) {
        super(collectorRegistry, metricsLabels, scheduledExecutorService);

        statTotalProcessedSuccessfully = counter(PULSAR_FUNCTION_METRICS_PREFIX + PROCESSED_SUCCESSFULLY_TOTAL,
                "Total number of messages processed successfully.");
        statTotalProcessedSuccessfullyChild = statTotalProcessedSuccessfully.labelValues(metricsLabels);

        statTotalSysExceptions = counter(PULSAR_FUNCTION_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL,
                "Total number of system exceptions.");
        statTotalSysExceptionsChild = statTotalSysExceptions.labelValues(metricsLabels);

        statTotalUserExceptions = counter(PULSAR_FUNCTION_METRICS_PREFIX + USER_EXCEPTIONS_TOTAL,
                "Total number of user exceptions.");
        statTotalUserExceptionsChild = statTotalUserExceptions.labelValues(metricsLabels);

        statProcessLatency = latencySummary(PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS,
                "Process latency in milliseconds.");
        statProcessLatencyChild = (Summary.DataPoint) statProcessLatency.labelValues(metricsLabels);

        statlastInvocation = gauge(PULSAR_FUNCTION_METRICS_PREFIX + LAST_INVOCATION,
                "The timestamp of the last invocation of the function.");
        statlastInvocationChild = statlastInvocation.labelValues(metricsLabels);

        statTotalRecordsReceived = counter(PULSAR_FUNCTION_METRICS_PREFIX + RECEIVED_TOTAL,
                "Total number of messages received from source.");
        statTotalRecordsReceivedChild = statTotalRecordsReceived.labelValues(metricsLabels);

        statTotalProcessedSuccessfully1min = counter(
                PULSAR_FUNCTION_METRICS_PREFIX + PROCESSED_SUCCESSFULLY_TOTAL_1min,
                "Total number of messages processed successfully in the last 1 minute.");
        statTotalProcessedSuccessfully1minChild = statTotalProcessedSuccessfully1min.labelValues(metricsLabels);

        statTotalSysExceptions1min = counter(PULSAR_FUNCTION_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL_1min,
                "Total number of system exceptions in the last 1 minute.");
        statTotalSysExceptions1minChild = statTotalSysExceptions1min.labelValues(metricsLabels);

        statTotalUserExceptions1min = counter(PULSAR_FUNCTION_METRICS_PREFIX + USER_EXCEPTIONS_TOTAL_1min,
                "Total number of user exceptions in the last 1 minute.");
        statTotalUserExceptions1minChild = statTotalUserExceptions1min.labelValues(metricsLabels);

        statProcessLatency1min = latencySummary(PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS_1min,
                "Process latency in milliseconds in the last 1 minute.");
        statProcessLatency1minChild = (Summary.DataPoint) statProcessLatency1min.labelValues(metricsLabels);

        statTotalRecordsReceived1min = counter(PULSAR_FUNCTION_METRICS_PREFIX + RECEIVED_TOTAL_1min,
                "Total number of messages received from source in the last 1 minute.");
        statTotalRecordsReceivedChild1min = statTotalRecordsReceived1min.labelValues(metricsLabels);

        userExceptions = exceptionGauge(PULSAR_FUNCTION_METRICS_PREFIX + "user_exception",
                "Exception from user code.");
        sysExceptions = exceptionGauge(PULSAR_FUNCTION_METRICS_PREFIX + "system_exception",
                "Exception from system code.");
        sourceExceptions = exceptionGauge(PULSAR_FUNCTION_METRICS_PREFIX + "source_exception",
                "Exception from source.");
        sinkExceptions = exceptionGauge(PULSAR_FUNCTION_METRICS_PREFIX + "sink_exception",
                "Exception from sink.");

        userExceptionRateLimiter = RateLimiter.create(5.0d / 60.0d);
        sysExceptionRateLimiter = RateLimiter.create(5.0d / 60.0d);
    }

    private Summary latencySummary(String name, String help) {
        return collectorRegistry.registerIfNotExist(name, Summary.builder()
                .name(name)
                .help(help)
                .quantile(0.5, 0.01)
                .quantile(0.9, 0.01)
                .quantile(0.99, 0.01)
                .quantile(0.999, 0.01)
                .maxAgeSeconds(QUANTILE_WINDOW_SECONDS)
                .labelNames(METRICS_LABEL_NAMES)
                .build());
    }

    public void addUserException(Throwable ex) {
        long ts = System.currentTimeMillis();
        FunctionStatus.ExceptionInformation info = getExceptionInfo(ex, ts);
        latestUserExceptions.add(info);

        // report exception throw prometheus
        if (userExceptionRateLimiter.tryAcquire()) {
            String[] exceptionMetricsLabels = getExceptionMetricsLabels(ex);
            userExceptions.labelValues(exceptionMetricsLabels).set(1.0);
        }
    }

    public void addSystemException(Throwable ex) {
        long ts = System.currentTimeMillis();
        FunctionStatus.ExceptionInformation info = getExceptionInfo(ex, ts);
        latestSystemExceptions.add(info);

        // report exception throw prometheus
        if (sysExceptionRateLimiter.tryAcquire()) {
            String[] exceptionMetricsLabels = getExceptionMetricsLabels(ex);
            sysExceptions.labelValues(exceptionMetricsLabels).set(1.0);
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



    @Override
    public void processTimeEnd(long startTime) {
            double endTimeMs = ((double) System.nanoTime() - startTime) / 1.0E6D;
            statProcessLatencyChild.observe(endTimeMs);
            statProcessLatency1minChild.observe(endTimeMs);
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
        return averageLatency(statProcessLatencyChild);
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
        return averageLatency(statProcessLatency1minChild);
    }

    private static double averageLatency(Summary.DataPoint latency) {
        long count = latency.getCount();
        return count <= 0 ? 0 : latency.getSum() / count;
    }

    @Override
    public EvictingQueue<FunctionStatus.ExceptionInformation> getLatestSourceExceptions() {
        return emptyQueue;
    }

    @Override
    public EvictingQueue<FunctionStatus.ExceptionInformation> getLatestSinkExceptions() {
        return emptyQueue;
    }

    @Override
    public void reset() {
        statTotalProcessedSuccessfully1min.clear();
        statTotalProcessedSuccessfully1minChild = statTotalProcessedSuccessfully1min.labelValues(metricsLabels);

        statTotalSysExceptions1min.clear();
        statTotalSysExceptions1minChild = statTotalSysExceptions1min.labelValues(metricsLabels);

        statTotalUserExceptions1min.clear();
        statTotalUserExceptions1minChild = statTotalUserExceptions1min.labelValues(metricsLabels);

        statProcessLatency1min.clear();
        statProcessLatency1minChild = (Summary.DataPoint) statProcessLatency1min.labelValues(metricsLabels);

        statTotalRecordsReceived1min.clear();
        statTotalRecordsReceivedChild1min = statTotalRecordsReceived1min.labelValues(metricsLabels);
    }
}
