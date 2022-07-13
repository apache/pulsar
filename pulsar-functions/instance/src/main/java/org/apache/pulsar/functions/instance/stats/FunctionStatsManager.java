/**
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
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.RateLimiter;
import org.apache.pulsar.functions.proto.InstanceCommunication;

import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Function stats.
 */
@Slf4j
@Getter
@Setter
public class FunctionStatsManager extends ComponentStatsManager{

    public static final String PULSAR_FUNCTION_METRICS_PREFIX = "pulsar_function_";

    /** Declare metric names **/
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

    /** Declare Prometheus stats **/

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
    private final Counter.Child _statTotalProcessedSuccessfully;
    private final Counter.Child _statTotalSysExceptions;
    private final Counter.Child _statTotalUserExceptions;
    private final Summary.Child _statProcessLatency;
    private final Gauge.Child _statlastInvocation;
    private final Counter.Child _statTotalRecordsReceived;
    private Counter.Child _statTotalProcessedSuccessfully1min;
    private Counter.Child _statTotalSysExceptions1min;
    private Counter.Child _statTotalUserExceptions1min;
    private Summary.Child _statProcessLatency1min;
    private Counter.Child _statTotalRecordsReceived1min;

    @Getter
    private EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> latestUserExceptions = EvictingQueue.create(10);
    @Getter
    private EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> latestSystemExceptions = EvictingQueue.create(10);

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
                .labelNames(metricsLabelNames)
                .create());
        _statTotalProcessedSuccessfully = statTotalProcessedSuccessfully.labels(metricsLabels);

        statTotalSysExceptions = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL)
                .help("Total number of system exceptions.")
                .labelNames(metricsLabelNames)
                .create());
        _statTotalSysExceptions = statTotalSysExceptions.labels(metricsLabels);

        statTotalUserExceptions = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + USER_EXCEPTIONS_TOTAL,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + USER_EXCEPTIONS_TOTAL)
                .help("Total number of user exceptions.")
                .labelNames(metricsLabelNames)
                .create());
        _statTotalUserExceptions = statTotalUserExceptions.labels(metricsLabels);

        statProcessLatency = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS,
                Summary.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS)
                .help("Process latency in milliseconds.")
                .quantile(0.5, 0.01)
                .quantile(0.9, 0.01)
                .quantile(0.99, 0.01)
                .quantile(0.999, 0.01)
                .labelNames(metricsLabelNames)
                .create());
        _statProcessLatency = statProcessLatency.labels(metricsLabels);

        statlastInvocation = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + LAST_INVOCATION,
                Gauge.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + LAST_INVOCATION)
                .help("The timestamp of the last invocation of the function.")
                .labelNames(metricsLabelNames)
                .create());
        _statlastInvocation = statlastInvocation.labels(metricsLabels);

        statTotalRecordsReceived = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + RECEIVED_TOTAL,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + RECEIVED_TOTAL)
                .help("Total number of messages received from source.")
                .labelNames(metricsLabelNames)
                .create());
        _statTotalRecordsReceived = statTotalRecordsReceived.labels(metricsLabels);

        statTotalProcessedSuccessfully1min = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + PROCESSED_SUCCESSFULLY_TOTAL_1min,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + PROCESSED_SUCCESSFULLY_TOTAL_1min)
                .help("Total number of messages processed successfully in the last 1 minute.")
                .labelNames(metricsLabelNames)
                .create());
        _statTotalProcessedSuccessfully1min = statTotalProcessedSuccessfully1min.labels(metricsLabels);

        statTotalSysExceptions1min = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL_1min,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL_1min)
                .help("Total number of system exceptions in the last 1 minute.")
                .labelNames(metricsLabelNames)
                .create());
        _statTotalSysExceptions1min = statTotalSysExceptions1min.labels(metricsLabels);

        statTotalUserExceptions1min = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + USER_EXCEPTIONS_TOTAL_1min,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + USER_EXCEPTIONS_TOTAL_1min)
                .help("Total number of user exceptions in the last 1 minute.")
                .labelNames(metricsLabelNames)
                .create());
        _statTotalUserExceptions1min = statTotalUserExceptions1min.labels(metricsLabels);

        statProcessLatency1min = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS_1min,
                Summary.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS_1min)
                .help("Process latency in milliseconds in the last 1 minute.")
                .quantile(0.5, 0.01)
                .quantile(0.9, 0.01)
                .quantile(0.99, 0.01)
                .quantile(0.999, 0.01)
                .labelNames(metricsLabelNames)
                .create());
        _statProcessLatency1min = statProcessLatency1min.labels(metricsLabels);

        statTotalRecordsReceived1min = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + RECEIVED_TOTAL_1min,
                Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + RECEIVED_TOTAL_1min)
                .help("Total number of messages received from source in the last 1 minute.")
                .labelNames(metricsLabelNames)
                .create());
        _statTotalRecordsReceived1min = statTotalRecordsReceived1min.labels(metricsLabels);

        userExceptions = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + "user_exception",
                Gauge.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + "user_exception")
                .labelNames(exceptionMetricsLabelNames)
                .help("Exception from user code.")
                .create());
        sysExceptions = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + "system_exception",
                Gauge.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + "system_exception")
                .labelNames(exceptionMetricsLabelNames)
                .help("Exception from system code.")
                .create());

        sourceExceptions = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + "source_exception",
                Gauge.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + "source_exception")
                .labelNames(exceptionMetricsLabelNames)
                .help("Exception from source.")
                .create());

        sinkExceptions = collectorRegistry.registerIfNotExist(
                PULSAR_FUNCTION_METRICS_PREFIX + "sink_exception",
                Gauge.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + "sink_exception")
                .labelNames(exceptionMetricsLabelNames)
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
        _statTotalRecordsReceived.inc();
        _statTotalRecordsReceived1min.inc();
    }

    @Override
    public void incrTotalProcessedSuccessfully() {
        _statTotalProcessedSuccessfully.inc();
        _statTotalProcessedSuccessfully1min.inc();
    }

    @Override
    public void incrSysExceptions(Throwable sysException) {
        _statTotalSysExceptions.inc();
        _statTotalSysExceptions1min.inc();
        addSystemException(sysException);
    }

    @Override
    public void incrUserExceptions(Throwable userException) {
        _statTotalUserExceptions.inc();
        _statTotalUserExceptions1min.inc();
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
        _statlastInvocation.set(ts);
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
            _statProcessLatency.observe(endTimeMs);
            _statProcessLatency1min.observe(endTimeMs);
        }
    }

    @Override
    public double getTotalProcessedSuccessfully() {
        return _statTotalProcessedSuccessfully.get();
    }

    @Override
    public double getTotalRecordsReceived() {
        return _statTotalRecordsReceived.get();
    }

    @Override
    public double getTotalSysExceptions() {
        return _statTotalSysExceptions.get();
    }

    @Override
    public double getTotalUserExceptions() {
        return _statTotalUserExceptions.get();
    }

    @Override
    public double getLastInvocation() {
        return _statlastInvocation.get();
    }

    public double getAvgProcessLatency() {
        return _statProcessLatency.get().count <= 0.0
                ? 0 : _statProcessLatency.get().sum / _statProcessLatency.get().count;
    }

    public double getProcessLatency50P() {
        return _statProcessLatency.get().quantiles.get(0.5);
    }

    public double getProcessLatency90P() {
        return _statProcessLatency.get().quantiles.get(0.9);
    }

    public double getProcessLatency99P() {
        return _statProcessLatency.get().quantiles.get(0.99);
    }

    public double getProcessLatency99_9P() {
        return _statProcessLatency.get().quantiles.get(0.999);
    }

    @Override
    public double getTotalProcessedSuccessfully1min() {
        return _statTotalProcessedSuccessfully1min.get();
    }

    @Override
    public double getTotalRecordsReceived1min() {
        return _statTotalRecordsReceived1min.get();
    }

    @Override
    public double getTotalSysExceptions1min() {
        return _statTotalSysExceptions1min.get();
    }

    @Override
    public double getTotalUserExceptions1min() {
        return _statTotalUserExceptions1min.get();
    }

    @Override
    public double getAvgProcessLatency1min() {
        return _statProcessLatency1min.get().count <= 0.0
                ? 0 : _statProcessLatency1min.get().sum / _statProcessLatency1min.get().count;
    }

    @Override
    public EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> getLatestSourceExceptions() {
        return EMPTY_QUEUE;
    }

    @Override
    public EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> getLatestSinkExceptions() {
        return EMPTY_QUEUE;
    }

    public double getProcessLatency50P1min() {
        return _statProcessLatency1min.get().quantiles.get(0.5);
    }

    public double getProcessLatency90P1min() {
        return _statProcessLatency1min.get().quantiles.get(0.9);
    }

    public double getProcessLatency99P1min() {
        return _statProcessLatency1min.get().quantiles.get(0.99);
    }

    public double getProcessLatency99_9P1min() {
        return _statProcessLatency1min.get().quantiles.get(0.999);
    }

    @Override
    public void reset() {
        statTotalProcessedSuccessfully1min.clear();
        _statTotalProcessedSuccessfully1min = statTotalProcessedSuccessfully1min.labels(metricsLabels);

        statTotalSysExceptions1min.clear();
        _statTotalSysExceptions1min = statTotalSysExceptions1min.labels(metricsLabels);

        statTotalUserExceptions1min.clear();
        _statTotalUserExceptions1min = statTotalUserExceptions1min.labels(metricsLabels);

        statProcessLatency1min.clear();
        _statProcessLatency1min = statProcessLatency1min.labels(metricsLabels);

        statTotalRecordsReceived1min.clear();
        _statTotalRecordsReceived1min = statTotalRecordsReceived1min.labels(metricsLabels);
    }
}
