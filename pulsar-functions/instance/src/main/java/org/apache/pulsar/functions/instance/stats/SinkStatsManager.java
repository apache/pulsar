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
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import lombok.Getter;
import org.apache.pulsar.common.util.RateLimiter;
import org.apache.pulsar.functions.proto.InstanceCommunication;

import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SinkStatsManager extends ComponentStatsManager {

    public static final String PULSAR_SINK_METRICS_PREFIX = "pulsar_sink_";

    /** Declare metric names **/
    public static final String SYSTEM_EXCEPTIONS_TOTAL = "system_exceptions_total";
    public static final String SINK_EXCEPTIONS_TOTAL = "sink_exceptions_total";
    public static final String LAST_INVOCATION = "last_invocation";
    public static final String RECEIVED_TOTAL = "received_total";
    public static final String WRITTEN_TOTAL = "written_total";

    public static final String SYSTEM_EXCEPTIONS_TOTAL_1min = "system_exceptions_total_1min";
    public static final String SINK_EXCEPTIONS_TOTAL_1min = "sink_exceptions_total_1min";
    public static final String RECEIVED_TOTAL_1min = "received_total_1min";
    public static final String WRITTEN_TOTAL_1min = "written_total_1min";

    /** Declare Prometheus stats **/

    private final Counter statTotalRecordsReceived;

    private final Counter statTotalSysExceptions;

    private final Counter statTotalSinkExceptions;

    private final Counter statTotalWritten;

    private final Gauge statlastInvocation;

    // windowed metrics
    private final Counter statTotalRecordsReceived1min;

    private final Counter statTotalSysExceptions1min;

    private final Counter statTotalSinkExceptions1min;

    private final Counter statTotalWritten1min;

    // exceptions

    final Gauge sysExceptions;

    final Gauge sinkExceptions;

    // As an optimization
    private final Counter.Child _statTotalRecordsReceived;
    private final Counter.Child _statTotalSysExceptions;
    private final Counter.Child _statTotalSinkExceptions;
    private final Counter.Child _statTotalWritten;
    private final Gauge.Child _statlastInvocation;

    private Counter.Child _statTotalRecordsReceived1min;
    private Counter.Child _statTotalSysExceptions1min;
    private Counter.Child _statTotalSinkExceptions1min;
    private Counter.Child _statTotalWritten1min;

    @Getter
    private EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> latestSystemExceptions = EvictingQueue.create(10);
    @Getter
    private EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> latestSinkExceptions = EvictingQueue.create(10);

    private final RateLimiter sysExceptionRateLimiter;

    private final RateLimiter sinkExceptionRateLimiter;


    public SinkStatsManager(CollectorRegistry collectorRegistry, String[] metricsLabels, ScheduledExecutorService
            scheduledExecutorService) {
        super(collectorRegistry, metricsLabels, scheduledExecutorService);

        statTotalRecordsReceived = Counter.build()
                .name(PULSAR_SINK_METRICS_PREFIX + RECEIVED_TOTAL)
                .help("Total number of records sink has received from Pulsar topic(s).")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);
        _statTotalRecordsReceived = statTotalRecordsReceived.labels(metricsLabels);

        statTotalSysExceptions = Counter.build()
                .name(PULSAR_SINK_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL)
                .help("Total number of system exceptions.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);
        _statTotalSysExceptions = statTotalSysExceptions.labels(metricsLabels);

        statTotalSinkExceptions = Counter.build()
                .name(PULSAR_SINK_METRICS_PREFIX + SINK_EXCEPTIONS_TOTAL)
                .help("Total number of sink exceptions.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);
        _statTotalSinkExceptions = statTotalSinkExceptions.labels(metricsLabels);

        statTotalWritten = Counter.build()
                .name(PULSAR_SINK_METRICS_PREFIX + WRITTEN_TOTAL)
                .help("Total number of records processed by sink.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);
        _statTotalWritten = statTotalWritten.labels(metricsLabels);

        statlastInvocation = Gauge.build()
                .name(PULSAR_SINK_METRICS_PREFIX + LAST_INVOCATION)
                .help("The timestamp of the last invocation of the sink.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);
        _statlastInvocation = statlastInvocation.labels(metricsLabels);

        statTotalRecordsReceived1min = Counter.build()
                .name(PULSAR_SINK_METRICS_PREFIX + RECEIVED_TOTAL_1min)
                .help("Total number of messages sink has received from Pulsar topic(s) in the last 1 minute.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);
        _statTotalRecordsReceived1min = statTotalRecordsReceived1min.labels(metricsLabels);

        statTotalSysExceptions1min = Counter.build()
                .name(PULSAR_SINK_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL_1min)
                .help("Total number of system exceptions in the last 1 minute.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);
        _statTotalSysExceptions1min = statTotalSysExceptions1min.labels(metricsLabels);

        statTotalSinkExceptions1min = Counter.build()
                .name(PULSAR_SINK_METRICS_PREFIX + SINK_EXCEPTIONS_TOTAL_1min)
                .help("Total number of sink exceptions in the last 1 minute.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);
        _statTotalSinkExceptions1min = statTotalSinkExceptions1min.labels(metricsLabels);

        statTotalWritten1min = Counter.build()
                .name(PULSAR_SINK_METRICS_PREFIX + WRITTEN_TOTAL_1min)
                .help("Total number of records processed by sink the last 1 minute.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);
        _statTotalWritten1min = statTotalWritten1min.labels(metricsLabels);

        sysExceptions = Gauge.build()
                .name(PULSAR_SINK_METRICS_PREFIX + "system_exception")
                .labelNames(exceptionMetricsLabelNames)
                .help("Exception from system code.")
                .register(collectorRegistry);

        sinkExceptions = Gauge.build()
                .name(PULSAR_SINK_METRICS_PREFIX + "sink_exception")
                .labelNames(exceptionMetricsLabelNames)
                .help("Exception from sink.")
                .register(collectorRegistry);

        sysExceptionRateLimiter = new RateLimiter(scheduledExecutorService, 5, 1, TimeUnit.MINUTES, null);
        sinkExceptionRateLimiter = new RateLimiter(scheduledExecutorService, 5, 1, TimeUnit.MINUTES, null);
    }

    @Override
    public void reset() {
        statTotalRecordsReceived1min.clear();
        _statTotalRecordsReceived1min = statTotalRecordsReceived1min.labels(metricsLabels);

        statTotalSysExceptions1min.clear();
        _statTotalSysExceptions1min = statTotalSysExceptions1min.labels(metricsLabels);

        statTotalSinkExceptions1min.clear();
        _statTotalSinkExceptions1min = statTotalSinkExceptions1min.labels(metricsLabels);

        statTotalWritten1min.clear();
        _statTotalWritten1min = statTotalWritten1min.labels(metricsLabels);
    }

    @Override
    public void incrTotalReceived() {
        _statTotalRecordsReceived.inc();
        _statTotalRecordsReceived1min.inc();
    }

    @Override
    public void incrTotalProcessedSuccessfully() {
        _statTotalWritten.inc();
        _statTotalWritten1min.inc();
    }

    @Override
    public void incrSysExceptions(Throwable ex) {
        long ts = System.currentTimeMillis();
        InstanceCommunication.FunctionStatus.ExceptionInformation info = getExceptionInfo(ex, ts);
        latestSystemExceptions.add(info);

        // report exception throw prometheus
        if (sysExceptionRateLimiter.tryAcquire()) {
            String[] exceptionMetricsLabels = getExceptionMetricsLabels(ex, ts);
            sysExceptions.labels(exceptionMetricsLabels).set(1.0);
        }
    }

    @Override
    public void incrUserExceptions(Throwable ex) {
        incrSysExceptions(ex);
    }

    @Override
    public void incrSourceExceptions(Throwable ex) {
        incrSysExceptions(ex);
    }

    @Override
    public void incrSinkExceptions(Throwable ex) {
        long ts = System.currentTimeMillis();
        InstanceCommunication.FunctionStatus.ExceptionInformation info = getExceptionInfo(ex, ts);
        latestSinkExceptions.add(info);

        // report exception throw prometheus
        if (sinkExceptionRateLimiter.tryAcquire()) {
            String[] exceptionMetricsLabels = getExceptionMetricsLabels(ex, ts);
            sinkExceptions.labels(exceptionMetricsLabels).set(1.0);
        }
    }

    private String[] getExceptionMetricsLabels(Throwable ex, long ts) {
        String[] exceptionMetricsLabels = Arrays.copyOf(metricsLabels, metricsLabels.length + 2);
        exceptionMetricsLabels[exceptionMetricsLabels.length - 2] = ex.getMessage() != null ? ex.getMessage() : "";
        exceptionMetricsLabels[exceptionMetricsLabels.length - 1] = String.valueOf(ts);
        return exceptionMetricsLabels;
    }

    @Override
    public void setLastInvocation(long ts) {
        _statlastInvocation.set(ts);
    }

    @Override
    public void processTimeStart() {
        //no-p[
    }

    @Override
    public void processTimeEnd() {
        //no-op
    }

    @Override
    public double getTotalProcessedSuccessfully() {
        return _statTotalWritten.get();
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
        return 0;
    }

    @Override
    public double getLastInvocation() {
        return _statlastInvocation.get();
    }

    @Override
    public double getAvgProcessLatency() {
        return 0;
    }

    @Override
    public double getTotalProcessedSuccessfully1min() {
        return _statTotalWritten1min.get();
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
        return 0;
    }

    @Override
    public double getAvgProcessLatency1min() {
        return 0;
    }

    @Override
    public EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> getLatestUserExceptions() {
        return EMPTY_QUEUE;
    }

    @Override
    public EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> getLatestSystemExceptions() {
        return latestSystemExceptions;
    }

    @Override
    public EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> getLatestSourceExceptions() {
        return EMPTY_QUEUE;
    }

    @Override
    public EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> getLatestSinkExceptions() {
        return latestSinkExceptions;
    }
}
