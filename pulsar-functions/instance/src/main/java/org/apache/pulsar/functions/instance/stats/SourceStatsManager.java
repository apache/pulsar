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
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.functions.proto.FunctionStatus;

public class SourceStatsManager extends ComponentStatsManager {

    public static final String PULSAR_SOURCE_METRICS_PREFIX = "pulsar_source_";

    /** Declare metric names. **/
    public static final String SYSTEM_EXCEPTIONS_TOTAL = "system_exceptions_total";
    public static final String SOURCE_EXCEPTIONS_TOTAL = "source_exceptions_total";
    public static final String LAST_INVOCATION = "last_invocation";
    public static final String RECEIVED_TOTAL = "received_total";
    public static final String WRITTEN_TOTAL = "written_total";

    public static final String SYSTEM_EXCEPTIONS_TOTAL_1min = "system_exceptions_1min";
    public static final String SOURCE_EXCEPTIONS_TOTAL_1min = "source_exceptions_1min";
    public static final String RECEIVED_TOTAL_1min = "received_1min";
    public static final String WRITTEN_TOTAL_1min = "written_1min";

    /** Declare Prometheus stats. **/

    private final Counter statTotalRecordsReceived;

    private final Counter statTotalSysExceptions;

    private final Counter statTotalSourceExceptions;

    private final Counter statTotalWritten;

    private final Gauge statlastInvocation;

    // windowed metrics
    private final Counter statTotalRecordsReceived1min;

    private final Counter statTotalSysExceptions1min;

    private final Counter statTotalSourceExceptions1min;

    private final Counter statTotalWritten1min;

    // exceptions

    final Gauge sysExceptions;

    final Gauge sourceExceptions;

    // As an optimization
    private final CounterDataPoint statTotalRecordsReceivedChild;
    private final CounterDataPoint statTotalSysExceptionsChild;
    private final CounterDataPoint statTotalSourceExceptionsChild;
    private final CounterDataPoint statTotalWrittenChild;
    private final GaugeDataPoint statlastInvocationChild;

    private CounterDataPoint statTotalRecordsReceivedChild1min;
    private CounterDataPoint statTotalSysExceptions1minChild;
    private CounterDataPoint statTotalSourceExceptionsChild1min;
    private CounterDataPoint statTotalWrittenChild1min;

    private EvictingQueue<FunctionStatus.ExceptionInformation> latestSystemExceptions =
            EvictingQueue.create(10);
    private EvictingQueue<FunctionStatus.ExceptionInformation> latestSourceExceptions =
            EvictingQueue.create(10);

    protected final RateLimiter sysExceptionRateLimiter;

    protected final RateLimiter sourceExceptionRateLimiter;

    public SourceStatsManager(FunctionCollectorRegistry collectorRegistry, String[] metricsLabels,
                              ScheduledExecutorService
                                      scheduledExecutorService) {
        super(collectorRegistry, metricsLabels, scheduledExecutorService);

        statTotalRecordsReceived = counter(PULSAR_SOURCE_METRICS_PREFIX + RECEIVED_TOTAL,
                "Total number of records received from source.");
        statTotalRecordsReceivedChild = statTotalRecordsReceived.labelValues(metricsLabels);

        statTotalSysExceptions = counter(PULSAR_SOURCE_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL,
                "Total number of system exceptions.");
        statTotalSysExceptionsChild = statTotalSysExceptions.labelValues(metricsLabels);

        statTotalSourceExceptions = counter(PULSAR_SOURCE_METRICS_PREFIX + SOURCE_EXCEPTIONS_TOTAL,
                "Total number of source exceptions.");
        statTotalSourceExceptionsChild = statTotalSourceExceptions.labelValues(metricsLabels);

        statTotalWritten = counter(PULSAR_SOURCE_METRICS_PREFIX + WRITTEN_TOTAL,
                "Total number of records written to a Pulsar topic.");
        statTotalWrittenChild = statTotalWritten.labelValues(metricsLabels);

        statlastInvocation = gauge(PULSAR_SOURCE_METRICS_PREFIX + LAST_INVOCATION,
                "The timestamp of the last invocation of the source.");
        statlastInvocationChild = statlastInvocation.labelValues(metricsLabels);

        statTotalRecordsReceived1min = counter(PULSAR_SOURCE_METRICS_PREFIX + RECEIVED_TOTAL_1min,
                "Total number of records received from source in the last 1 minute.");
        statTotalRecordsReceivedChild1min = statTotalRecordsReceived1min.labelValues(metricsLabels);

        statTotalSysExceptions1min = counter(PULSAR_SOURCE_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL_1min,
                "Total number of system exceptions in the last 1 minute.");
        statTotalSysExceptions1minChild = statTotalSysExceptions1min.labelValues(metricsLabels);

        statTotalSourceExceptions1min = counter(PULSAR_SOURCE_METRICS_PREFIX + SOURCE_EXCEPTIONS_TOTAL_1min,
                "Total number of source exceptions in the last 1 minute.");
        statTotalSourceExceptionsChild1min = statTotalSourceExceptions1min.labelValues(metricsLabels);

        statTotalWritten1min = counter(PULSAR_SOURCE_METRICS_PREFIX + WRITTEN_TOTAL_1min,
                "Total number of records written to a Pulsar topic in the last 1 minute.");
        statTotalWrittenChild1min = statTotalWritten1min.labelValues(metricsLabels);

        sysExceptions = exceptionGauge(PULSAR_SOURCE_METRICS_PREFIX + "system_exception",
                "Exception from system code.");

        sourceExceptions = exceptionGauge(PULSAR_SOURCE_METRICS_PREFIX + "source_exception",
                "Exception from source.");

        sysExceptionRateLimiter = RateLimiter.create(5.0d / 60.0d);
        sourceExceptionRateLimiter = RateLimiter.create(5.0d / 60.0d);
    }

    @Override
    public void reset() {
        statTotalRecordsReceived1min.clear();
        statTotalRecordsReceivedChild1min = statTotalRecordsReceived1min.labelValues(metricsLabels);

        statTotalSysExceptions1min.clear();
        statTotalSysExceptions1minChild = statTotalSysExceptions1min.labelValues(metricsLabels);

        statTotalSourceExceptions1min.clear();
        statTotalSourceExceptionsChild1min = statTotalSourceExceptions1min.labelValues(metricsLabels);

        statTotalWritten1min.clear();
        statTotalWrittenChild1min = statTotalWritten1min.labelValues(metricsLabels);
    }

    @Override
    public void incrTotalReceived() {
        statTotalRecordsReceivedChild.inc();
        statTotalRecordsReceivedChild1min.inc();
    }

    @Override
    public void incrTotalProcessedSuccessfully() {
        statTotalWrittenChild.inc();
        statTotalWrittenChild1min.inc();
    }

    @Override
    public void incrSysExceptions(Throwable ex) {
        statTotalSysExceptionsChild.inc();
        statTotalSysExceptions1minChild.inc();

        long ts = System.currentTimeMillis();
        FunctionStatus.ExceptionInformation info = getExceptionInfo(ex, ts);
        latestSystemExceptions.add(info);

        // report exception throw prometheus
        if (sysExceptionRateLimiter.tryAcquire()) {
            String[] exceptionMetricsLabels = getExceptionMetricsLabels(ex);
            sysExceptions.labelValues(exceptionMetricsLabels).set(1.0);
        }
    }

    @Override
    public void incrUserExceptions(Throwable ex) {
        incrSysExceptions(ex);
    }

    @Override
    public void incrSourceExceptions(Throwable ex) {
        statTotalSourceExceptionsChild.inc();
        statTotalSourceExceptionsChild1min.inc();

        long ts = System.currentTimeMillis();
        FunctionStatus.ExceptionInformation info = getExceptionInfo(ex, ts);
        latestSourceExceptions.add(info);

        // report exception throw prometheus
        if (sourceExceptionRateLimiter.tryAcquire()) {
            String[] exceptionMetricsLabels = getExceptionMetricsLabels(ex);
            sourceExceptions.labelValues(exceptionMetricsLabels).set(1.0);
        }
    }

    private String[] getExceptionMetricsLabels(Throwable ex) {
        String[] exceptionMetricsLabels = Arrays.copyOf(metricsLabels, metricsLabels.length + 1);
        exceptionMetricsLabels[exceptionMetricsLabels.length - 1] = ex.getMessage() != null ? ex.getMessage() : "";
        return exceptionMetricsLabels;
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
        //no-op
    }

    @Override
    public double getTotalProcessedSuccessfully() {
        return statTotalWrittenChild.get();
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
        return 0;
    }

    @Override
    public double getLastInvocation() {
        return statlastInvocationChild.get();
    }

    @Override
    public double getAvgProcessLatency() {
        return 0;
    }

    @Override
    public double getTotalProcessedSuccessfully1min() {
        return statTotalWrittenChild1min.get();
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
        return 0;
    }

    @Override
    public double getAvgProcessLatency1min() {
        return 0;
    }

    @Override
    public EvictingQueue<FunctionStatus.ExceptionInformation> getLatestUserExceptions() {
        return EvictingQueue.create(0);
    }

    @Override
    public EvictingQueue<FunctionStatus.ExceptionInformation> getLatestSystemExceptions() {
        return latestSystemExceptions;
    }

    @Override
    public EvictingQueue<FunctionStatus.ExceptionInformation> getLatestSourceExceptions() {
        return latestSourceExceptions;
    }

    @Override
    public EvictingQueue<FunctionStatus.ExceptionInformation> getLatestSinkExceptions() {
        return EvictingQueue.create(0);
    }
}
