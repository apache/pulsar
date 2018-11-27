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
package org.apache.pulsar.functions.instance;

import com.google.common.collect.EvictingQueue;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.common.TextFormat;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.proto.InstanceCommunication;

import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Function stats.
 */
@Slf4j
@Getter
@Setter
public class FunctionStatsManager implements AutoCloseable {

    static final String[] metricsLabelNames = {"tenant", "namespace", "function", "instance_id", "cluster"};

    public static final String PULSAR_FUNCTION_METRICS_PREFIX = "pulsar_function_";
    public final static String USER_METRIC_PREFIX = "user_metric_";

    /** Declare metric names **/
    public static final String PROCESSED_TOTAL = "processed_total";
    public static final String PROCESSED_SUCCESSFULLY_TOTAL = "processed_successfully_total";
    public static final String SYSTEM_EXCEPTIONS_TOTAL = "system_exceptions_total";
    public static final String USER_EXCEPTIONS_TOTAL = "user_exceptions_total";
    public static final String PROCESS_LATENCY_MS = "process_latency_ms";
    public static final String LAST_INVOCATION = "last_invocation";
    public static final String RECEIVED_TOTAL = "received_total";

    public static final String PROCESSED_TOTAL_1min = "processed_total_1min";
    public static final String PROCESSED_SUCCESSFULLY_TOTAL_1min = "processed_successfully_total_1min";
    public static final String SYSTEM_EXCEPTIONS_TOTAL_1min = "system_exceptions_total_1min";
    public static final String USER_EXCEPTIONS_TOTAL_1min = "user_exceptions_total_1min";
    public static final String PROCESS_LATENCY_MS_1min = "process_latency_ms_1min";
    public static final String RECEIVED_TOTAL_1min = "received_total_1min";

    /** Declare Prometheus stats **/

    final Counter statTotalProcessed;

    final Counter statTotalProcessedSuccessfully;

    final Counter statTotalSysExceptions;

    final Counter statTotalUserExceptions;

    final Summary statProcessLatency;

    final Gauge statlastInvocation;

    final Counter statTotalRecordsRecieved;
    
    // windowed metrics

    final Counter statTotalProcessed1min;

    final Counter statTotalProcessedSuccessfully1min;

    final Counter statTotalSysExceptions1min;

    final Counter statTotalUserExceptions1min;

    final Summary statProcessLatency1min;

    final Counter statTotalRecordsRecieved1min;

    private String[] metricsLabels;

    private ScheduledFuture<?> scheduledFuture;

    private final CollectorRegistry collectorRegistry;

    @Getter
    private EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> latestUserExceptions = EvictingQueue.create(10);
    @Getter
    private EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> latestSystemExceptions = EvictingQueue.create(10);

    public FunctionStatsManager(CollectorRegistry collectorRegistry, String[] metricsLabels, ScheduledExecutorService scheduledExecutorService) {

        this.collectorRegistry = collectorRegistry;

        this.metricsLabels = metricsLabels;

        statTotalProcessed = Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + PROCESSED_TOTAL)
                .help("Total number of messages processed.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);

        statTotalProcessedSuccessfully = Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + PROCESSED_SUCCESSFULLY_TOTAL)
                .help("Total number of messages processed successfully.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);

        statTotalSysExceptions = Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL)
                .help("Total number of system exceptions.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);

        statTotalUserExceptions = Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + USER_EXCEPTIONS_TOTAL)
                .help("Total number of user exceptions.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);

        statProcessLatency = Summary.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS)
                .help("Process latency in milliseconds.")
                .quantile(0.5, 0.01)
                .quantile(0.9, 0.01)
                .quantile(0.99, 0.01)
                .quantile(0.999, 0.01)
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);

        statlastInvocation = Gauge.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + LAST_INVOCATION)
                .help("The timestamp of the last invocation of the function.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);

        statTotalRecordsRecieved = Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + RECEIVED_TOTAL)
                .help("Total number of messages received from source.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);

        statTotalProcessed1min = Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + PROCESSED_TOTAL_1min)
                .help("Total number of messages processed in the last 1 minute.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);

        statTotalProcessedSuccessfully1min = Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + PROCESSED_SUCCESSFULLY_TOTAL_1min)
                .help("Total number of messages processed successfully in the last 1 minute.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);

        statTotalSysExceptions1min = Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + SYSTEM_EXCEPTIONS_TOTAL_1min)
                .help("Total number of system exceptions in the last 1 minute.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);

        statTotalUserExceptions1min = Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + USER_EXCEPTIONS_TOTAL_1min)
                .help("Total number of user exceptions in the last 1 minute.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);

        statProcessLatency1min = Summary.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS_1min)
                .help("Process latency in milliseconds in the last 1 minute.")
                .quantile(0.5, 0.01)
                .quantile(0.9, 0.01)
                .quantile(0.99, 0.01)
                .quantile(0.999, 0.01)
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);

        statTotalRecordsRecieved1min = Counter.build()
                .name(PULSAR_FUNCTION_METRICS_PREFIX + RECEIVED_TOTAL_1min)
                .help("Total number of messages received from source in the last 1 minute.")
                .labelNames(metricsLabelNames)
                .register(collectorRegistry);

        scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    reset();
                } catch (Exception e) {
                    log.error("Failed to reset metrics for 1min window", e);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public void addUserException(Exception ex) {
        InstanceCommunication.FunctionStatus.ExceptionInformation info =
                    InstanceCommunication.FunctionStatus.ExceptionInformation.newBuilder()
                    .setExceptionString(ex.getMessage()).setMsSinceEpoch(System.currentTimeMillis()).build();
        latestUserExceptions.add(info);
    }

    public void addSystemException(Throwable ex) {
        InstanceCommunication.FunctionStatus.ExceptionInformation info =
                InstanceCommunication.FunctionStatus.ExceptionInformation.newBuilder()
                        .setExceptionString(ex.getMessage()).setMsSinceEpoch(System.currentTimeMillis()).build();
        latestSystemExceptions.add(info);

    }

    public void incrTotalReceived() {
        statTotalRecordsRecieved.labels(metricsLabels).inc();
        statTotalRecordsRecieved1min.labels(metricsLabels).inc();
    }

    public void incrTotalProcessed() {
        statTotalProcessed.labels(metricsLabels).inc();
        statTotalProcessed1min.labels(metricsLabels).inc();
    }

    public void incrTotalProcessedSuccessfully() {
        statTotalProcessedSuccessfully.labels(metricsLabels).inc();
        statTotalProcessedSuccessfully1min.labels(metricsLabels).inc();
    }

    public void incrSysExceptions(Throwable sysException) {
        statTotalSysExceptions.labels(metricsLabels).inc();
        statTotalSysExceptions1min.labels(metricsLabels).inc();
        addSystemException(sysException);
    }

    public void incrUserExceptions(Exception userException) {
        statTotalUserExceptions.labels(metricsLabels).inc();
        statTotalUserExceptions1min.labels(metricsLabels).inc();
        addUserException(userException);
    }

    public void setLastInvocation(long ts) {
        statlastInvocation.labels(metricsLabels).set(ts);
    }

    private Long processTimeStart;
    public void processTimeStart() {
        processTimeStart = System.nanoTime();
    }

    public void processTimeEnd() {
        if (processTimeStart != null) {
            double endTimeMs = ((double) System.nanoTime() - processTimeStart) / 1.0E6D;
            statProcessLatency.labels(metricsLabels).observe(endTimeMs);
            statProcessLatency1min.labels(metricsLabels).observe(endTimeMs);
        }
    }

    public double getTotalProcessed() {
        return statTotalProcessed.labels(metricsLabels).get();
    }

    public double getTotalProcessedSuccessfully() {
        return statTotalProcessedSuccessfully.labels(metricsLabels).get();
    }

    public double getTotalRecordsReceived() {
        return statTotalRecordsRecieved.labels(metricsLabels).get();
    }

    public double getTotalSysExceptions() {
        return statTotalSysExceptions.labels(metricsLabels).get();
    }

    public double getTotalUserExceptions() {
        return statTotalUserExceptions.labels(metricsLabels).get();
    }

    public double getLastInvocation() {
        return statlastInvocation.labels(metricsLabels).get();
    }

    public double getAvgProcessLatency() {
        return statProcessLatency.labels(metricsLabels).get().count <= 0.0
                ? 0 : statProcessLatency.labels(metricsLabels).get().sum / statProcessLatency.labels(metricsLabels).get().count;
    }

    public double getProcessLatency50P() {
        return statProcessLatency.labels(metricsLabels).get().quantiles.get(0.5);
    }

    public double getProcessLatency90P() {
        return statProcessLatency.labels(metricsLabels).get().quantiles.get(0.9);
    }

    public double getProcessLatency99P() {
        return statProcessLatency.labels(metricsLabels).get().quantiles.get(0.99);
    }

    public double getProcessLatency99_9P() {
        return statProcessLatency.labels(metricsLabels).get().quantiles.get(0.999);
    }

    public double getTotalProcessed1min() {
        return statTotalProcessed1min.labels(metricsLabels).get();
    }

    public double getTotalProcessedSuccessfully1min() {
        return statTotalProcessedSuccessfully1min.labels(metricsLabels).get();
    }

    public double getTotalRecordsReceived1min() {
        return statTotalRecordsRecieved1min.labels(metricsLabels).get();
    }

    public double getTotalSysExceptions1min() {
        return statTotalSysExceptions1min.labels(metricsLabels).get();
    }

    public double getTotalUserExceptions1min() {
        return statTotalUserExceptions1min.labels(metricsLabels).get();
    }

    public double getAvgProcessLatency1min() {
        return statProcessLatency1min.labels(metricsLabels).get().count <= 0.0
                ? 0 : statProcessLatency1min.labels(metricsLabels).get().sum / statProcessLatency1min.labels(metricsLabels).get().count;
    }

    public double getProcessLatency50P1min() {
        return statProcessLatency1min.labels(metricsLabels).get().quantiles.get(0.5);
    }

    public double getProcessLatency90P1min() {
        return statProcessLatency1min.labels(metricsLabels).get().quantiles.get(0.9);
    }

    public double getProcessLatency99P1min() {
        return statProcessLatency1min.labels(metricsLabels).get().quantiles.get(0.99);
    }

    public double getProcessLatency99_9P1min() {
        return statProcessLatency1min.labels(metricsLabels).get().quantiles.get(0.999);
    }

    public void reset() {
        statTotalProcessed1min.clear();
        statTotalProcessedSuccessfully1min.clear();
        statTotalSysExceptions1min.clear();
        statTotalUserExceptions1min.clear();
        statProcessLatency1min.clear();
        statTotalRecordsRecieved1min.clear();
        latestUserExceptions.clear();
        latestSystemExceptions.clear();
    }

    public String getStatsAsString() throws IOException {
        StringWriter outputWriter = new StringWriter();

        TextFormat.write004(outputWriter, collectorRegistry.metricFamilySamples());

        return outputWriter.toString();
    }

    @Override
    public void close() {
        scheduledFuture.cancel(false);
    }
}
