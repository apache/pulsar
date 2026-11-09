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
package org.apache.pulsar.functions.worker;

import static org.apache.pulsar.common.stats.JvmMetrics.getJvmDirectMemoryUsed;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.hotspot.DefaultExports;
import io.prometheus.metrics.core.datapoints.DistributionDataPoint;
import io.prometheus.metrics.core.datapoints.GaugeDataPoint;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Summary;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Setter;
import org.apache.pulsar.common.util.DirectMemoryUtils;
import org.apache.pulsar.functions.proto.FunctionMetaData;
import org.apache.pulsar.opentelemetry.annotations.PulsarDeprecatedMetric;

public class WorkerStatsManager implements AutoCloseable {

  static {
    DefaultExports.initialize();
  }

  private static final String PULSAR_FUNCTION_WORKER_METRICS_PREFIX = "pulsar_function_worker_";
  private static final String START_UP_TIME = "start_up_time_ms";
  private static final String INSTANCE_COUNT = "instance_count";
  @PulsarDeprecatedMetric(newMetricName = OpenTelemetryWorkerStats.EXPECTED_INSTANCE_COUNT)
  private static final String TOTAL_EXPECTED_INSTANCE_COUNT = "total_expected_instance_count";
  @PulsarDeprecatedMetric(newMetricName = OpenTelemetryWorkerStats.FUNCTION_COUNT)
  private static final String TOTAL_FUNCTIONS_COUNT = "total_function_count";
  private static final String SCHEDULE_TOTAL_EXEC_TIME = "schedule_execution_time_total_ms";
  private static final String SCHEDULE_STRATEGY_EXEC_TIME = "schedule_strategy_execution_time_ms";
  private static final String REBALANCE_TOTAL_EXEC_TIME = "rebalance_execution_time_total_ms";
  private static final String REBALANCE_STRATEGY_EXEC_TIME = "rebalance_strategy_execution_time_ms";
  private static final String STOPPING_INSTANCE_PROCESS_TIME = "stop_instance_process_time_ms";
  private static final String STARTING_INSTANCE_PROCESS_TIME = "start_instance_process_time_ms";
  private static final String DRAIN_TOTAL_EXEC_TIME = "drain_execution_time_total_ms";
  @PulsarDeprecatedMetric(newMetricName = OpenTelemetryWorkerStats.LEADER)
  private static final String IS_LEADER = "is_leader";

  /** simpleclient kept quantiles over a 10 minute window; the 1.x default is 5 minutes. */
  private static final long QUANTILE_WINDOW_SECONDS = TimeUnit.MINUTES.toSeconds(10);
  private static final PrometheusTextFormatWriter TEXT_WRITER = PrometheusTextFormatWriter.builder()
      .setIncludeCreatedTimestamps(false).build();

  private static final String[] metricsLabelNames = {"cluster"};
  private final String[] metricsLabels;

  private FunctionRuntimeManager functionRuntimeManager;

  private FunctionMetaDataManager functionMetaDataManager;

  @Setter
  private LeaderService leaderService;

  private Supplier<Boolean> isLeader;

  private final PrometheusRegistry registry = new PrometheusRegistry();

  private final OpenTelemetryWorkerStats openTelemetryWorkerStats;

  @PulsarDeprecatedMetric(newMetricName = OpenTelemetryWorkerStats.STARTUP_DURATION)
  private final Summary statWorkerStartupTime;
  @PulsarDeprecatedMetric(newMetricName = OpenTelemetryWorkerStats.INSTANCE_COUNT)
  private final Gauge statNumInstances;
  @PulsarDeprecatedMetric(newMetricName = OpenTelemetryWorkerStats.SCHEDULE_DURATION)
  private final Summary scheduleTotalExecutionTime;
  @PulsarDeprecatedMetric(newMetricName = OpenTelemetryWorkerStats.SCHEDULE_STRATEGY_DURATION)
  private final Summary scheduleStrategyExecutionTime;
  @PulsarDeprecatedMetric(newMetricName = OpenTelemetryWorkerStats.REBALANCE_DURATION)
  private final Summary rebalanceTotalExecutionTime;
  @PulsarDeprecatedMetric(newMetricName = OpenTelemetryWorkerStats.REBALANCE_STRATEGY_DURATION)
  private final Summary rebalanceStrategyExecutionTime;
  @PulsarDeprecatedMetric(newMetricName = OpenTelemetryWorkerStats.INSTANCE_STOP_DURATION)
  private final Summary stopInstanceProcessTime;
  @PulsarDeprecatedMetric(newMetricName = OpenTelemetryWorkerStats.INSTANCE_START_DURATION)
  private final Summary startInstanceProcessTime;
  @PulsarDeprecatedMetric(newMetricName = OpenTelemetryWorkerStats.DRAIN_DURATION)
  private final Summary drainTotalExecutionTime;

  // As an optimization
  private final DistributionDataPoint statWorkerStartupTimeChild;
  private final GaugeDataPoint statNumInstancesChild;
  private final DistributionDataPoint scheduleTotalExecutionTimeChild;
  private final DistributionDataPoint scheduleStrategyExecutionTimeChild;
  private final DistributionDataPoint rebalanceTotalExecutionTimeChild;
  private final DistributionDataPoint rebalanceStrategyExecutionTimeChild;
  private final DistributionDataPoint stopInstanceProcessTimeChild;
  private final DistributionDataPoint startInstanceProcessTimeChild;
  private final DistributionDataPoint drainTotalExecutionTimeChild;

  public WorkerStatsManager(WorkerConfig workerConfig, boolean runAsStandalone) {
    this(workerConfig, runAsStandalone,
        OpenTelemetry.noop().getMeter(PulsarWorkerOpenTelemetry.INSTRUMENTATION_SCOPE_NAME));
  }

  public WorkerStatsManager(WorkerConfig workerConfig, boolean runAsStandalone, Meter meter) {

    metricsLabels = new String[]{workerConfig.getPulsarFunctionsCluster()};
    this.openTelemetryWorkerStats = new OpenTelemetryWorkerStats(meter);

    statWorkerStartupTime = Summary.builder()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + START_UP_TIME)
      .help("Worker service startup time in milliseconds.")
      .labelNames(metricsLabelNames)
      .maxAgeSeconds(QUANTILE_WINDOW_SECONDS)
      .register(registry);
    statWorkerStartupTimeChild = statWorkerStartupTime.labelValues(metricsLabels);

    statNumInstances = Gauge.builder()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + INSTANCE_COUNT)
      .help("Number of instances run by this worker.")
      .labelNames(metricsLabelNames)
      .register(registry);
    statNumInstancesChild = statNumInstances.labelValues(metricsLabels);

    scheduleTotalExecutionTime = durationSummary(SCHEDULE_TOTAL_EXEC_TIME,
        "Total execution time of schedule in milliseconds.");
    scheduleTotalExecutionTimeChild = scheduleTotalExecutionTime.labelValues(metricsLabels);

    scheduleStrategyExecutionTime = durationSummary(SCHEDULE_STRATEGY_EXEC_TIME,
        "Execution time of schedule strategy in milliseconds.");
    scheduleStrategyExecutionTimeChild = scheduleStrategyExecutionTime.labelValues(metricsLabels);

    rebalanceTotalExecutionTime = durationSummary(REBALANCE_TOTAL_EXEC_TIME,
        "Total execution time of a rebalance in milliseconds.");
    rebalanceTotalExecutionTimeChild = rebalanceTotalExecutionTime.labelValues(metricsLabels);

    rebalanceStrategyExecutionTime = durationSummary(REBALANCE_STRATEGY_EXEC_TIME,
        "Execution time of rebalance strategy in milliseconds.");
    rebalanceStrategyExecutionTimeChild = rebalanceStrategyExecutionTime.labelValues(metricsLabels);

    stopInstanceProcessTime = durationSummary(STOPPING_INSTANCE_PROCESS_TIME,
        "Stopping instance process time in milliseconds.");
    stopInstanceProcessTimeChild = stopInstanceProcessTime.labelValues(metricsLabels);

    startInstanceProcessTime = durationSummary(STARTING_INSTANCE_PROCESS_TIME,
        "Starting instance process time in milliseconds.");
    startInstanceProcessTimeChild = startInstanceProcessTime.labelValues(metricsLabels);

    drainTotalExecutionTime = durationSummary(DRAIN_TOTAL_EXEC_TIME,
        "Total execution time of a drain in milliseconds.");
    drainTotalExecutionTimeChild = drainTotalExecutionTime.labelValues(metricsLabels);

    if (runAsStandalone) {
      // JVM gauges stay on the simpleclient default registry until the JVM metrics move as one.
      io.prometheus.client.Gauge.build("jvm_memory_direct_bytes_used", "-").create()
          .setChild(new io.prometheus.client.Gauge.Child() {
            @Override
            public double get() {
              return getJvmDirectMemoryUsed();
            }
          }).register(CollectorRegistry.defaultRegistry);

      io.prometheus.client.Gauge.build("jvm_memory_direct_bytes_max", "-").create()
          .setChild(new io.prometheus.client.Gauge.Child() {
            @Override
            public double get() {
              return DirectMemoryUtils.jvmMaxDirectMemory();
            }
          }).register(CollectorRegistry.defaultRegistry);
    }
  }

  /** Millisecond duration summary with the median, p90 and max over the quantile window. */
  private Summary durationSummary(String name, String help) {
    return Summary.builder()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + name)
      .help(help)
      .labelNames(metricsLabelNames)
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(1, 0.01)
      .maxAgeSeconds(QUANTILE_WINDOW_SECONDS)
      .register(registry);
  }

  public void setFunctionRuntimeManager(FunctionRuntimeManager functionRuntimeManager) {
    this.functionRuntimeManager = functionRuntimeManager;
    openTelemetryWorkerStats.setFunctionRuntimeManager(functionRuntimeManager);
  }

  public void setFunctionMetaDataManager(FunctionMetaDataManager functionMetaDataManager) {
    this.functionMetaDataManager = functionMetaDataManager;
    openTelemetryWorkerStats.setFunctionMetaDataManager(functionMetaDataManager);
  }

  public void setIsLeader(Supplier<Boolean> isLeader) {
    this.isLeader = isLeader;
    openTelemetryWorkerStats.setIsLeader(isLeader);
  }

  private Long startupTimeStart;
  public void startupTimeStart() {
    startupTimeStart = System.nanoTime();
  }

  public void startupTimeEnd() {
    if (startupTimeStart != null) {
      long elapsedNanos = System.nanoTime() - startupTimeStart;
      statWorkerStartupTimeChild.observe(elapsedNanos / 1.0E6D);
      openTelemetryWorkerStats.recordStartupDuration(elapsedNanos);
    }
  }

  private Long scheduleTotalExecTimeStart;
  public void scheduleTotalExecTimeStart() {
    scheduleTotalExecTimeStart = System.nanoTime();
  }

  public void scheduleTotalExecTimeEnd() {
    if (scheduleTotalExecTimeStart != null) {
      long elapsedNanos = System.nanoTime() - scheduleTotalExecTimeStart;
      scheduleTotalExecutionTimeChild.observe(elapsedNanos / 1.0E6D);
      openTelemetryWorkerStats.recordScheduleDuration(elapsedNanos);
    }
  }

  private Long scheduleStrategyExecTimeStart;
  public void scheduleStrategyExecTimeStartStart() {
    scheduleStrategyExecTimeStart = System.nanoTime();
  }

  public void scheduleStrategyExecTimeStartEnd() {
    if (scheduleStrategyExecTimeStart != null) {
      long elapsedNanos = System.nanoTime() - scheduleStrategyExecTimeStart;
      scheduleStrategyExecutionTimeChild.observe(elapsedNanos / 1.0E6D);
      openTelemetryWorkerStats.recordScheduleStrategyDuration(elapsedNanos);
    }
  }

  private Long rebalanceTotalExecTimeStart;
  public void rebalanceTotalExecTimeStart() {
    rebalanceTotalExecTimeStart = System.nanoTime();
  }

  public void rebalanceTotalExecTimeEnd() {
    if (rebalanceTotalExecTimeStart != null) {
      long elapsedNanos = System.nanoTime() - rebalanceTotalExecTimeStart;
      rebalanceTotalExecutionTimeChild.observe(elapsedNanos / 1.0E6D);
      openTelemetryWorkerStats.recordRebalanceDuration(elapsedNanos);
    }
  }

  private Long rebalanceStrategyExecTimeStart;
  public void rebalanceStrategyExecTimeStart() {
    rebalanceStrategyExecTimeStart = System.nanoTime();
  }

  public void rebalanceStrategyExecTimeEnd() {
    if (rebalanceStrategyExecTimeStart != null) {
      long elapsedNanos = System.nanoTime() - rebalanceStrategyExecTimeStart;
      rebalanceStrategyExecutionTimeChild.observe(elapsedNanos / 1.0E6D);
      openTelemetryWorkerStats.recordRebalanceStrategyDuration(elapsedNanos);
    }
  }

  private Long drainTotalExecTimeStart;
  public void drainTotalExecTimeStart() {
    drainTotalExecTimeStart = System.nanoTime();
  }

  public void drainTotalExecTimeEnd() {
    if (drainTotalExecTimeStart != null) {
      long elapsedNanos = System.nanoTime() - drainTotalExecTimeStart;
      drainTotalExecutionTimeChild.observe(elapsedNanos / 1.0E6D);
      openTelemetryWorkerStats.recordDrainDuration(elapsedNanos);
    }
  }

  private Long stopInstanceProcessTimeStart;
  public void stopInstanceProcessTimeStart() {
    stopInstanceProcessTimeStart = System.nanoTime();
  }

  public void stopInstanceProcessTimeEnd() {
    if (stopInstanceProcessTimeStart != null) {
      long elapsedNanos = System.nanoTime() - stopInstanceProcessTimeStart;
      stopInstanceProcessTimeChild.observe(elapsedNanos / 1.0E6D);
      openTelemetryWorkerStats.recordInstanceStopDuration(elapsedNanos);
    }
  }

  private Long startInstanceProcessTimeStart;
  public void startInstanceProcessTimeStart() {
    startInstanceProcessTimeStart = System.nanoTime();
  }

  public void startInstanceProcessTimeEnd() {
    if (startInstanceProcessTimeStart != null) {
      long elapsedNanos = System.nanoTime() - startInstanceProcessTimeStart;
      startInstanceProcessTimeChild.observe(elapsedNanos / 1.0E6D);
      openTelemetryWorkerStats.recordInstanceStartDuration(elapsedNanos);
    }
  }

  @Override
  public void close() {
    openTelemetryWorkerStats.close();
  }

  public String getStatsAsString() throws IOException {

    statNumInstancesChild.set(functionRuntimeManager.getMyInstances());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TEXT_WRITER.write(out, registry.scrape());

    Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
    generateLeaderMetrics(writer);
    writer.flush();
    return out.toString(StandardCharsets.UTF_8);
  }

  private void generateLeaderMetrics(Writer stream) throws IOException {
    if (isLeader.get()) {

      List<FunctionMetaData> metadata = functionMetaDataManager.getAllFunctionMetaData();
      // get total number functions
      long totalFunctions = metadata.size();
      writeMetric(TOTAL_FUNCTIONS_COUNT, totalFunctions, stream);

      // get total expected number of instances
      long totalInstances = 0;
      for (FunctionMetaData entry : metadata) {
        totalInstances += entry.getFunctionDetails().getParallelism();
      }
      writeMetric(TOTAL_EXPECTED_INSTANCE_COUNT, totalInstances, stream);

      // is this worker is the leader
      writeMetric(IS_LEADER, 1, stream);
    }
  }

  private void writeMetric(String metricName, long value, Writer stream) throws IOException {
    stream.write("# TYPE ");
    stream.write(PULSAR_FUNCTION_WORKER_METRICS_PREFIX);
    stream.write(metricName);
    stream.write(" gauge");
    stream.write("\n");

    stream.write(PULSAR_FUNCTION_WORKER_METRICS_PREFIX);
    stream.write(metricName);
    stream.write("{");

    for (int i = 0; i < metricsLabelNames.length; i++) {
      stream.write(metricsLabelNames[i]);
      stream.write('=');
      stream.write('\"');
      stream.write(metricsLabels[i]);
      stream.write("\",");
    }
    stream.write('}');

    stream.write(' ');
    stream.write(String.valueOf(value));
    stream.write('\n');
  }
}
