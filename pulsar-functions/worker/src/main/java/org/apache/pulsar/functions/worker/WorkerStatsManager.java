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
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.function.Supplier;
import lombok.Setter;
import org.apache.pulsar.common.util.DirectMemoryUtils;
import org.apache.pulsar.functions.instance.stats.PrometheusTextFormat;
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


  private static final String[] metricsLabelNames = {"cluster"};
  private final String[] metricsLabels;

  private FunctionRuntimeManager functionRuntimeManager;

  private FunctionMetaDataManager functionMetaDataManager;

  @Setter
  private LeaderService leaderService;

  private Supplier<Boolean> isLeader;

  private CollectorRegistry collectorRegistry = new CollectorRegistry();

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
  private final Summary.Child statWorkerStartupTimeChild;
  private final Gauge.Child statNumInstancesChild;
  private final Summary.Child scheduleTotalExecutionTimeChild;
  private final Summary.Child scheduleStrategyExecutionTimeChild;
  private final Summary.Child rebalanceTotalExecutionTimeChild;
  private final Summary.Child rebalanceStrategyExecutionTimeChild;
  private final Summary.Child stopInstanceProcessTimeChild;
  private final Summary.Child startInstanceProcessTimeChild;
  private final Summary.Child drainTotalExecutionTimeChild;

  public WorkerStatsManager(WorkerConfig workerConfig, boolean runAsStandalone) {
    this(workerConfig, runAsStandalone,
        OpenTelemetry.noop().getMeter(PulsarWorkerOpenTelemetry.INSTRUMENTATION_SCOPE_NAME));
  }

  public WorkerStatsManager(WorkerConfig workerConfig, boolean runAsStandalone, Meter meter) {

    metricsLabels = new String[]{workerConfig.getPulsarFunctionsCluster()};
    this.openTelemetryWorkerStats = new OpenTelemetryWorkerStats(meter);

    statWorkerStartupTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + START_UP_TIME)
      .help("Worker service startup time in milliseconds.")
      .labelNames(metricsLabelNames)
      .register(collectorRegistry);
    statWorkerStartupTimeChild = statWorkerStartupTime.labels(metricsLabels);

    statNumInstances = Gauge.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + INSTANCE_COUNT)
      .help("Number of instances run by this worker.")
      .labelNames(metricsLabelNames)
      .register(collectorRegistry);
    statNumInstancesChild = statNumInstances.labels(metricsLabels);

    scheduleTotalExecutionTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + SCHEDULE_TOTAL_EXEC_TIME)
      .help("Total execution time of schedule in milliseconds.")
      .labelNames(metricsLabelNames)
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(1, 0.01)
      .register(collectorRegistry);
    scheduleTotalExecutionTimeChild = scheduleTotalExecutionTime.labels(metricsLabels);

    scheduleStrategyExecutionTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + SCHEDULE_STRATEGY_EXEC_TIME)
      .help("Execution time of schedule strategy in milliseconds.")
      .labelNames(metricsLabelNames)
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(1, 0.01)
      .register(collectorRegistry);
    scheduleStrategyExecutionTimeChild = scheduleStrategyExecutionTime.labels(metricsLabels);

    rebalanceTotalExecutionTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + REBALANCE_TOTAL_EXEC_TIME)
      .help("Total execution time of a rebalance in milliseconds.")
      .labelNames(metricsLabelNames)
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(1, 0.01)
      .register(collectorRegistry);
    rebalanceTotalExecutionTimeChild = rebalanceTotalExecutionTime.labels(metricsLabels);

    rebalanceStrategyExecutionTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + REBALANCE_STRATEGY_EXEC_TIME)
      .help("Execution time of rebalance strategy in milliseconds.")
      .labelNames(metricsLabelNames)
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(1, 0.01)
      .register(collectorRegistry);
    rebalanceStrategyExecutionTimeChild = rebalanceStrategyExecutionTime.labels(metricsLabels);

    stopInstanceProcessTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + STOPPING_INSTANCE_PROCESS_TIME)
      .help("Stopping instance process time in milliseconds.")
      .labelNames(metricsLabelNames)
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(1, 0.01)
      .register(collectorRegistry);
    stopInstanceProcessTimeChild = stopInstanceProcessTime.labels(metricsLabels);

    startInstanceProcessTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + STARTING_INSTANCE_PROCESS_TIME)
      .help("Starting instance process time in milliseconds.")
      .labelNames(metricsLabelNames)
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(1, 0.01)
      .register(collectorRegistry);
    startInstanceProcessTimeChild = startInstanceProcessTime.labels(metricsLabels);

    drainTotalExecutionTime = Summary.build()
            .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + DRAIN_TOTAL_EXEC_TIME)
            .help("Total execution time of a drain in milliseconds.")
            .labelNames(metricsLabelNames)
            .quantile(0.5, 0.01)
            .quantile(0.9, 0.01)
            .quantile(1, 0.01)
            .register(collectorRegistry);
    drainTotalExecutionTimeChild = drainTotalExecutionTime.labels(metricsLabels);

    if (runAsStandalone) {
      Gauge.build("jvm_memory_direct_bytes_used", "-").create().setChild(new Gauge.Child() {
        @Override
        public double get() {
          return getJvmDirectMemoryUsed();
        }
      }).register(CollectorRegistry.defaultRegistry);

      Gauge.build("jvm_memory_direct_bytes_max", "-").create().setChild(new Gauge.Child() {
        @Override
        public double get() {
          return DirectMemoryUtils.jvmMaxDirectMemory();
        }
      }).register(CollectorRegistry.defaultRegistry);
    }
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

    StringWriter outputWriter = new StringWriter();

    PrometheusTextFormat.write004(outputWriter, collectorRegistry.metricFamilySamples());

    generateLeaderMetrics(outputWriter);
    return outputWriter.toString();
  }

  private void generateLeaderMetrics(StringWriter stream) {
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

  private void writeMetric(String metricName, long value, StringWriter stream) {
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
