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

package org.apache.pulsar.functions.worker;

import static org.apache.pulsar.common.stats.JvmMetrics.getJvmDirectMemoryUsed;

import io.netty.util.internal.PlatformDependent;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import io.prometheus.client.hotspot.DefaultExports;
import lombok.Setter;
import org.apache.pulsar.functions.instance.stats.PrometheusTextFormat;
import org.apache.pulsar.functions.proto.Function;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.function.Supplier;

public class WorkerStatsManager {

  static {
    DefaultExports.initialize();
  }

  private static final String PULSAR_FUNCTION_WORKER_METRICS_PREFIX = "pulsar_function_worker_";
  private static final String START_UP_TIME = "start_up_time_ms";
  private static final String INSTANCE_COUNT = "instance_count";
  private static final String TOTAL_EXPECTED_INSTANCE_COUNT = "total_expected_instance_count";
  private static final String TOTAL_FUNCTIONS_COUNT = "total_function_count";
  private static final String SCHEDULE_TOTAL_EXEC_TIME = "schedule_execution_time_total_ms";
  private static final String SCHEDULE_STRATEGY_EXEC_TIME = "schedule_strategy_execution_time_ms";
  private static final String REBALANCE_TOTAL_EXEC_TIME = "rebalance_execution_time_total_ms";
  private static final String REBALANCE_STRATEGY_EXEC_TIME = "rebalance_strategy_execution_time_ms";
  private static final String STOPPING_INSTANCE_PROCESS_TIME = "stop_instance_process_time_ms";
  private static final String STARTING_INSTANCE_PROCESS_TIME = "start_instance_process_time_ms";
  private static final String DRAIN_TOTAL_EXEC_TIME = "drain_execution_time_total_ms";
  private static final String IS_LEADER = "is_leader";


  private static final String[] metricsLabelNames = {"cluster"};
  private final String[] metricsLabels;

  @Setter
  private FunctionRuntimeManager functionRuntimeManager;

  @Setter
  private FunctionMetaDataManager functionMetaDataManager;

  @Setter
  private LeaderService leaderService;

  @Setter
  private Supplier<Boolean> isLeader;

  private CollectorRegistry collectorRegistry = new CollectorRegistry();

  private final Summary statWorkerStartupTime;
  private final Gauge statNumInstances;
  private final Summary scheduleTotalExecutionTime;
  private final Summary scheduleStrategyExecutionTime;
  private final Summary rebalanceTotalExecutionTime;
  private final Summary rebalanceStrategyExecutionTime;
  private final Summary stopInstanceProcessTime;
  private final Summary startInstanceProcessTime;
  private final Summary drainTotalExecutionTime;

  // As an optimization
  private final Summary.Child _statWorkerStartupTime;
  private final Gauge.Child _statNumInstances;
  private final Summary.Child _scheduleTotalExecutionTime;
  private final Summary.Child _scheduleStrategyExecutionTime;
  private final Summary.Child _rebalanceTotalExecutionTime;
  private final Summary.Child _rebalanceStrategyExecutionTime;
  private final Summary.Child _stopInstanceProcessTime;
  private final Summary.Child _startInstanceProcessTime;
  private final Summary.Child _drainTotalExecutionTime;

  public WorkerStatsManager(WorkerConfig workerConfig, boolean runAsStandalone) {

    metricsLabels = new String[]{workerConfig.getPulsarFunctionsCluster()};

    statWorkerStartupTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + START_UP_TIME)
      .help("Worker service startup time in milliseconds.")
      .labelNames(metricsLabelNames)
      .register(collectorRegistry);
    _statWorkerStartupTime = statWorkerStartupTime.labels(metricsLabels);

    statNumInstances = Gauge.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + INSTANCE_COUNT)
      .help("Number of instances run by this worker.")
      .labelNames(metricsLabelNames)
      .register(collectorRegistry);
    _statNumInstances = statNumInstances.labels(metricsLabels);

    scheduleTotalExecutionTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + SCHEDULE_TOTAL_EXEC_TIME)
      .help("Total execution time of schedule in milliseconds.")
      .labelNames(metricsLabelNames)
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(1, 0.01)
      .register(collectorRegistry);
    _scheduleTotalExecutionTime = scheduleTotalExecutionTime.labels(metricsLabels);

    scheduleStrategyExecutionTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + SCHEDULE_STRATEGY_EXEC_TIME)
      .help("Execution time of schedule strategy in milliseconds.")
      .labelNames(metricsLabelNames)
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(1, 0.01)
      .register(collectorRegistry);
    _scheduleStrategyExecutionTime = scheduleStrategyExecutionTime.labels(metricsLabels);

    rebalanceTotalExecutionTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + REBALANCE_TOTAL_EXEC_TIME)
      .help("Total execution time of a rebalance in milliseconds.")
      .labelNames(metricsLabelNames)
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(1, 0.01)
      .register(collectorRegistry);
    _rebalanceTotalExecutionTime = rebalanceTotalExecutionTime.labels(metricsLabels);

    rebalanceStrategyExecutionTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + REBALANCE_STRATEGY_EXEC_TIME)
      .help("Execution time of rebalance strategy in milliseconds.")
      .labelNames(metricsLabelNames)
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(1, 0.01)
      .register(collectorRegistry);
    _rebalanceStrategyExecutionTime = rebalanceStrategyExecutionTime.labels(metricsLabels);

    stopInstanceProcessTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + STOPPING_INSTANCE_PROCESS_TIME)
      .help("Stopping instance process time in milliseconds.")
      .labelNames(metricsLabelNames)
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(1, 0.01)
      .register(collectorRegistry);
    _stopInstanceProcessTime = stopInstanceProcessTime.labels(metricsLabels);

    startInstanceProcessTime = Summary.build()
      .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + STARTING_INSTANCE_PROCESS_TIME)
      .help("Starting instance process time in milliseconds.")
      .labelNames(metricsLabelNames)
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(1, 0.01)
      .register(collectorRegistry);
    _startInstanceProcessTime = startInstanceProcessTime.labels(metricsLabels);

    drainTotalExecutionTime = Summary.build()
            .name(PULSAR_FUNCTION_WORKER_METRICS_PREFIX + DRAIN_TOTAL_EXEC_TIME)
            .help("Total execution time of a drain in milliseconds.")
            .labelNames(metricsLabelNames)
            .quantile(0.5, 0.01)
            .quantile(0.9, 0.01)
            .quantile(1, 0.01)
            .register(collectorRegistry);
    _drainTotalExecutionTime = drainTotalExecutionTime.labels(metricsLabels);

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
          return PlatformDependent.maxDirectMemory();
        }
      }).register(CollectorRegistry.defaultRegistry);
    }
  }

  private Long startupTimeStart;
  public void startupTimeStart() {
    startupTimeStart = System.nanoTime();
  }

  public void startupTimeEnd() {
    if (startupTimeStart != null) {
      double endTimeMs = ((double) System.nanoTime() - startupTimeStart) / 1.0E6D;
      _statWorkerStartupTime.observe(endTimeMs);
    }
  }

  private Long scheduleTotalExecTimeStart;
  public void scheduleTotalExecTimeStart() {
    scheduleTotalExecTimeStart = System.nanoTime();
  }

  public void scheduleTotalExecTimeEnd() {
    if (scheduleTotalExecTimeStart != null) {
      double endTimeMs = ((double) System.nanoTime() - scheduleTotalExecTimeStart) / 1.0E6D;
      _scheduleTotalExecutionTime.observe(endTimeMs);
    }
  }

  private Long scheduleStrategyExecTimeStart;
  public void scheduleStrategyExecTimeStartStart() {
    scheduleStrategyExecTimeStart = System.nanoTime();
  }

  public void scheduleStrategyExecTimeStartEnd() {
    if (scheduleStrategyExecTimeStart != null) {
      double endTimeMs = ((double) System.nanoTime() - scheduleStrategyExecTimeStart) / 1.0E6D;
      _scheduleStrategyExecutionTime.observe(endTimeMs);
    }
  }

  private Long rebalanceTotalExecTimeStart;
  public void rebalanceTotalExecTimeStart() {
    rebalanceTotalExecTimeStart = System.nanoTime();
  }

  public void rebalanceTotalExecTimeEnd() {
    if (rebalanceTotalExecTimeStart != null) {
      double endTimeMs = ((double) System.nanoTime() - rebalanceTotalExecTimeStart) / 1.0E6D;
      _rebalanceTotalExecutionTime.observe(endTimeMs);
    }
  }

  private Long rebalanceStrategyExecTimeStart;
  public void rebalanceStrategyExecTimeStart() {
    rebalanceStrategyExecTimeStart = System.nanoTime();
  }

  public void rebalanceStrategyExecTimeEnd() {
    if (rebalanceStrategyExecTimeStart != null) {
      double endTimeMs = ((double) System.nanoTime() - rebalanceStrategyExecTimeStart) / 1.0E6D;
      _rebalanceStrategyExecutionTime.observe(endTimeMs);
    }
  }

  private Long drainTotalExecTimeStart;
  public void drainTotalExecTimeStart() {
    drainTotalExecTimeStart = System.nanoTime();
  }

  public void drainTotalExecTimeEnd() {
    if (drainTotalExecTimeStart != null) {
      double endTimeMs = ((double) System.nanoTime() - drainTotalExecTimeStart) / 1.0E6D;
      _drainTotalExecutionTime.observe(endTimeMs);
    }
  }

  private Long stopInstanceProcessTimeStart;
  public void stopInstanceProcessTimeStart() {
    stopInstanceProcessTimeStart = System.nanoTime();
  }

  public void stopInstanceProcessTimeEnd() {
    if (stopInstanceProcessTimeStart != null) {
      double endTimeMs = ((double) System.nanoTime() - stopInstanceProcessTimeStart) / 1.0E6D;
      _stopInstanceProcessTime.observe(endTimeMs);
    }
  }

  private Long startInstanceProcessTimeStart;
  public void startInstanceProcessTimeStart() {
    startInstanceProcessTimeStart = System.nanoTime();
  }

  public void startInstanceProcessTimeEnd() {
    if (startInstanceProcessTimeStart != null) {
      double endTimeMs = ((double) System.nanoTime() - startInstanceProcessTimeStart) / 1.0E6D;
      _startInstanceProcessTime.observe(endTimeMs);
    }
  }

  public String getStatsAsString() throws IOException {

    _statNumInstances.set(functionRuntimeManager.getMyInstances());

    StringWriter outputWriter = new StringWriter();

    PrometheusTextFormat.write004(outputWriter, collectorRegistry.metricFamilySamples());

    generateLeaderMetrics(outputWriter);
    return outputWriter.toString();
  }

  private void generateLeaderMetrics(StringWriter stream) {
    if (isLeader.get()) {

      List<Function.FunctionMetaData> metadata = functionMetaDataManager.getAllFunctionMetaData();
      // get total number functions
      long totalFunctions = metadata.size();
      writeMetric(TOTAL_FUNCTIONS_COUNT, totalFunctions, stream);

      // get total expected number of instances
      long totalInstances = 0;
      for (Function.FunctionMetaData entry : metadata) {
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
