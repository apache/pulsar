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

import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Setter;
import org.apache.pulsar.common.stats.MetricsUtil;
import org.apache.pulsar.functions.proto.FunctionMetaData;

/**
 * OpenTelemetry instruments for Function Worker operational metrics.
 *
 * <p>Registers instruments on the Function Worker Meter from {@link PulsarWorkerOpenTelemetry}.
 */
public class OpenTelemetryWorkerStats implements AutoCloseable {

    // Replaces pulsar_function_worker_start_up_time_ms
    public static final String STARTUP_DURATION = "pulsar.function_worker.startup.duration";

    // Replaces pulsar_function_worker_instance_count
    public static final String INSTANCE_COUNT = "pulsar.function_worker.instance.count";

    // Replaces pulsar_function_worker_total_expected_instance_count
    public static final String EXPECTED_INSTANCE_COUNT = "pulsar.function_worker.instance.expected.count";

    // Replaces pulsar_function_worker_total_function_count
    public static final String FUNCTION_COUNT = "pulsar.function_worker.function.count";

    // Replaces pulsar_function_worker_schedule_execution_time_total_ms
    public static final String SCHEDULE_DURATION = "pulsar.function_worker.schedule.duration";

    // Replaces pulsar_function_worker_schedule_strategy_execution_time_ms
    public static final String SCHEDULE_STRATEGY_DURATION = "pulsar.function_worker.schedule.strategy.duration";

    // Replaces pulsar_function_worker_rebalance_execution_time_total_ms
    public static final String REBALANCE_DURATION = "pulsar.function_worker.rebalance.duration";

    // Replaces pulsar_function_worker_rebalance_strategy_execution_time_ms
    public static final String REBALANCE_STRATEGY_DURATION = "pulsar.function_worker.rebalance.strategy.duration";

    // Replaces pulsar_function_worker_stop_instance_process_time_ms
    public static final String INSTANCE_STOP_DURATION = "pulsar.function_worker.instance.stop.duration";

    // Replaces pulsar_function_worker_start_instance_process_time_ms
    public static final String INSTANCE_START_DURATION = "pulsar.function_worker.instance.start.duration";

    // Replaces pulsar_function_worker_drain_execution_time_total_ms
    public static final String DRAIN_DURATION = "pulsar.function_worker.drain.duration";

    // Replaces pulsar_function_worker_is_leader
    public static final String LEADER = "pulsar.function_worker.leader";

    private static final List<Double> DURATION_BUCKETS = Arrays.asList(
            .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0);

    @Setter
    private volatile FunctionRuntimeManager functionRuntimeManager;

    @Setter
    private volatile FunctionMetaDataManager functionMetaDataManager;

    @Setter
    private volatile Supplier<Boolean> isLeader;

    private volatile double startupDurationSeconds = Double.NaN;

    private final DoubleHistogram scheduleDuration;
    private final DoubleHistogram scheduleStrategyDuration;
    private final DoubleHistogram rebalanceDuration;
    private final DoubleHistogram rebalanceStrategyDuration;
    private final DoubleHistogram instanceStopDuration;
    private final DoubleHistogram instanceStartDuration;
    private final DoubleHistogram drainDuration;

    private final ObservableDoubleGauge startupDurationGauge;
    private final ObservableLongGauge instanceCountGauge;
    private final ObservableLongGauge expectedInstanceCountGauge;
    private final ObservableLongGauge functionCountGauge;
    private final ObservableLongGauge leaderGauge;

    public OpenTelemetryWorkerStats(Meter meter) {
        scheduleDuration = durationHistogram(meter, SCHEDULE_DURATION,
                "Total execution time of a schedule cycle.");
        scheduleStrategyDuration = durationHistogram(meter, SCHEDULE_STRATEGY_DURATION,
                "Execution time of the schedule strategy.");
        rebalanceDuration = durationHistogram(meter, REBALANCE_DURATION,
                "Total execution time of a rebalance cycle.");
        rebalanceStrategyDuration = durationHistogram(meter, REBALANCE_STRATEGY_DURATION,
                "Execution time of the rebalance strategy.");
        instanceStopDuration = durationHistogram(meter, INSTANCE_STOP_DURATION,
                "Time taken to stop a function instance.");
        instanceStartDuration = durationHistogram(meter, INSTANCE_START_DURATION,
                "Time taken to start a function instance.");
        drainDuration = durationHistogram(meter, DRAIN_DURATION,
                "Total execution time of a drain cycle.");

        startupDurationGauge = meter.gaugeBuilder(STARTUP_DURATION)
                .setDescription("Worker service startup time.")
                .setUnit("s")
                .buildWithCallback(measurement -> {
                    double duration = startupDurationSeconds;
                    if (!Double.isNaN(duration)) {
                        measurement.record(duration);
                    }
                });

        instanceCountGauge = meter.gaugeBuilder(INSTANCE_COUNT)
                .ofLongs()
                .setDescription("Number of function instances running on this worker.")
                .setUnit("{instance}")
                .buildWithCallback(measurement -> {
                    FunctionRuntimeManager manager = functionRuntimeManager;
                    if (manager != null) {
                        measurement.record(manager.getMyInstances());
                    }
                });

        expectedInstanceCountGauge = meter.gaugeBuilder(EXPECTED_INSTANCE_COUNT)
                .ofLongs()
                .setDescription("Total expected function instance count in the cluster.")
                .setUnit("{instance}")
                .buildWithCallback(measurement -> {
                    if (!isClusterLeader()) {
                        return;
                    }
                    FunctionMetaDataManager manager = functionMetaDataManager;
                    if (manager != null) {
                        measurement.record(expectedInstanceCount(manager));
                    }
                });

        functionCountGauge = meter.gaugeBuilder(FUNCTION_COUNT)
                .ofLongs()
                .setDescription("Total number of functions in the cluster.")
                .setUnit("{function}")
                .buildWithCallback(measurement -> {
                    if (!isClusterLeader()) {
                        return;
                    }
                    FunctionMetaDataManager manager = functionMetaDataManager;
                    if (manager != null) {
                        measurement.record(manager.getAllFunctionMetaData().size());
                    }
                });

        leaderGauge = meter.gaugeBuilder(LEADER)
                .ofLongs()
                .setDescription("Whether this worker is the functions cluster leader.")
                .setUnit("1")
                .buildWithCallback(measurement -> measurement.record(isClusterLeader() ? 1 : 0));
    }

    public void recordStartupDuration(long durationNanos) {
        startupDurationSeconds = MetricsUtil.convertToSeconds(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void recordScheduleDuration(long durationNanos) {
        scheduleDuration.record(toSeconds(durationNanos));
    }

    public void recordScheduleStrategyDuration(long durationNanos) {
        scheduleStrategyDuration.record(toSeconds(durationNanos));
    }

    public void recordRebalanceDuration(long durationNanos) {
        rebalanceDuration.record(toSeconds(durationNanos));
    }

    public void recordRebalanceStrategyDuration(long durationNanos) {
        rebalanceStrategyDuration.record(toSeconds(durationNanos));
    }

    public void recordInstanceStopDuration(long durationNanos) {
        instanceStopDuration.record(toSeconds(durationNanos));
    }

    public void recordInstanceStartDuration(long durationNanos) {
        instanceStartDuration.record(toSeconds(durationNanos));
    }

    public void recordDrainDuration(long durationNanos) {
        drainDuration.record(toSeconds(durationNanos));
    }

    @Override
    public void close() {
        startupDurationGauge.close();
        instanceCountGauge.close();
        expectedInstanceCountGauge.close();
        functionCountGauge.close();
        leaderGauge.close();
    }

    private boolean isClusterLeader() {
        Supplier<Boolean> leader = isLeader;
        return leader != null && Boolean.TRUE.equals(leader.get());
    }

    private static long expectedInstanceCount(FunctionMetaDataManager manager) {
        long total = 0;
        for (FunctionMetaData entry : manager.getAllFunctionMetaData()) {
            total += entry.getFunctionDetails().getParallelism();
        }
        return total;
    }

    private static double toSeconds(long durationNanos) {
        return MetricsUtil.convertToSeconds(durationNanos, TimeUnit.NANOSECONDS);
    }

    private static DoubleHistogram durationHistogram(Meter meter, String name, String description) {
        return meter.histogramBuilder(name)
                .setDescription(description)
                .setUnit("s")
                .setExplicitBucketBoundariesAdvice(DURATION_BUCKETS)
                .build();
    }
}
