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

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.functions.proto.FunctionMetaData;
import org.apache.pulsar.opentelemetry.OpenTelemetryService;
import org.assertj.core.api.Assertions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OpenTelemetryWorkerStatsTest {

    private static final long FIVE_MILLIS_NANOS = TimeUnit.MILLISECONDS.toNanos(5);

    private OpenTelemetryService openTelemetryService;
    private InMemoryMetricReader reader;
    private OpenTelemetryWorkerStats stats;

    @BeforeMethod
    public void setup() {
        reader = InMemoryMetricReader.create();
        openTelemetryService = OpenTelemetryService.builder()
                .clusterName("test-cluster")
                .serviceName(PulsarWorkerOpenTelemetry.SERVICE_NAME)
                .builderCustomizer(builder -> builder
                        .disableShutdownHook()
                        .addMeterProviderCustomizer((providerBuilder, config) ->
                                providerBuilder.registerMetricReader(reader))
                        .addPropertiesSupplier(() -> Map.of(
                                OpenTelemetryService.OTEL_SDK_DISABLED_KEY, "false",
                                "otel.metrics.exporter", "none",
                                "otel.traces.exporter", "none",
                                "otel.logs.exporter", "none")))
                .build();
        var meter = openTelemetryService.getOpenTelemetry()
                .getMeter(PulsarWorkerOpenTelemetry.INSTRUMENTATION_SCOPE_NAME);
        stats = new OpenTelemetryWorkerStats(meter);
    }

    @AfterMethod
    public void teardown() throws Exception {
        stats.close();
        openTelemetryService.close();
        reader.close();
    }

    @Test
    public void testDurationHistograms() {
        stats.recordScheduleDuration(FIVE_MILLIS_NANOS);
        stats.recordScheduleStrategyDuration(FIVE_MILLIS_NANOS);
        stats.recordRebalanceDuration(FIVE_MILLIS_NANOS);
        stats.recordRebalanceStrategyDuration(FIVE_MILLIS_NANOS);
        stats.recordInstanceStartDuration(FIVE_MILLIS_NANOS);
        stats.recordInstanceStopDuration(FIVE_MILLIS_NANOS);
        stats.recordDrainDuration(FIVE_MILLIS_NANOS);

        var metrics = reader.collectAllMetrics();
        assertHistogramCount(metrics, OpenTelemetryWorkerStats.SCHEDULE_DURATION);
        assertHistogramCount(metrics, OpenTelemetryWorkerStats.SCHEDULE_STRATEGY_DURATION);
        assertHistogramCount(metrics, OpenTelemetryWorkerStats.REBALANCE_DURATION);
        assertHistogramCount(metrics, OpenTelemetryWorkerStats.REBALANCE_STRATEGY_DURATION);
        assertHistogramCount(metrics, OpenTelemetryWorkerStats.INSTANCE_START_DURATION);
        assertHistogramCount(metrics, OpenTelemetryWorkerStats.INSTANCE_STOP_DURATION);
        assertHistogramCount(metrics, OpenTelemetryWorkerStats.DRAIN_DURATION);
    }

    @Test
    public void testGaugesWhenLeader() {
        FunctionRuntimeManager runtimeManager = mock(FunctionRuntimeManager.class);
        when(runtimeManager.getMyInstances()).thenReturn(4);

        FunctionMetaData function1 = new FunctionMetaData();
        function1.setFunctionDetails().setName("fn-1").setParallelism(2);
        FunctionMetaData function2 = new FunctionMetaData();
        function2.setFunctionDetails().setName("fn-2").setParallelism(3);
        FunctionMetaDataManager metaDataManager = mock(FunctionMetaDataManager.class);
        when(metaDataManager.getAllFunctionMetaData()).thenReturn(List.of(function1, function2));

        stats.setFunctionRuntimeManager(runtimeManager);
        stats.setFunctionMetaDataManager(metaDataManager);
        stats.setIsLeader(() -> true);
        stats.recordStartupDuration(FIVE_MILLIS_NANOS);

        var metrics = reader.collectAllMetrics();
        assertLongGauge(metrics, OpenTelemetryWorkerStats.INSTANCE_COUNT, 4);
        assertLongGauge(metrics, OpenTelemetryWorkerStats.FUNCTION_COUNT, 2);
        assertLongGauge(metrics, OpenTelemetryWorkerStats.EXPECTED_INSTANCE_COUNT, 5);
        assertLongGauge(metrics, OpenTelemetryWorkerStats.LEADER, 1);
        assertThat(metrics).anySatisfy(metric -> assertThat(metric)
                .hasName(OpenTelemetryWorkerStats.STARTUP_DURATION)
                .hasDoubleGaugeSatisfying(gauge ->
                        gauge.hasPointsSatisfying(point -> point.hasValue(0.005))));
    }

    @Test
    public void testGaugesWhenNotLeader() {
        FunctionRuntimeManager runtimeManager = mock(FunctionRuntimeManager.class);
        when(runtimeManager.getMyInstances()).thenReturn(1);
        FunctionMetaData function = new FunctionMetaData();
        function.setFunctionDetails().setName("fn-1").setParallelism(1);
        FunctionMetaDataManager metaDataManager = mock(FunctionMetaDataManager.class);
        when(metaDataManager.getAllFunctionMetaData()).thenReturn(List.of(function));
        stats.setFunctionRuntimeManager(runtimeManager);
        stats.setFunctionMetaDataManager(metaDataManager);
        stats.setIsLeader(() -> false);

        var metrics = reader.collectAllMetrics();
        assertLongGauge(metrics, OpenTelemetryWorkerStats.INSTANCE_COUNT, 1);
        assertLongGauge(metrics, OpenTelemetryWorkerStats.LEADER, 0);
        Assertions.assertThat(metrics)
                .noneMatch(metric -> OpenTelemetryWorkerStats.FUNCTION_COUNT.equals(metric.getName()))
                .noneMatch(metric -> OpenTelemetryWorkerStats.EXPECTED_INSTANCE_COUNT.equals(metric.getName()));
    }

    @Test
    public void testWorkerStatsManagerDualPublish() throws Exception {
        var localReader = InMemoryMetricReader.create();
        var localOpenTelemetryService = OpenTelemetryService.builder()
                .clusterName("test-cluster")
                .serviceName(PulsarWorkerOpenTelemetry.SERVICE_NAME)
                .builderCustomizer(builder -> builder
                        .disableShutdownHook()
                        .addMeterProviderCustomizer((providerBuilder, config) ->
                                providerBuilder.registerMetricReader(localReader))
                        .addPropertiesSupplier(() -> Map.of(
                                OpenTelemetryService.OTEL_SDK_DISABLED_KEY, "false",
                                "otel.metrics.exporter", "none",
                                "otel.traces.exporter", "none",
                                "otel.logs.exporter", "none")))
                .build();
        WorkerConfig workerConfig = new WorkerConfig().setPulsarFunctionsCluster("test-cluster");
        var meter = localOpenTelemetryService.getOpenTelemetry()
                .getMeter(PulsarWorkerOpenTelemetry.INSTRUMENTATION_SCOPE_NAME);
        WorkerStatsManager workerStatsManager = new WorkerStatsManager(workerConfig, false, meter);

        FunctionRuntimeManager runtimeManager = mock(FunctionRuntimeManager.class);
        when(runtimeManager.getMyInstances()).thenReturn(2);
        workerStatsManager.setFunctionRuntimeManager(runtimeManager);
        workerStatsManager.setIsLeader(new AtomicBoolean(true)::get);

        workerStatsManager.startupTimeStart();
        workerStatsManager.startupTimeEnd();
        workerStatsManager.scheduleTotalExecTimeStart();
        workerStatsManager.scheduleTotalExecTimeEnd();
        workerStatsManager.scheduleStrategyExecTimeStartStart();
        workerStatsManager.scheduleStrategyExecTimeStartEnd();
        workerStatsManager.rebalanceTotalExecTimeStart();
        workerStatsManager.rebalanceTotalExecTimeEnd();
        workerStatsManager.rebalanceStrategyExecTimeStart();
        workerStatsManager.rebalanceStrategyExecTimeEnd();
        workerStatsManager.startInstanceProcessTimeStart();
        workerStatsManager.startInstanceProcessTimeEnd();
        workerStatsManager.stopInstanceProcessTimeStart();
        workerStatsManager.stopInstanceProcessTimeEnd();
        workerStatsManager.drainTotalExecTimeStart();
        workerStatsManager.drainTotalExecTimeEnd();

        try {
            var metrics = localReader.collectAllMetrics();
            assertThat(metrics).anySatisfy(metric ->
                    assertThat(metric).hasName(OpenTelemetryWorkerStats.STARTUP_DURATION));
            assertHistogramCount(metrics, OpenTelemetryWorkerStats.SCHEDULE_DURATION);
            assertHistogramCount(metrics, OpenTelemetryWorkerStats.SCHEDULE_STRATEGY_DURATION);
            assertHistogramCount(metrics, OpenTelemetryWorkerStats.REBALANCE_DURATION);
            assertHistogramCount(metrics, OpenTelemetryWorkerStats.REBALANCE_STRATEGY_DURATION);
            assertHistogramCount(metrics, OpenTelemetryWorkerStats.INSTANCE_START_DURATION);
            assertHistogramCount(metrics, OpenTelemetryWorkerStats.INSTANCE_STOP_DURATION);
            assertHistogramCount(metrics, OpenTelemetryWorkerStats.DRAIN_DURATION);
            assertLongGauge(metrics, OpenTelemetryWorkerStats.INSTANCE_COUNT, 2);
            assertLongGauge(metrics, OpenTelemetryWorkerStats.LEADER, 1);
        } finally {
            workerStatsManager.close();
            localOpenTelemetryService.close();
            localReader.close();
        }
    }

    private static void assertHistogramCount(Collection<MetricData> metrics, String name) {
        assertThat(metrics).anySatisfy(metric -> assertThat(metric)
                .hasName(name)
                .hasHistogramSatisfying(histogram ->
                        histogram.hasPointsSatisfying(point -> point.hasCount(1))));
    }

    private static void assertLongGauge(Collection<MetricData> metrics, String name, long value) {
        assertThat(metrics).anySatisfy(metric -> assertThat(metric)
                .hasName(name)
                .hasLongGaugeSatisfying(gauge ->
                        gauge.hasPointsSatisfying(point -> point.hasValue(value))));
    }
}
