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
package org.apache.pulsar.opentelemetry;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.satisfies;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.instrumentation.resources.JarServiceNameDetector;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.semconv.ResourceAttributes;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient;
import org.assertj.core.api.AbstractCharSequenceAssert;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OpenTelemetryServiceTest {

    private OpenTelemetryService openTelemetryService;
    private InMemoryMetricReader reader;
    private Meter meter;

    @BeforeMethod
    public void setup() throws Exception {
        reader = InMemoryMetricReader.create();
        openTelemetryService = OpenTelemetryService.builder()
                .builderCustomizer(
                        getBuilderCustomizer(reader, Map.of(OpenTelemetryService.OTEL_SDK_DISABLED_KEY, "false")))
                .clusterName("openTelemetryServiceTestCluster")
                .build();
        meter = openTelemetryService.getOpenTelemetry().getMeter("openTelemetryServiceTestInstrument");
    }

    @AfterMethod
    public void teardown() throws Exception {
        openTelemetryService.close();
        reader.close();
    }

    // Customizes the SDK builder to include the MetricReader and extra properties for testing purposes.
    private static Consumer<AutoConfiguredOpenTelemetrySdkBuilder> getBuilderCustomizer(MetricReader extraReader,
                                                                                Map<String, String> extraProperties) {
        return autoConfigurationCustomizer -> {
            if (extraReader != null) {
                autoConfigurationCustomizer.addMeterProviderCustomizer(
                        (sdkMeterProviderBuilder, __) -> sdkMeterProviderBuilder.registerMetricReader(extraReader));
            }
            autoConfigurationCustomizer.disableShutdownHook();
            // disable all autoconfigured exporters
            autoConfigurationCustomizer.addPropertiesSupplier(() ->
                    Map.of("otel.metrics.exporter", "none",
                            "otel.traces.exporter", "none",
                            "otel.logs.exporter", "none"));
            autoConfigurationCustomizer.addPropertiesSupplier(() -> extraProperties);
        };
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClusterNameCannotBeNull() {
        @Cleanup
        var ots = OpenTelemetryService.builder().build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClusterNameCannotBeEmpty() {
        @Cleanup
        var ots = OpenTelemetryService.builder().clusterName(StringUtils.EMPTY).build();
    }

    @Test
    public void testResourceAttributesAreSet() throws Exception {
        @Cleanup
        var reader = InMemoryMetricReader.create();

        @Cleanup
        var ots = OpenTelemetryService.builder()
                .builderCustomizer(getBuilderCustomizer(reader,
                        Map.of(OpenTelemetryService.OTEL_SDK_DISABLED_KEY, "false",
                               "otel.java.disabled.resource.providers", JarServiceNameDetector.class.getName())))
                .clusterName("testServiceNameAndVersion")
                .serviceName("openTelemetryServiceTestService")
                .serviceVersion("1.0.0")
                .build();

        assertThat(reader.collectAllMetrics())
            .allSatisfy(metric -> assertThat(metric)
                .hasResourceSatisfying(resource -> resource
                    .hasAttribute(OpenTelemetryAttributes.PULSAR_CLUSTER, "testServiceNameAndVersion")
                    .hasAttribute(ResourceAttributes.SERVICE_NAME, "openTelemetryServiceTestService")
                    .hasAttribute(ResourceAttributes.SERVICE_VERSION, "1.0.0")
                    .hasAttribute(satisfies(ResourceAttributes.HOST_NAME, AbstractCharSequenceAssert::isNotBlank))));
    }

    @Test
    public void testIsInstrumentationNameSetOnMeter() {
        var meter = openTelemetryService.getOpenTelemetry().getMeter("testInstrumentationScope");
        meter.counterBuilder("dummyCounter").build().add(1);
        assertThat(reader.collectAllMetrics())
            .anySatisfy(metricData -> assertThat(metricData)
                .hasInstrumentationScope(InstrumentationScopeInfo.create("testInstrumentationScope")));
    }

    @Test
    public void testMetricCardinalityIsSet() {
        var prometheusExporterPort = 9464;
        @Cleanup
        var ots = OpenTelemetryService.builder()
                .builderCustomizer(getBuilderCustomizer(null,
                        Map.of(OpenTelemetryService.OTEL_SDK_DISABLED_KEY, "false",
                        "otel.metrics.exporter", "prometheus",
                        "otel.exporter.prometheus.port", Integer.toString(prometheusExporterPort))))
                .clusterName("openTelemetryServiceCardinalityTestCluster")
                .build();
        var meter = ots.getOpenTelemetry().getMeter("openTelemetryMetricCardinalityTest");
        var counter = meter.counterBuilder("dummyCounter").build();
        for (int i = 0; i < OpenTelemetryService.MAX_CARDINALITY_LIMIT + 100; i++) {
            counter.add(1, Attributes.of(AttributeKey.stringKey("attribute"), "value" + i));
        }

        Awaitility.waitAtMost(30, TimeUnit.SECONDS).ignoreExceptions().until(() -> {
            var client = new PrometheusMetricsClient("localhost", prometheusExporterPort);
            var allMetrics = client.getMetrics();
            var actualMetrics = allMetrics.findByNameAndLabels("dummyCounter_total");
            var overflowMetric = allMetrics.findByNameAndLabels("dummyCounter_total", "otel_metric_overflow", "true");
            return actualMetrics.size() == OpenTelemetryService.MAX_CARDINALITY_LIMIT + 1 && overflowMetric.size() == 1;
        });
    }

    @Test
    public void testLongCounter() {
        var longCounter = meter.counterBuilder("dummyLongCounter").build();
        var attributes = Attributes.of(AttributeKey.stringKey("dummyAttr"), "dummyValue");
        longCounter.add(1, attributes);
        longCounter.add(2, attributes);

        assertThat(reader.collectAllMetrics())
            .anySatisfy(metric -> assertThat(metric)
                .hasName("dummyLongCounter")
                .hasLongSumSatisfying(sum -> sum
                    .hasPointsSatisfying(point -> point
                        .hasAttributes(attributes)
                        .hasValue(3))));
    }

    @Test
    public void testServiceIsDisabledByDefault() throws Exception {
        @Cleanup
        var metricReader = InMemoryMetricReader.create();

        @Cleanup
        var ots = OpenTelemetryService.builder()
                .builderCustomizer(getBuilderCustomizer(metricReader, Map.of()))
                .clusterName("openTelemetryServiceTestCluster")
                .build();
        var meter = ots.getOpenTelemetry().getMeter("openTelemetryServiceTestInstrument");

        var builders = List.of(
                meter.counterBuilder("dummyCounterA"),
                meter.counterBuilder("dummyCounterB").setDescription("desc"),
                meter.counterBuilder("dummyCounterC").setDescription("desc").setUnit("unit"),
                meter.counterBuilder("dummyCounterD").setUnit("unit")
        );

        var callback = new AtomicBoolean();
        // Validate that no matter how the counters are being built, they are all backed by the same underlying object.
        // This ensures we conserve memory when the SDK is disabled.
        assertThat(builders.stream().map(LongCounterBuilder::build).distinct()).hasSize(1);
        assertThat(builders.stream().map(LongCounterBuilder::buildObserver).distinct()).hasSize(1);
        assertThat(builders.stream().map(b -> b.buildWithCallback(__ -> callback.set(true))).distinct()).hasSize(1);

        // Validate that no metrics are being emitted at all.
        assertThat(metricReader.collectAllMetrics()).isEmpty();

        // Validate that the callback has not being called.
        assertThat(callback).isFalse();
    }

    @Test
    public void testJvmRuntimeMetrics() {
        // Attempt collection of GC metrics. The metrics should be populated regardless if GC is triggered or not.
        Runtime.getRuntime().gc();

        var metrics = reader.collectAllMetrics();

        // Process Metrics
        // Replaces process_cpu_seconds_total
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.cpu.time"));

        // Memory Metrics
        // Replaces jvm_memory_bytes_used
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.memory.used"));
        // Replaces jvm_memory_bytes_committed
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.memory.committed"));
        // Replaces jvm_memory_bytes_max
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.memory.limit"));
        // Replaces jvm_memory_bytes_init
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.memory.init"));
        // Replaces jvm_memory_pool_allocated_bytes_total
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.memory.used_after_last_gc"));

        // Buffer Pool Metrics
        // Replaces jvm_buffer_pool_used_bytes
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.buffer.memory.usage"));
        // Replaces jvm_buffer_pool_capacity_bytes
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.buffer.memory.limit"));
        // Replaces jvm_buffer_pool_used_buffers
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.buffer.count"));

        // Garbage Collector Metrics
        // Replaces jvm_gc_collection_seconds
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.gc.duration"));

        // Thread Metrics
        // Replaces jvm_threads_state, jvm_threads_current and jvm_threads_daemon
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.thread.count"));

        // Class Loading Metrics
        // Replaces jvm_classes_currently_loaded
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.class.count"));
        // Replaces jvm_classes_loaded_total
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.class.loaded"));
        // Replaces jvm_classes_unloaded_total
        assertThat(metrics).anySatisfy(metric -> assertThat(metric).hasName("jvm.class.unloaded"));
    }
}
