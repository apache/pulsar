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
package org.apache.pulsar.common.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.internal.SdkMeterProviderUtil;
import io.opentelemetry.sdk.metrics.internal.state.MetricStorage;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import lombok.Cleanup;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient;
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
        openTelemetryService = OpenTelemetryService.builder().
                sdkBuilder(getSdkBuilder(reader)).
                clusterName("openTelemetryServiceTestCluster").
                extraProperty(OpenTelemetryService.OTEL_SDK_DISABLED, "false").
                build();
        meter = openTelemetryService.getMeter("openTelemetryServiceTestInstrument");
    }

    @AfterMethod
    public void teardown() throws Exception {
        openTelemetryService.close();
        reader.close();
    }

    // Overrides the default sdkBuilder to include the InMemoryMetricReader for testing purposes.
    private static AutoConfiguredOpenTelemetrySdkBuilder getSdkBuilder(MetricReader extraReader) {
        return AutoConfiguredOpenTelemetrySdk.builder().
                addMeterProviderCustomizer((sdkMeterProviderBuilder, configProperties) -> {
                    SdkMeterProviderUtil.registerMetricReaderWithCardinalitySelector(
                            sdkMeterProviderBuilder, extraReader,
                            // Override the max cardinality limit for this extra reader.
                            instrumentType -> OpenTelemetryService.MAX_CARDINALITY_LIMIT + 1);
                    return sdkMeterProviderBuilder;
                });
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClusterNameCannotBeNull() throws Exception {
        @Cleanup
        OpenTelemetryService ots = OpenTelemetryService.builder().build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClusterNameCannotBeEmpty() throws Exception {
        @Cleanup
        OpenTelemetryService ots = OpenTelemetryService.builder().clusterName(StringUtils.EMPTY).build();
    }

    @Test
    public void testIsClusterNameSet() throws Exception {
        @Cleanup
        InMemoryMetricReader reader = InMemoryMetricReader.create();

        @Cleanup
        OpenTelemetryService ots = OpenTelemetryService.builder().
                sdkBuilder(getSdkBuilder(reader)).
                clusterName("testCluster").
                extraProperty(OpenTelemetryService.OTEL_SDK_DISABLED, "false").
                build();

        Predicate<MetricData> predicate = MetricDataMatcher.builder().
                resourceAttribute(Attributes.of(AttributeKey.stringKey("pulsar.cluster"), "testCluster")).
                build();

        Collection<MetricData> metricData = reader.collectAllMetrics();
        assertTrue(metricData.stream().anyMatch(predicate));
    }

    @Test
    public void testIsInstrumentationNameSetOnMeter() throws Exception {
        Meter meter = openTelemetryService.getMeter("testInstrumentationScope");
        meter.counterBuilder("dummyCounter").build().add(1);
        MetricDataMatcher predicate = MetricDataMatcher.builder().
                name("dummyCounter").
                instrumentationScopeInfo(InstrumentationScopeInfo.create("testInstrumentationScope")).
                build();
        Collection<MetricData> metricData = reader.collectAllMetrics();
        assertTrue(metricData.stream().anyMatch(predicate));
    }

    @Test
    public void testMetricCardinalityIsSet() throws Exception {
        var prometheusExporterPort = 9464;
        @Cleanup
        var ots = OpenTelemetryService.builder().
                clusterName("openTelemetryServiceCardinalityTestCluster").
                extraProperty(OpenTelemetryService.OTEL_SDK_DISABLED, "false").
                extraProperty("otel.metrics.exporter", "prometheus").
                extraProperty("otel.exporter.prometheus.port", Integer.toString(prometheusExporterPort)).
                extraProperty("otel.metric.export.interval", "100").
                build();
        var meter = ots.getMeter("openTelemetryMetricCardinalityTest");
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
    public void testLongCounter() throws Exception {
        LongCounter longCounter = meter.counterBuilder("dummyLongCounter").build();
        longCounter.add(1, Attributes.of(AttributeKey.stringKey("dummyAttr"), "dummyValue"));
        longCounter.add(2, Attributes.of(AttributeKey.stringKey("dummyAttr"), "dummyValue"));

        Predicate<MetricData> predicate = MetricDataMatcher.builder().
                name("dummyLongCounter").
                dataAttribute(Attributes.of(AttributeKey.stringKey("dummyAttr"), "dummyValue")).
                type(MetricDataType.LONG_SUM).
                longValue(3L).
                build();

        Collection<MetricData> metricData = reader.collectAllMetrics();
        assertTrue(metricData.stream().anyMatch(predicate));
    }

    @Test
    public void testServiceIsDisabledByDefault() throws Exception {
        @Cleanup
        var metricReader = InMemoryMetricReader.create();

        @Cleanup
        var ots = OpenTelemetryService.builder().
                sdkBuilder(getSdkBuilder(metricReader)).
                clusterName("openTelemetryServiceTestCluster").
                build();
        var meter = ots.getMeter("openTelemetryServiceTestInstrument");

        var builders = List.of(
                meter.counterBuilder("dummyCounterA"),
                meter.counterBuilder("dummyCounterB").setDescription("desc"),
                meter.counterBuilder("dummyCounterC").setDescription("desc").setUnit("unit"),
                meter.counterBuilder("dummyCounterD").setUnit("unit")
        );

        var callbackCount = new AtomicInteger();
        // Validate that no matter how the counters are being built, they are all backed by the same underlying object.
        // This ensures we conserve memory when the SDK is disabled.
        assertEquals(builders.stream().map(LongCounterBuilder::build).distinct().count(), 1);
        assertEquals(builders.stream().map(LongCounterBuilder::buildObserver).distinct().count(), 1);
        assertEquals(builders.stream().map(b -> b.buildWithCallback(__ -> callbackCount.incrementAndGet()))
                .distinct().count(), 1);

        // Validate that no metrics are being emitted at all.
        assertTrue(metricReader.collectAllMetrics().isEmpty());

        // Validate that the callback has not being called.
        assertEquals(callbackCount.get(), 0);
    }
}
