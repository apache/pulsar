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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.LongCounter;
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
import java.util.function.Predicate;
import lombok.Cleanup;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
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

    @Test(expectedExceptions = IllegalArgumentException.class, enabled = false)
    public void testClusterNameCannotBeNull() throws Exception {
        @Cleanup
        OpenTelemetryService ots = OpenTelemetryService.builder().build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, enabled = false)
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
        Assert.assertTrue(metricData.stream().anyMatch(predicate));
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
        Assert.assertTrue(metricData.stream().anyMatch(predicate));
    }

    @Test
    public void testMetricCardinality() throws Exception {
        LongCounter longCounter = meter.counterBuilder("dummyMetricCardinalityTest").build();

        for (int i = 0; i < OpenTelemetryService.MAX_CARDINALITY_LIMIT; i++) {
            longCounter.add(1, Attributes.of(AttributeKey.stringKey("attribute"), "value" + i));
        }

        Predicate<MetricData> hasOverflowAttribute = MetricDataMatcher.builder().
                        name("dummyMetricCardinalityTest").
                        dataAttribute(MetricStorage.CARDINALITY_OVERFLOW).
                        build();

        Collection<MetricData> metricData = reader.collectAllMetrics();
        Assert.assertTrue(metricData.stream().noneMatch(hasOverflowAttribute));

        for (int i = 0; i < OpenTelemetryService.MAX_CARDINALITY_LIMIT + 1; i++) {
            longCounter.add(1, Attributes.of(AttributeKey.stringKey("attribute"), "value" + i));
        }

        metricData = reader.collectAllMetrics();
        Assert.assertTrue(metricData.stream().anyMatch(hasOverflowAttribute));
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
        Assert.assertTrue(metricData.stream().anyMatch(predicate));
    }

    @Test
    public void testDoubleCounter() throws Exception {
        DoubleCounter doubleCounter = meter.counterBuilder("dummyDoubleCounter").ofDoubles().build();
        doubleCounter.add(3.14, Attributes.of(AttributeKey.stringKey("dummyAttr"), "dummyValue"));
        doubleCounter.add(2.71, Attributes.of(AttributeKey.stringKey("dummyAttr"), "dummyValue"));

        Predicate<MetricData> predicate = MetricDataMatcher.builder().
                name("dummyDoubleCounter").
                dataAttribute(Attributes.of(AttributeKey.stringKey("dummyAttr"), "dummyValue")).
                doubleValue(5.85).
                build();

        Collection<MetricData> metricData = reader.collectAllMetrics();
        Assert.assertTrue(metricData.stream().anyMatch(predicate));
    }
}
