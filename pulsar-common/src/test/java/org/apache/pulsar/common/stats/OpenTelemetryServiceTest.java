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
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.internal.state.MetricStorage;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Singular;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OpenTelemetryServiceTest {

    @Builder
    public static final class MetricDataMatcher implements Predicate<MetricData> {
        final String name;
        final MetricDataType type;
        final InstrumentationScopeInfo instrumentationScopeInfo;
        final Resource resource;
        @Singular final List<Attributes> resourceAttributes;
        @Singular final List<Attributes> dataAttributes;
        @Singular final List<Long> longValues;
        @Singular final List<Double> doubleValues;

        @Override
        public boolean test(MetricData md) {
            return (name == null || name.equals(md.getName()))
                && (type == null || type.equals(md.getType()))
                && (instrumentationScopeInfo == null
                    || instrumentationScopeInfo.equals(md.getInstrumentationScopeInfo()))
                && (resource == null || resource.equals(md.getResource()))
                && matchesResourceAttributes(md)
                && (dataAttributes == null || matchesDataAttributes(md))
                && matchesLongValues(md)
                && matchesDoubleValues(md);
        }

        private boolean matchesResourceAttributes(MetricData md) {
            Attributes actual = md.getResource().getAttributes();
            return resourceAttributes.stream().allMatch(expected -> matchesAttributes(actual, expected));
        }

        private boolean matchesDataAttributes(MetricData md) {
            Collection<Attributes> actuals =
                    md.getData().getPoints().stream().map(PointData::getAttributes).collect(Collectors.toSet());
            return dataAttributes.stream().
                    allMatch(expected -> actuals.stream().anyMatch(actual -> matchesAttributes(actual, expected)));
        }

        private boolean matchesAttributes(Attributes actual, Attributes expected) {
            // Return true iff all attribute pairs in expected are a subset of those in actual. Allows tests to specify
            // just the attributes they care about, insted of exhaustively having to list all of them.
            return expected.asMap().entrySet().stream().allMatch(e -> e.getValue().equals(actual.get(e.getKey())));
        }

        private boolean matchesLongValues(MetricData md) {
            Collection<Long> actualData =
                    md.getLongSumData().getPoints().stream().map(LongPointData::getValue).collect(Collectors.toSet());
            return actualData.containsAll(longValues);
        }

        private boolean matchesDoubleValues(MetricData md) {
            Collection<Double> actualData =
                md.getDoubleSumData().getPoints().stream().map(DoublePointData::getValue).collect(Collectors.toSet());
            return actualData.containsAll(doubleValues);
        }
    }

    private OpenTelemetryService openTelemetryService;
    private InMemoryMetricReader reader;
    private Meter meter;

    @BeforeMethod
    public void setup() throws Exception {
        reader = InMemoryMetricReader.create();
        openTelemetryService = OpenTelemetryService.builder().
                clusterName("openTelemetryServiceTestCluster").
                extraMetricReader(reader).
                build();
        meter = openTelemetryService.getMeter("openTelemetryServiceTestInstrument");
    }

    @AfterMethod
    public void teardown() throws Exception {
        openTelemetryService.close();
        reader.close();
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testIsClusterNameRequired() throws Exception {
        @Cleanup
        OpenTelemetryService ots = OpenTelemetryService.builder().build();
    }

    @Test
    public void testIsClusterNameSet() throws Exception {
        @Cleanup
        InMemoryMetricReader reader = InMemoryMetricReader.create();

        @Cleanup
        OpenTelemetryService ots = OpenTelemetryService.builder().
                clusterName("testCluster").
                extraMetricReader(reader).
                build();

        Predicate<MetricData> predicate = MetricDataMatcher.builder().
                resourceAttribute(Attributes.of(AttributeKey.stringKey("pulsar.cluster"), "testCluster")).
                build();

        Collection<MetricData> metricData = reader.collectAllMetrics();
        Assert.assertTrue(metricData.stream().anyMatch(predicate));
    }

    @Test
    public void testIsServiceNameSet() throws Exception {
        @Cleanup
        InMemoryMetricReader reader = InMemoryMetricReader.create();

        @Cleanup
        OpenTelemetryService ots = OpenTelemetryService.builder().
                clusterName("testCluster").
                serviceName("testServiceName").
                extraMetricReader(reader).
                build();

        Predicate<MetricData> predicate = MetricDataMatcher.builder().
                resourceAttribute(Attributes.of(AttributeKey.stringKey("service.name"), "testServiceName")).
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
