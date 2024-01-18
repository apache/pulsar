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
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.internal.state.MetricStorage;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Singular;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OpenTelemetryServiceTest {

    @Builder
    public static final class MetricDataMatchPredicate implements Predicate<MetricData> {
        final String name;
        final MetricDataType type;
        final Attributes attributes;
        @Singular final List<Long> longValues;

        @Override
        public boolean test(MetricData md) {
            return (type == null || type.equals(md.getType()))
                    && (name == null || name.equals(md.getName()))
                    && matchesAttributes(md)
                    && matchesLongValues(md);
        }

        private boolean matchesAttributes(MetricData md) {
            return attributes == null
                    || md.getData().getPoints().stream().map(PointData::getAttributes).anyMatch(attributes::equals);
        }

        private boolean matchesLongValues(MetricData md) {
            return longValues == null || longValues.stream().allMatch(
                    value -> md.getLongSumData().getPoints().stream().mapToLong(LongPointData::getValue).anyMatch(
                            valueA -> value == valueA));
        }
    }

    @Test
    public void testInMemoryReader() throws Exception {
        @Cleanup
        InMemoryMetricReader reader = InMemoryMetricReader.create();

        @Cleanup
        OpenTelemetryService ots =
                OpenTelemetryService.builder().clusterName("clusterName").extraMetricReader(reader).build();

        Meter meter = ots.getMeter("pulsar.test");
        LongCounter longCounter = meter.counterBuilder("counter.inMemory").build();
        longCounter.add(1);

        Predicate<MetricData> predicate = MetricDataMatchPredicate.builder().
                name("counter.inMemory").
                type(MetricDataType.LONG_SUM).
                longValue(1L).
                build();

        Collection<MetricData> metricData = reader.collectAllMetrics();
        Assert.assertTrue(metricData.stream().anyMatch(predicate));
    }

    @Test
    public void testMetricCardinality() throws Exception {
        @Cleanup
        InMemoryMetricReader reader = InMemoryMetricReader.create();

        @Cleanup
        OpenTelemetryService ots = OpenTelemetryService.builder().
                clusterName("clusterName").
                extraMetricReader(reader).
                build();

        Meter meter = ots.getMeter("pulsar.testMetricCardinality");
        LongCounter longCounter = meter.counterBuilder("count").build();

        for (int i = 0; i < OpenTelemetryService.MAX_CARDINALITY_LIMIT; i++) {
            longCounter.add(1, Attributes.of(AttributeKey.stringKey("attribute"), "value" + i));
        }

        Predicate<MetricData> hasOverflowAttribute = MetricDataMatchPredicate.builder().
                        name("count").
                        attributes(MetricStorage.CARDINALITY_OVERFLOW).
                        build();

        Collection<MetricData> metricData = reader.collectAllMetrics();
        Assert.assertTrue(metricData.stream().noneMatch(hasOverflowAttribute));

        for (int i = 0; i < OpenTelemetryService.MAX_CARDINALITY_LIMIT + 1; i++) {
            longCounter.add(1, Attributes.of(AttributeKey.stringKey("attribute"), "value" + i));
        }

        metricData = reader.collectAllMetrics();
        Assert.assertTrue(metricData.stream().anyMatch(hasOverflowAttribute));
    }
}
