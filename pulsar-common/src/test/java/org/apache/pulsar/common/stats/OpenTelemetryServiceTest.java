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
import java.util.List;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Singular;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class OpenTelemetryServiceTest {

    @Builder
    public static final class MetricDataArgumentMatcher implements ArgumentMatcher<MetricData> {
        final String name;
        final MetricDataType type;
        final Attributes attributes;
        @Singular final List<LongPointData> longs;

        @Override
        public boolean matches(MetricData md) {
            return (type == null || type.equals(md.getType()))
                    && (name == null || name.equals(md.getName()))
                    && matchesAttributes(md)
                    && matchesLongPointData(md);
        }

        private boolean matchesAttributes(MetricData md) {
            return attributes == null
                    || md.getData().getPoints().stream().map(PointData::getAttributes).anyMatch(attributes::equals);
        }

        private boolean matchesLongPointData(MetricData md) {
            return longs == null
                    || longs.stream().mapToLong(LongPointData::getValue).allMatch(
                    value -> md.getLongSumData().getPoints().stream().mapToLong(LongPointData::getValue).anyMatch(
                            valueA -> value == valueA));
        }
    }

    @Test
    public void testMetricExport() throws Exception {
        Consumer<MetricData> consumer = Mockito.mock(Consumer.class);
        String consumerUuid = TestMetricProvider.registerMetricConsumer(consumer);

        @Cleanup
        OpenTelemetryService ots = OpenTelemetryService.builder().
                clusterName("clusterName").
                extraProperty("otel.metric.export.interval", "100").
                extraProperty("otel.metrics.exporter", TestMetricProvider.METRIC_PROVIDER_NAME).
                extraProperty(TestMetricProvider.METRIC_CONSUMER_CONFIG_KEY, consumerUuid).
                build();

        Meter meter = ots.getMeter("pulsar.test");
        LongCounter longCounter = meter.counterBuilder("counter.A").build();
        longCounter.add(1);

        ArgumentMatcher<MetricData> matcher =
                MetricDataArgumentMatcher.builder().name("counter.A").type(MetricDataType.LONG_SUM).build();

        Mockito.verify(consumer, Mockito.timeout(1000).atLeastOnce()).accept(Mockito.argThat(matcher));
    }

    @Test
    public void testMetricCardinality() throws Exception {
        Consumer<MetricData> consumer = Mockito.mock(Consumer.class);
        String consumerUuid = TestMetricProvider.registerMetricConsumer(consumer);

        @Cleanup
        OpenTelemetryService ots = OpenTelemetryService.builder().
                clusterName("clusterName").
                extraProperty("otel.metric.export.interval", "100").
                extraProperty("otel.metrics.exporter", TestMetricProvider.METRIC_PROVIDER_NAME).
                extraProperty(TestMetricProvider.METRIC_CONSUMER_CONFIG_KEY, consumerUuid).
                build();

        Meter meter = ots.getMeter("pulsar.testMetricCardinality");
        LongCounter longCounter = meter.counterBuilder("count").build();
        ArgumentMatcher<MetricData> matcher =
                MetricDataArgumentMatcher.builder().name("count").attributes(MetricStorage.CARDINALITY_OVERFLOW).build();

        for (int i = 0; i < OpenTelemetryService.MAX_CARDINALITY_LIMIT; i++) {
            longCounter.add(1, Attributes.of(AttributeKey.stringKey("attribute"), "value" + i));
        }
        Mockito.verify(consumer, Mockito.never()).accept(Mockito.argThat(matcher));

        for (int i = 0; i < OpenTelemetryService.MAX_CARDINALITY_LIMIT + 1; i++) {
            longCounter.add(1, Attributes.of(AttributeKey.stringKey("attribute"), "value" + i));
        }
        Mockito.verify(consumer, Mockito.timeout(1000).atLeastOnce()).accept(Mockito.argThat(matcher));
    }
}
