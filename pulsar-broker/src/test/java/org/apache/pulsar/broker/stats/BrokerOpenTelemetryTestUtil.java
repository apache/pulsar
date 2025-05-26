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
package org.apache.pulsar.broker.stats;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.pulsar.opentelemetry.OpenTelemetryService;

public class BrokerOpenTelemetryTestUtil {
    // Creates an OpenTelemetrySdkBuilder customizer for use in tests.
    public static Consumer<AutoConfiguredOpenTelemetrySdkBuilder> getOpenTelemetrySdkBuilderConsumer(
            InMemoryMetricReader reader) {
        return sdkBuilder -> {
            sdkBuilder.addMeterProviderCustomizer(
                    (meterProviderBuilder, __) -> meterProviderBuilder.registerMetricReader(reader));
            sdkBuilder.disableShutdownHook();
            disableExporters(sdkBuilder);
            sdkBuilder.addPropertiesSupplier(
                    () -> Map.of(OpenTelemetryService.OTEL_SDK_DISABLED_KEY, "false",
                            "otel.java.enabled.resource.providers", "none"));
        };
    }

    public static void disableExporters(AutoConfiguredOpenTelemetrySdkBuilder sdkBuilder) {
        sdkBuilder.addPropertiesSupplier(() ->
                Map.of("otel.metrics.exporter", "none",
                        "otel.traces.exporter", "none",
                        "otel.logs.exporter", "none"));
    }

    public static void assertMetricDoubleSumValue(Collection<MetricData> metrics, String metricName,
                                                  Attributes attributes, Consumer<Double> valueConsumer) {
        Map<AttributeKey<?>, Object> attributesMap = attributes.asMap();
        assertThat(metrics)
                .anySatisfy(metric -> assertThat(metric)
                        .hasName(metricName)
                        .hasDoubleSumSatisfying(sum -> sum.satisfies(
                                sumData -> assertThat(sumData.getPoints()).anySatisfy(
                                        point -> {
                                            assertThat(point.getAttributes().asMap()).isEqualTo(attributesMap);
                                            valueConsumer.accept(point.getValue());
                                        }))));
    }

    public static void assertMetricLongSumValue(Collection<MetricData> metrics, String metricName,
                                                Attributes attributes, long expected) {
        assertMetricLongSumValue(metrics, metricName, attributes, actual -> assertThat(actual).isEqualTo(expected));
    }

    public static void assertMetricLongSumValue(Collection<MetricData> metrics, String metricName,
                                                Attributes attributes, Consumer<Long> valueConsumer) {
        Map<AttributeKey<?>, Object> attributesMap = attributes.asMap();
        assertThat(metrics)
                .anySatisfy(metric -> assertThat(metric)
                        .hasName(metricName)
                        .hasLongSumSatisfying(sum -> sum.satisfies(
                                sumData -> assertThat(sumData.getPoints()).anySatisfy(
                                        point -> {
                                            assertThat(point.getAttributes().asMap()).isEqualTo(attributesMap);
                                            valueConsumer.accept(point.getValue());
                                        }))));
    }

    public static void assertMetricLongGaugeValue(Collection<MetricData> metrics, String metricName,
                                                  Attributes attributes, long expected) {
        assertMetricLongGaugeValue(metrics, metricName, attributes, actual -> assertThat(actual).isEqualTo(expected));
    }

    public static void assertMetricLongGaugeValue(Collection<MetricData> metrics, String metricName,
                                                  Attributes attributes, Consumer<Long> valueConsumer) {
        Map<AttributeKey<?>, Object> attributesMap = attributes.asMap();
        assertThat(metrics)
                .anySatisfy(metric -> assertThat(metric)
                        .hasName(metricName)
                        .hasLongGaugeSatisfying(gauge -> gauge.satisfies(
                                pointData -> assertThat(pointData.getPoints()).anySatisfy(
                                        point -> {
                                            assertThat(point.getAttributes().asMap()).isEqualTo(attributesMap);
                                            valueConsumer.accept(point.getValue());
                                        }))));
    }

    public static void assertMetricDoubleGaugeValue(Collection<MetricData> metrics, String metricName,
                                                    Attributes attributes, double expected) {
        assertMetricDoubleGaugeValue(metrics, metricName, attributes, actual -> assertThat(actual).isEqualTo(expected));
    }

    public static void assertMetricDoubleGaugeValue(Collection<MetricData> metrics, String metricName,
                                                  Attributes attributes, Consumer<Double> valueConsumer) {
        Map<AttributeKey<?>, Object> attributesMap = attributes.asMap();
        assertThat(metrics)
                .anySatisfy(metric -> assertThat(metric)
                        .hasName(metricName)
                        .hasDoubleGaugeSatisfying(gauge -> gauge.satisfies(
                                pointData -> assertThat(pointData.getPoints()).anySatisfy(
                                        point -> {
                                            assertThat(point.getAttributes().asMap()).isEqualTo(attributesMap);
                                            valueConsumer.accept(point.getValue());
                                        }))));
    }
}
