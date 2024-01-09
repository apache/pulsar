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

import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.autoconfigure.spi.metrics.ConfigurableMetricExporterProvider;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class TestMetricProvider implements ConfigurableMetricExporterProvider {

    public static final String METRIC_CONSUMER_CONFIG_KEY = "otel.metric.export.pulsartest.consumer";

    private static final Map<String, Consumer<MetricData>> metricConsumers = new ConcurrentHashMap<>();

    public static String registerMetricConsumer(Consumer<MetricData> consumer) {
        String uuid = UUID.randomUUID().toString();
        metricConsumers.put(uuid, consumer);
        return uuid;
    }

    @Override
    public MetricExporter createExporter(ConfigProperties config) {
        final String consumerKey = config.getString(METRIC_CONSUMER_CONFIG_KEY);
        final Consumer<MetricData> consumer = consumerKey != null ? metricConsumers.get(consumerKey) : null;
        return new MetricExporter() {
            @Override
            public CompletableResultCode export(Collection<MetricData> metrics) {
                if (consumer != null) {
                    metrics.forEach(consumer);
                }
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public CompletableResultCode flush() {
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public CompletableResultCode shutdown() {
                metricConsumers.remove(consumerKey);
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
                return AggregationTemporality.CUMULATIVE;
            }
        };
    }

    @Override
    public String getName() {
        return "test";
    }
}
