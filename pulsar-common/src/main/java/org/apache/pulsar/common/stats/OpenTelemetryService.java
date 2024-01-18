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

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.internal.SdkMeterProviderUtil;
import io.opentelemetry.sdk.metrics.internal.export.CardinalityLimitSelector;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.resources.ResourceBuilder;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Singular;

public class OpenTelemetryService implements Closeable {

    private static final AttributeKey<String> CLUSTER_NAME_ATTRIBUTE = AttributeKey.stringKey("pulsar.cluster");
    private static final AttributeKey<String> SERVICE_NAME_ATTRIBUTE = AttributeKey.stringKey("service.name");

    private static final String MAX_CARDINALITY_LIMIT_KEY = "otel.experimental.metrics.cardinality.limit";
    public static final int MAX_CARDINALITY_LIMIT = 10000;

    private final OpenTelemetrySdk openTelemetrySdk;

    @lombok.Builder
    public OpenTelemetryService(
            String clusterName,
            String serviceName,
            @Singular Map<String, String> extraProperties,
            @VisibleForTesting @Singular List<MetricReader> extraMetricReaders) {
        Objects.requireNonNull(clusterName);
        AutoConfiguredOpenTelemetrySdkBuilder builder = AutoConfiguredOpenTelemetrySdk.builder();
        builder.addPropertiesSupplier(
                () -> Collections.singletonMap(MAX_CARDINALITY_LIMIT_KEY, Integer.toString(MAX_CARDINALITY_LIMIT + 1)));
        builder.addPropertiesSupplier(() -> extraProperties);
        builder.addResourceCustomizer(
                (resource, __) -> {
                    ResourceBuilder resourceBuilder = Resource.builder();
                    resourceBuilder.put(CLUSTER_NAME_ATTRIBUTE, clusterName);
                    if (serviceName != null) {
                        resourceBuilder.put(SERVICE_NAME_ATTRIBUTE, serviceName);
                    }
                    return resource.merge(resourceBuilder.build());
                });
        final CardinalityLimitSelector cardinalityLimitSelector = __ -> MAX_CARDINALITY_LIMIT + 1;
        extraMetricReaders.forEach(metricReader -> builder.addMeterProviderCustomizer((sdkMeterProviderBuilder, __) -> {
            SdkMeterProviderUtil.registerMetricReaderWithCardinalitySelector(
                    sdkMeterProviderBuilder, metricReader, cardinalityLimitSelector);
            return sdkMeterProviderBuilder;
        }));
        openTelemetrySdk = builder.build().getOpenTelemetrySdk();
    }

    public Meter getMeter(String instrumentationScopeName) {
        return openTelemetrySdk.getMeter(instrumentationScopeName);
    }

    @Override
    public void close() {
        openTelemetrySdk.close();
    }
}
