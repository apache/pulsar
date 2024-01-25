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

import static com.google.common.base.Preconditions.checkArgument;
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
import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Singular;
import org.apache.commons.lang3.StringUtils;

public class OpenTelemetryService implements Closeable {

    private static final AttributeKey<String> CLUSTER_NAME_ATTRIBUTE = AttributeKey.stringKey("pulsar.cluster");

    public static final String OTEL_SDK_DISABLED = "otel.sdk.disabled";
    private static final String MAX_CARDINALITY_LIMIT_KEY = "otel.experimental.metrics.cardinality.limit";

    public static final int MAX_CARDINALITY_LIMIT = 10000;

    private final OpenTelemetrySdk openTelemetrySdk;

    @lombok.Builder
    public OpenTelemetryService(
            String clusterName,
            @Singular Map<String, String> extraProperties,
            @VisibleForTesting @Singular List<MetricReader> extraMetricReaders) {
        checkArgument(StringUtils.isNotEmpty(clusterName), "Cluster name cannot be empty");
        AutoConfiguredOpenTelemetrySdkBuilder builder = AutoConfiguredOpenTelemetrySdk.builder();

        Map<String, String> overrideProperties = new HashMap<>();
        overrideProperties.put(OTEL_SDK_DISABLED, "true");
        overrideProperties.put(MAX_CARDINALITY_LIMIT_KEY, Integer.toString(MAX_CARDINALITY_LIMIT + 1));
        builder.addPropertiesSupplier(() -> overrideProperties);
        builder.addPropertiesSupplier(() -> extraProperties);

        builder.addResourceCustomizer(
                (resource, __) -> {
                    if (resource.getAttribute(CLUSTER_NAME_ATTRIBUTE) != null) {
                        return resource; // Do not override the attribute if already set.
                    }
                    return resource.merge(Resource.builder().put(CLUSTER_NAME_ATTRIBUTE, clusterName).build());
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
