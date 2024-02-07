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
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.resources.Resource;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Singular;
import org.apache.commons.lang3.StringUtils;

/**
 * Provides a common OpenTelemetry service for Pulsar components to use. Responsible for instantiating the OpenTelemetry
 * SDK with a set of override properties. Once initialized, furnishes access to OpenTelemetry.
 */
public class OpenTelemetryService implements Closeable {

    public static final AttributeKey<String> CLUSTER_ATTRIBUTE = AttributeKey.stringKey("pulsar.cluster");
    public static final AttributeKey<String> SERVICE_NAME_ATTRIBUTE = AttributeKey.stringKey("service.name");
    public static final AttributeKey<String> SERVICE_VERSION_ATTRIBUTE = AttributeKey.stringKey("service.version");
    public static final String OTEL_SDK_DISABLED = "otel.sdk.disabled";
    private static final String MAX_CARDINALITY_LIMIT_KEY = "otel.experimental.metrics.cardinality.limit";
    public static final int MAX_CARDINALITY_LIMIT = 10000;

    private final OpenTelemetrySdk openTelemetrySdk;

    @Builder
    public OpenTelemetryService(String clusterName,
                                String serviceName,
                                String serviceVersion,
                                @VisibleForTesting @Singular Map<String, String> extraProperties,
                                // Allows customizing the SDK builder; for testing purposes only.
                                @VisibleForTesting Consumer<AutoConfigurationCustomizer> autoConfigurationCustomizer) {
        checkArgument(StringUtils.isNotEmpty(clusterName), "Cluster name cannot be empty");
        AutoConfiguredOpenTelemetrySdkBuilder sdkBuilder = AutoConfiguredOpenTelemetrySdk.builder();
        if (autoConfigurationCustomizer != null) {
            autoConfigurationCustomizer.accept(sdkBuilder);
        }

        Map<String, String> overrideProperties = new HashMap<>();
        overrideProperties.put(OTEL_SDK_DISABLED, "true");
        // Cardinality limit includes the overflow attribute set, so we need to add 1.
        overrideProperties.put(MAX_CARDINALITY_LIMIT_KEY, Integer.toString(MAX_CARDINALITY_LIMIT + 1));
        sdkBuilder.addPropertiesSupplier(() -> overrideProperties);
        sdkBuilder.addPropertiesSupplier(() -> extraProperties);

        sdkBuilder.addResourceCustomizer(
                (resource, __) -> {
                    var resourceBuilder = Resource.builder();
                    // Do not override attributes if already set (via system properties or environment variables).
                    if (resource.getAttribute(CLUSTER_ATTRIBUTE) == null) {
                        resourceBuilder.put(CLUSTER_ATTRIBUTE, clusterName);
                    }
                    if (StringUtils.isNotEmpty(serviceName)
                            && Objects.equals(Resource.getDefault().getAttribute(SERVICE_NAME_ATTRIBUTE),
                                              resource.getAttribute(SERVICE_NAME_ATTRIBUTE))) {
                        resourceBuilder.put(SERVICE_NAME_ATTRIBUTE, serviceName);
                    }
                    if (StringUtils.isNotEmpty(serviceVersion)
                            && resource.getAttribute(SERVICE_VERSION_ATTRIBUTE) == null) {
                        resourceBuilder.put(SERVICE_VERSION_ATTRIBUTE, serviceVersion);
                    }
                    return resource.merge(resourceBuilder.build());
                });

        openTelemetrySdk = sdkBuilder.build().getOpenTelemetrySdk();
    }

    public Meter getMeter(String instrumentationScopeName) {
        return openTelemetrySdk.getMeter(instrumentationScopeName);
    }

    @Override
    public void close() {
        openTelemetrySdk.close();
    }
}
