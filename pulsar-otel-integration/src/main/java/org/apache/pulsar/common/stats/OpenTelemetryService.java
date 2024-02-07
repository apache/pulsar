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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.ResourceAttributes;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;

/**
 * Provides a common OpenTelemetry service for Pulsar components to use. Responsible for instantiating the OpenTelemetry
 * SDK with a set of override properties. Once initialized, furnishes access to OpenTelemetry.
 */
public class OpenTelemetryService implements Closeable {

    public static final AttributeKey<String> CLUSTER_ATTRIBUTE = AttributeKey.stringKey("pulsar.cluster");
    static final String OTEL_SDK_DISABLED_KEY = "otel.sdk.disabled";
    static final int MAX_CARDINALITY_LIMIT = 10000;

    private final OpenTelemetrySdk openTelemetrySdk;

    @Builder
    public OpenTelemetryService(String clusterName,
                                String serviceName,
                                String serviceVersion,
                                // Allows customizing the SDK builder; for testing purposes only.
                                @VisibleForTesting Consumer<AutoConfiguredOpenTelemetrySdkBuilder> sdkBuilderConsumer) {
        checkArgument(StringUtils.isNotBlank(clusterName), "Cluster name cannot be empty");
        AutoConfiguredOpenTelemetrySdkBuilder sdkBuilder = AutoConfiguredOpenTelemetrySdk.builder();

        Map<String, String> overrideProperties = new HashMap<>();
        overrideProperties.put(OTEL_SDK_DISABLED_KEY, "true");
        // Cardinality limit includes the overflow attribute set, so we need to add 1.
        overrideProperties.put(
                "otel.experimental.metrics.cardinality.limit", Integer.toString(MAX_CARDINALITY_LIMIT + 1));
        sdkBuilder.addPropertiesSupplier(() -> overrideProperties);

        sdkBuilder.addResourceCustomizer(
                (resource, __) -> {
                    var resourceBuilder = Resource.builder();
                    // Do not override attributes if already set (via system properties or environment variables).
                    if (resource.getAttribute(CLUSTER_ATTRIBUTE) == null) {
                        resourceBuilder.put(CLUSTER_ATTRIBUTE, clusterName);
                    }
                    if (StringUtils.isNotBlank(serviceName)
                            && Objects.equals(Resource.getDefault().getAttribute(ResourceAttributes.SERVICE_NAME),
                                              resource.getAttribute(ResourceAttributes.SERVICE_NAME))) {
                        resourceBuilder.put(ResourceAttributes.SERVICE_NAME, serviceName);
                    }
                    if (StringUtils.isNotBlank(serviceVersion)
                            && resource.getAttribute(ResourceAttributes.SERVICE_VERSION) == null) {
                        resourceBuilder.put(ResourceAttributes.SERVICE_VERSION, serviceVersion);
                    }
                    return resource.merge(resourceBuilder.build());
                });

        if (sdkBuilderConsumer != null) {
            sdkBuilderConsumer.accept(sdkBuilder);
        }

        openTelemetrySdk = sdkBuilder.build().getOpenTelemetrySdk();
    }

    public OpenTelemetry getOpenTelemetry() {
        return openTelemetrySdk;
    }

    @Override
    public void close() {
        openTelemetrySdk.close();
    }
}
