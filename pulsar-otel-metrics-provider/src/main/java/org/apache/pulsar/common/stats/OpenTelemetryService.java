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
import io.opentelemetry.sdk.resources.Resource;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Singular;
import org.apache.commons.lang3.StringUtils;

public class OpenTelemetryService implements Closeable {

    public static final String OTEL_SDK_DISABLED = "otel.sdk.disabled";
    private static final String OTEL_TRACES_EXPORTER = "otel.traces.exporter";
    private static final String OTEL_LOGS_EXPORTER = "otel.logs.exporter";
    private static final String MAX_CARDINALITY_LIMIT_KEY = "otel.experimental.metrics.cardinality.limit";

    public static final int MAX_CARDINALITY_LIMIT = 10000;

    private final OpenTelemetrySdk openTelemetrySdk;

    @Builder
    public OpenTelemetryService(String clusterName,
                                @Singular Map<String, String> extraProperties,
                                // Allows customizing the SDK builder; for testing purposes only.
                                @VisibleForTesting AutoConfiguredOpenTelemetrySdkBuilder sdkBuilder) {
        checkArgument(StringUtils.isNotEmpty(clusterName), "Cluster name cannot be empty");
        if (sdkBuilder == null) {
            sdkBuilder = AutoConfiguredOpenTelemetrySdk.builder();
        }

        Map<String, String> overrideProperties = new HashMap<>();
        overrideProperties.put(OTEL_SDK_DISABLED, "true");
        // The logs and traces exporters are not included in the distribution, so we need to disable them.
        overrideProperties.put(OTEL_LOGS_EXPORTER, "none");
        overrideProperties.put(OTEL_TRACES_EXPORTER, "none");
        // Cardinality limit property is exclusive, so we need to add 1.
        overrideProperties.put(MAX_CARDINALITY_LIMIT_KEY, Integer.toString(MAX_CARDINALITY_LIMIT + 1));
        sdkBuilder.addPropertiesSupplier(() -> overrideProperties);
        sdkBuilder.addPropertiesSupplier(() -> extraProperties);

        sdkBuilder.addResourceCustomizer(
                (resource, __) -> {
                    AttributeKey<String> clusterNameAttribute = AttributeKey.stringKey("pulsar.cluster");
                    if (resource.getAttribute(clusterNameAttribute) != null) {
                        // Do not override if already set (via system properties or environment variables).
                        return resource;
                    }
                    return resource.merge(Resource.builder().put(clusterNameAttribute, clusterName).build());
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
