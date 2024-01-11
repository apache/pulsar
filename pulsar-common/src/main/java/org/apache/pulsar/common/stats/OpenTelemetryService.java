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

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.resources.Resource;
import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import lombok.Builder;
import lombok.Singular;

public class OpenTelemetryService implements Closeable {

    public static final String CLUSTER_NAME_ATTRIBUTE = "pulsar.cluster";

    private final OpenTelemetrySdk openTelemetrySdk;

    private static final String MAX_CARDINALITY_LIMIT_KEY = "otel.experimental.metrics.cardinality.limit";
    public static final int MAX_CARDINALITY_LIMIT = 10000;

    @lombok.Builder
    public OpenTelemetryService(
            String clusterName,
            @Singular Map<String, String> extraProperties,
            @Singular Map<String, String> extraResources) {
        Objects.requireNonNull(clusterName);
        AutoConfiguredOpenTelemetrySdkBuilder builder = AutoConfiguredOpenTelemetrySdk.builder();
        builder.addPropertiesSupplier(
                () -> Collections.singletonMap(MAX_CARDINALITY_LIMIT_KEY, Integer.toString(MAX_CARDINALITY_LIMIT + 1)));
        builder.addPropertiesSupplier(() -> extraProperties);
        builder.addResourceCustomizer(
                (resource, __) -> resource.merge(Resource.builder().put(CLUSTER_NAME_ATTRIBUTE, clusterName).build()));
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
