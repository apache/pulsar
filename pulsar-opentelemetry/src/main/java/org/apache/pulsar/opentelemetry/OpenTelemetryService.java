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
package org.apache.pulsar.opentelemetry;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.instrumentation.runtimetelemetry.RuntimeTelemetry;
import io.opentelemetry.instrumentation.runtimetelemetry.RuntimeTelemetryBuilder;
import io.opentelemetry.instrumentation.runtimetelemetry.internal.Experimental;
import io.opentelemetry.instrumentation.runtimetelemetry.internal.Internal;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.common.export.MemoryMode;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.ServiceAttributes;
import java.io.Closeable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;

/**
 * Provides a common OpenTelemetry service for Pulsar components to use. Responsible for instantiating the OpenTelemetry
 * SDK with a set of override properties. Once initialized, furnishes access to OpenTelemetry.
 */
public class OpenTelemetryService implements Closeable {

    public static final String OTEL_SDK_DISABLED_KEY = "otel.sdk.disabled";
    static final int MAX_CARDINALITY_LIMIT = 10000;

    private final AtomicReference<OpenTelemetrySdk> openTelemetrySdkReference = new AtomicReference<>();

    private final AtomicReference<RuntimeTelemetry> runtimeTelemetryReference = new AtomicReference<>();

    /**
     * Instantiates the OpenTelemetry SDK. All attributes are overridden by system properties or environment
     * variables.
     *
     * @param clusterName
     *      The name of the Pulsar cluster. Cannot be null or blank.
     * @param serviceName
     *      The name of the service. Optional.
     * @param serviceVersion
     *      The version of the service. Optional.
     * @param builderCustomizer
     *      Allows customizing the SDK builder; for testing purposes only.
     */
    @Builder
    public OpenTelemetryService(String clusterName,
                                String serviceName,
                                String serviceVersion,
                                @VisibleForTesting Consumer<AutoConfiguredOpenTelemetrySdkBuilder> builderCustomizer) {
        checkArgument(StringUtils.isNotBlank(clusterName), "Cluster name cannot be empty");
        var sdkBuilder = AutoConfiguredOpenTelemetrySdk.builder();

        sdkBuilder.addPropertiesSupplier(() -> Map.of(
                OTEL_SDK_DISABLED_KEY, "true",
                // Cardinality limit includes the overflow attribute set, so we need to add 1.
                "otel.java.metrics.cardinality.limit", Integer.toString(MAX_CARDINALITY_LIMIT + 1),
                // Reduce number of allocations by using reusable data mode.
                "otel.java.exporter.memory_mode", MemoryMode.REUSABLE_DATA.name()
        ));

        sdkBuilder.addResourceCustomizer(
                (resource, __) -> {
                    var resourceBuilder = Resource.builder();
                    // Do not override attributes if already set (via system properties or environment variables).
                    if (resource.getAttribute(OpenTelemetryAttributes.PULSAR_CLUSTER) == null) {
                        resourceBuilder.put(OpenTelemetryAttributes.PULSAR_CLUSTER, clusterName);
                    }
                    if (StringUtils.isNotBlank(serviceName)
                            && Objects.equals(Resource.getDefault().getAttribute(ServiceAttributes.SERVICE_NAME),
                                              resource.getAttribute(ServiceAttributes.SERVICE_NAME))) {
                        resourceBuilder.put(ServiceAttributes.SERVICE_NAME, serviceName);
                    }
                    if (StringUtils.isNotBlank(serviceVersion)
                            && resource.getAttribute(ServiceAttributes.SERVICE_VERSION) == null) {
                        resourceBuilder.put(ServiceAttributes.SERVICE_VERSION, serviceVersion);
                    }
                    return resource.merge(resourceBuilder.build());
                });

        sdkBuilder.addMetricReaderCustomizer((metricReader, configProperties) -> {
            if (metricReader instanceof PrometheusHttpServer prometheusHttpServer) {
                // At this point, the server is already started. We need to close it and create a new one with the
                // correct resource attributes filter.
                prometheusHttpServer.close();

                // Allow all resource attributes to be exposed.
                return prometheusHttpServer.toBuilder()
                        .setAllowedResourceAttributesFilter(s -> true)
                        .build();
            }
            return metricReader;
        });

        if (builderCustomizer != null) {
            builderCustomizer.accept(sdkBuilder);
        }

        openTelemetrySdkReference.set(sdkBuilder.build().getOpenTelemetrySdk());

        // For a list of exposed metrics, see https://opentelemetry.io/docs/specs/semconv/runtime/jvm-metrics/
        RuntimeTelemetryBuilder runtimeTelemetryBuilder =
                RuntimeTelemetry.builder(openTelemetrySdkReference.get());
        // Disable JFR-based telemetry and rely on JMX-based metrics only.
        Internal.setDisableAllJfrFeatures(runtimeTelemetryBuilder, true);
        // Emit experimental JMX-based runtime metrics in addition to the stable ones.
        Experimental.setEmitExperimentalMetrics(runtimeTelemetryBuilder, true);
        runtimeTelemetryReference.set(runtimeTelemetryBuilder.build());
    }

    public OpenTelemetry getOpenTelemetry() {
        return openTelemetrySdkReference.get();
    }

    @Override
    public void close() {
        RuntimeTelemetry runtimeTelemetry = runtimeTelemetryReference.getAndSet(null);
        if (runtimeTelemetry != null) {
            runtimeTelemetry.close();
        }
        OpenTelemetrySdk openTelemetrySdk = openTelemetrySdkReference.getAndSet(null);
        if (openTelemetrySdk != null) {
            openTelemetrySdk.close();
        }
    }
}
