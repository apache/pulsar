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
package org.apache.pulsar.broker.service.schema;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Summary;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.stats.MetricsUtil;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.apache.pulsar.opentelemetry.annotations.PulsarDeprecatedMetric;

class SchemaRegistryStats implements AutoCloseable, Runnable {
    private static final String NAMESPACE = "namespace";
    private static final double[] QUANTILES = {0.50, 0.75, 0.95, 0.99, 0.999, 0.9999, 1};
    /** simpleclient's default sliding window for summary quantiles; the 1.x default is 5 minutes. */
    private static final long QUANTILE_WINDOW_SECONDS = TimeUnit.MINUTES.toSeconds(10);

    public static final AttributeKey<String> REQUEST_TYPE_KEY =
            AttributeKey.stringKey("pulsar.schema_registry.request");
    @VisibleForTesting
    enum RequestType {
        GET,
        LIST,
        PUT,
        DELETE;

        public final Attributes attributes = Attributes.of(REQUEST_TYPE_KEY, name().toLowerCase());
    }

    public static final AttributeKey<String> RESPONSE_TYPE_KEY =
            AttributeKey.stringKey("pulsar.schema_registry.response");
    @VisibleForTesting
    enum ResponseType {
        SUCCESS,
        FAILURE;

        public final Attributes attributes = Attributes.of(RESPONSE_TYPE_KEY, name().toLowerCase());
    }

    public static final AttributeKey<String> COMPATIBILITY_CHECK_RESPONSE_KEY =
            AttributeKey.stringKey("pulsar.schema_registry.compatibility_check.response");
    @VisibleForTesting
    enum CompatibilityCheckResponse {
        COMPATIBLE,
        INCOMPATIBLE;

        public final Attributes attributes = Attributes.of(COMPATIBILITY_CHECK_RESPONSE_KEY, name().toLowerCase());
    }

    public static final String SCHEMA_REGISTRY_REQUEST_DURATION_METRIC_NAME =
            "pulsar.broker.request.schema_registry.duration";
    private final DoubleHistogram latencyHistogram;

    public static final String COMPATIBLE_COUNTER_METRIC_NAME =
            "pulsar.broker.operation.schema_registry.compatibility_check.count";
    private final LongCounter schemaCompatibilityCounter;

    // The 1.x client appends _total to counter names, so these are declared without the suffix
    // and are still exposed as pulsar_schema_*_total.
    @PulsarDeprecatedMetric(newMetricName = SCHEMA_REGISTRY_REQUEST_DURATION_METRIC_NAME)
    private static final Counter getOpsFailedCounter = buildCounter("pulsar_schema_get_ops_failed");
    @PulsarDeprecatedMetric(newMetricName = SCHEMA_REGISTRY_REQUEST_DURATION_METRIC_NAME)
    private static final Counter putOpsFailedCounter = buildCounter("pulsar_schema_put_ops_failed");
    @PulsarDeprecatedMetric(newMetricName = SCHEMA_REGISTRY_REQUEST_DURATION_METRIC_NAME)
    private static final Counter deleteOpsFailedCounter = buildCounter("pulsar_schema_del_ops_failed");

    @PulsarDeprecatedMetric(newMetricName = COMPATIBLE_COUNTER_METRIC_NAME)
    private static final Counter compatibleCounter = buildCounter("pulsar_schema_compatible");
    @PulsarDeprecatedMetric(newMetricName = COMPATIBLE_COUNTER_METRIC_NAME)
    private static final Counter incompatibleCounter = buildCounter("pulsar_schema_incompatible");

    @PulsarDeprecatedMetric(newMetricName = SCHEMA_REGISTRY_REQUEST_DURATION_METRIC_NAME)
    private static final Summary deleteOpsLatency = buildSummary("pulsar_schema_del_ops_latency", "-");

    @PulsarDeprecatedMetric(newMetricName = SCHEMA_REGISTRY_REQUEST_DURATION_METRIC_NAME)
    private static final Summary getOpsLatency = buildSummary("pulsar_schema_get_ops_latency", "-");

    @PulsarDeprecatedMetric(newMetricName = SCHEMA_REGISTRY_REQUEST_DURATION_METRIC_NAME)
    private static final Summary putOpsLatency = buildSummary("pulsar_schema_put_ops_latency", "-");

    private final Map<String, Long> namespaceAccess = new ConcurrentHashMap<>();
    private final ScheduledFuture<?> future;

    public SchemaRegistryStats(PulsarService pulsarService) {
        this.future = pulsarService.getExecutor().scheduleAtFixedRate(this, 1, 1, TimeUnit.MINUTES);

        var meter = pulsarService.getOpenTelemetry().getMeter();
        latencyHistogram = meter.histogramBuilder(SCHEMA_REGISTRY_REQUEST_DURATION_METRIC_NAME)
                .setDescription("The duration of Schema Registry requests.")
                .setUnit("s")
                .build();
        schemaCompatibilityCounter = meter.counterBuilder(COMPATIBLE_COUNTER_METRIC_NAME)
                .setDescription("The number of Schema Registry compatibility check operations performed by the broker.")
                .setUnit("{operation}")
                .build();
    }

    private static Counter buildCounter(String name) {
        return Counter.builder().name(name).help("-").labelNames(NAMESPACE).register();
    }

    private static Summary buildSummary(String name, String help) {
        Summary.Builder builder = Summary.builder().name(name).help(help).labelNames(NAMESPACE)
                .maxAgeSeconds(QUANTILE_WINDOW_SECONDS);

        for (double quantile : QUANTILES) {
            builder.quantile(quantile, 0.01D);
        }

        return builder.register();
    }

    void recordDelFailed(String schemaId, long millis) {
        deleteOpsFailedCounter.labelValues(getNamespace(schemaId)).inc();
        recordOperationLatency(schemaId, millis, RequestType.DELETE, ResponseType.FAILURE);
    }

    void recordGetFailed(String schemaId, long millis) {
        getOpsFailedCounter.labelValues(getNamespace(schemaId)).inc();
        recordOperationLatency(schemaId, millis, RequestType.GET, ResponseType.FAILURE);
    }

    void recordListFailed(String schemaId, long millis) {
        getOpsFailedCounter.labelValues(getNamespace(schemaId)).inc();
        recordOperationLatency(schemaId, millis, RequestType.LIST, ResponseType.FAILURE);
    }

    void recordPutFailed(String schemaId, long millis) {
        putOpsFailedCounter.labelValues(getNamespace(schemaId)).inc();
        recordOperationLatency(schemaId, millis, RequestType.PUT, ResponseType.FAILURE);
    }

    void recordDelLatency(String schemaId, long millis) {
        deleteOpsLatency.labelValues(getNamespace(schemaId)).observe(millis);
        recordOperationLatency(schemaId, millis, RequestType.DELETE, ResponseType.SUCCESS);
    }

    void recordGetLatency(String schemaId, long millis) {
        getOpsLatency.labelValues(getNamespace(schemaId)).observe(millis);
        recordOperationLatency(schemaId, millis, RequestType.GET, ResponseType.SUCCESS);
    }

    void recordListLatency(String schemaId, long millis) {
        getOpsLatency.labelValues(getNamespace(schemaId)).observe(millis);
        recordOperationLatency(schemaId, millis, RequestType.LIST, ResponseType.SUCCESS);
    }

    void recordPutLatency(String schemaId, long millis) {
        putOpsLatency.labelValues(getNamespace(schemaId)).observe(millis);
        recordOperationLatency(schemaId, millis, RequestType.PUT, ResponseType.SUCCESS);
    }

    private void recordOperationLatency(String schemaId, long millis,
                                        RequestType requestType, ResponseType responseType) {
        var duration = MetricsUtil.convertToSeconds(millis, TimeUnit.MILLISECONDS);
        var namespace = getNamespace(schemaId);
        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, namespace)
                .putAll(requestType.attributes)
                .putAll(responseType.attributes)
                .build();
        latencyHistogram.record(duration, attributes);
    }

    void recordSchemaIncompatible(String schemaId) {
        var namespace = getNamespace(schemaId);
        incompatibleCounter.labelValues(namespace).inc();
        recordSchemaCompabilityResult(namespace, CompatibilityCheckResponse.INCOMPATIBLE);
    }

    void recordSchemaCompatible(String schemaId) {
        var namespace = getNamespace(schemaId);
        compatibleCounter.labelValues(namespace).inc();
        recordSchemaCompabilityResult(namespace, CompatibilityCheckResponse.COMPATIBLE);
    }

    private void recordSchemaCompabilityResult(String namespace, CompatibilityCheckResponse result) {
        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, namespace)
                .putAll(result.attributes)
                .build();
        schemaCompatibilityCounter.add(1, attributes);
    }

    private String getNamespace(String schemaId) {
        String namespace;
        try {
            namespace = TopicName.get(schemaId).getNamespace();
        } catch (IllegalArgumentException t) {
            namespace = "unknown";
        }

        this.namespaceAccess.put(namespace, System.currentTimeMillis());
        return namespace;
    }


    private void removeChild(String namespace) {
        getOpsFailedCounter.remove(namespace);
        putOpsFailedCounter.remove(namespace);
        deleteOpsFailedCounter.remove(namespace);
        compatibleCounter.remove(namespace);
        incompatibleCounter.remove(namespace);
        deleteOpsLatency.remove(namespace);
        getOpsLatency.remove(namespace);
        putOpsLatency.remove(namespace);
    }

    @Override
    public synchronized void close() throws Exception {
        namespaceAccess.keySet().forEach(this::removeChild);
        future.cancel(false);
    }

    @Override
    public void run() {
        long now = System.currentTimeMillis();
        long interval = TimeUnit.MINUTES.toMillis(5);

        this.namespaceAccess.entrySet().removeIf(entry -> {
            String namespace = entry.getKey();
            long accessTime = entry.getValue();
            if (now - accessTime > interval) {
                this.removeChild(namespace);
                return true;
            }
            return false;
        });
    }
}
