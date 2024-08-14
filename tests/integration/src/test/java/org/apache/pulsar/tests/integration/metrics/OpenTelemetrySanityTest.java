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
package org.apache.pulsar.tests.integration.metrics;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.waitAtMost;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.broker.stats.PulsarBrokerOpenTelemetry;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient;
import org.apache.pulsar.functions.worker.PulsarWorkerOpenTelemetry;
import org.apache.pulsar.proxy.stats.PulsarProxyOpenTelemetry;
import org.apache.pulsar.tests.integration.containers.ChaosContainer;
import org.apache.pulsar.tests.integration.containers.OpenTelemetryCollectorContainer;
import org.apache.pulsar.tests.integration.topologies.FunctionRuntimeType;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarTestBase;
import org.testng.annotations.Test;

public class OpenTelemetrySanityTest {

    // Validate that the OpenTelemetry metrics can be exported to a remote OpenTelemetry collector.
    @Test(timeOut = 360_000)
    public void testOpenTelemetryMetricsOtlpExport() throws Exception {
        var clusterName = "testOpenTelemetryMetrics-" + UUID.randomUUID();
        var openTelemetryCollectorContainer = new OpenTelemetryCollectorContainer(clusterName);

        var exporter = "otlp";
        var otlpEndpointProp =
                Pair.of("OTEL_EXPORTER_OTLP_ENDPOINT", openTelemetryCollectorContainer.getOtlpEndpoint());

        var brokerCollectorProps = getOpenTelemetryProps(exporter, otlpEndpointProp);
        var proxyCollectorProps = getOpenTelemetryProps(exporter, otlpEndpointProp);
        var functionWorkerCollectorProps = getOpenTelemetryProps(exporter, otlpEndpointProp);

        var spec = PulsarClusterSpec.builder()
                .clusterName(clusterName)
                .brokerEnvs(brokerCollectorProps)
                .proxyEnvs(proxyCollectorProps)
                .externalService("otel-collector", openTelemetryCollectorContainer)
                .functionWorkerEnvs(functionWorkerCollectorProps)
                .build();
        @Cleanup("stop")
        var pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();
        pulsarCluster.setupFunctionWorkers(PulsarTestBase.randomName(), FunctionRuntimeType.PROCESS, 1);

        // TODO: Validate cluster name and service version are present once
        // https://github.com/open-telemetry/opentelemetry-java/issues/6108 is solved.
        var metricName = "queueSize_ratio"; // Sent automatically by the OpenTelemetry SDK.
        waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).until(() -> {
            var metrics = getMetricsFromPrometheus(
                    openTelemetryCollectorContainer, OpenTelemetryCollectorContainer.PROMETHEUS_EXPORTER_PORT);
            return !metrics.findByNameAndLabels(metricName, "job", PulsarBrokerOpenTelemetry.SERVICE_NAME).isEmpty();
        });
        waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).until(() -> {
            var metrics = getMetricsFromPrometheus(
                    openTelemetryCollectorContainer, OpenTelemetryCollectorContainer.PROMETHEUS_EXPORTER_PORT);
            return !metrics.findByNameAndLabels(metricName, "job", PulsarProxyOpenTelemetry.SERVICE_NAME).isEmpty();
        });
        waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).until(() -> {
            var metrics = getMetricsFromPrometheus(
                    openTelemetryCollectorContainer, OpenTelemetryCollectorContainer.PROMETHEUS_EXPORTER_PORT);
            return !metrics.findByNameAndLabels(metricName, "job", PulsarWorkerOpenTelemetry.SERVICE_NAME).isEmpty();
        });
    }

    /*
     * Validate that the OpenTelemetry metrics can be exported to a local Prometheus endpoint running in the same
     * process space as the broker/proxy/function-worker.
     * https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md#prometheus-exporter
     */
    @Test(timeOut = 360_000)
    public void testOpenTelemetryMetricsPrometheusExport() throws Exception {
        var prometheusExporterPort = 9464;
        var clusterName = "testOpenTelemetryMetrics-" + UUID.randomUUID();

        var exporter = "prometheus";
        var prometheusExporterPortProp =
                Pair.of("OTEL_EXPORTER_PROMETHEUS_PORT", Integer.toString(prometheusExporterPort));

        var brokerCollectorProps = getOpenTelemetryProps(exporter, prometheusExporterPortProp);
        var proxyCollectorProps = getOpenTelemetryProps(exporter, prometheusExporterPortProp);
        var functionWorkerCollectorProps = getOpenTelemetryProps(exporter, prometheusExporterPortProp);

        var spec = PulsarClusterSpec.builder()
                .clusterName(clusterName)
                .brokerEnvs(brokerCollectorProps)
                .brokerAdditionalPorts(List.of(prometheusExporterPort))
                .proxyEnvs(proxyCollectorProps)
                .proxyAdditionalPorts(List.of(prometheusExporterPort))
                .functionWorkerEnvs(functionWorkerCollectorProps)
                .functionWorkerAdditionalPorts(List.of(prometheusExporterPort))
                .build();
        @Cleanup("stop")
        var pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();
        pulsarCluster.setupFunctionWorkers(PulsarTestBase.randomName(), FunctionRuntimeType.PROCESS, 1);

        var targetInfoMetricName = "target_info"; // Sent automatically by the OpenTelemetry SDK.
        var cpuCountMetricName = "jvm_cpu_count"; // Configured by the OpenTelemetryService.
        waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
            var expectedMetrics = new String[] {targetInfoMetricName, cpuCountMetricName, "pulsar_broker_topic_producer_count"};
            var actualMetrics = getMetricsFromPrometheus(pulsarCluster.getBroker(0), prometheusExporterPort);
            assertThat(expectedMetrics).allMatch(expectedMetric -> !actualMetrics.findByNameAndLabels(expectedMetric,
                    Pair.of("pulsar_cluster", clusterName),
                    Pair.of("service_name", PulsarBrokerOpenTelemetry.SERVICE_NAME),
                    Pair.of("service_version", PulsarVersion.getVersion()),
                    Pair.of("host_name", pulsarCluster.getBroker(0).getHostname())).isEmpty());
        });
        waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
            var expectedMetrics = new String[] {targetInfoMetricName, cpuCountMetricName};
            var actualMetrics = getMetricsFromPrometheus(pulsarCluster.getProxy(), prometheusExporterPort);
            assertThat(expectedMetrics).allMatch(expectedMetric -> !actualMetrics.findByNameAndLabels(expectedMetric,
                    Pair.of("pulsar_cluster", clusterName),
                    Pair.of("service_name", PulsarProxyOpenTelemetry.SERVICE_NAME),
                    Pair.of("service_version", PulsarVersion.getVersion()),
                    Pair.of("host_name", pulsarCluster.getProxy().getHostname())).isEmpty());
        });
        waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
            var expectedMetrics = new String[] {targetInfoMetricName, cpuCountMetricName};
            var actualMetrics = getMetricsFromPrometheus(pulsarCluster.getAnyWorker(), prometheusExporterPort);
            assertThat(expectedMetrics).allMatch(expectedMetric -> !actualMetrics.findByNameAndLabels(expectedMetric,
                    Pair.of("pulsar_cluster", clusterName),
                    Pair.of("service_name", PulsarWorkerOpenTelemetry.SERVICE_NAME),
                    Pair.of("service_version", PulsarVersion.getVersion()),
                    Pair.of("host_name", pulsarCluster.getAnyWorker().getHostname())).isEmpty());
        });
    }

    private static PrometheusMetricsClient.Metrics getMetricsFromPrometheus(ChaosContainer<?> container, int port) {
        var client = new PrometheusMetricsClient(container.getHost(), container.getMappedPort(port));
        return client.getMetrics();
    }

    private static Map<String, String> getOpenTelemetryProps(String exporter, Pair<String, String> ... extraProps) {
        var defaultProps = Map.of(
                "OTEL_SDK_DISABLED", "false",
                "OTEL_METRIC_EXPORT_INTERVAL", "1000",
                "OTEL_METRICS_EXPORTER", exporter
        );
        var props = new HashMap<>(defaultProps);
        Arrays.stream(extraProps).forEach(p -> props.put(p.getKey(), p.getValue()));
        return props;
    }
}
