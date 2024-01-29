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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.tests.integration.containers.OpenTelemetryCollectorContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.functions.PulsarFunctionsTest;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator;
import org.apache.pulsar.tests.integration.topologies.FunctionRuntimeType;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarTestBase;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Slf4j
public class MetricsTest {

    // Test with the included Prometheus exporter as well
    // https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md#prometheus-exporter
    @Test(timeOut = 300_000)
    public void testOpenTelemetryMetricsOtlpExport() throws Exception {
        var clusterName = "testOpenTelemetryMetrics-" + UUID.randomUUID();
        var openTelemetryCollectorContainer = new OpenTelemetryCollectorContainer(clusterName);

        var brokerOtelServiceName = clusterName + "-broker";
        var brokerCollectorProps = getCollectorProps(brokerOtelServiceName, openTelemetryCollectorContainer);

        var proxyOtelServiceName = clusterName + "-proxy";
        var proxyCollectorProps = getCollectorProps(proxyOtelServiceName, openTelemetryCollectorContainer);

        var functionWorkerServiceNameSuffix = PulsarTestBase.randomName();
        var functionWorkerOtelServiceName = "function-worker-" + functionWorkerServiceNameSuffix;
        var functionWorkerCollectorProps =
                getCollectorProps(functionWorkerOtelServiceName, openTelemetryCollectorContainer);

        var spec = PulsarClusterSpec.builder()
                .clusterName(clusterName)
                .brokerEnvs(brokerCollectorProps)
                .proxyEnvs(proxyCollectorProps)
                .externalService("otel-collector", openTelemetryCollectorContainer)
                .functionWorkerEnv(functionWorkerServiceNameSuffix, functionWorkerCollectorProps)
                .build();
        @Cleanup("stop")
        var pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        pulsarCluster.setupFunctionWorkers(functionWorkerServiceNameSuffix, FunctionRuntimeType.PROCESS, 1);
        var functionWorkerCommand = getFunctionWorkerCommand(pulsarCluster, functionWorkerServiceNameSuffix);
        pulsarCluster.getAnyWorker().execCmdAsync("sh", "-c", functionWorkerCommand);

        var metricName = "queueSize_ratio"; // Sent automatically by the OpenTelemetry SDK.
        Awaitility.waitAtMost(90, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            var metrics = openTelemetryCollectorContainer.getMetricsClient().getMetrics();
            // TODO: Validate cluster name is present once
            // https://github.com/open-telemetry/opentelemetry-java/issues/6108 is solved.
            var brokerMetrics = metrics.findByNameAndLabels(metricName, Pair.of("job", brokerOtelServiceName));
            var proxyMetrics = metrics.findByNameAndLabels(metricName, Pair.of("job", proxyOtelServiceName));
            var functionWorkerMetrics =
                    metrics.findByNameAndLabels(metricName, Pair.of("job", functionWorkerOtelServiceName));
            return !brokerMetrics.isEmpty() && !proxyMetrics.isEmpty() && !functionWorkerMetrics.isEmpty();
        });
    }

    @Test(timeOut = 300_000)
    public void testOpenTelemetryMetricsPrometheusExport() throws Exception {
        var prometheusExporterPort = 9464;
        var clusterName = "testOpenTelemetryMetrics-" + UUID.randomUUID();

        var brokerOtelServiceName = clusterName + "-broker";
        var brokerCollectorProps = getCollectorProps(brokerOtelServiceName, prometheusExporterPort);

        var proxyOtelServiceName = clusterName + "-proxy";
        var proxyCollectorProps = getCollectorProps(proxyOtelServiceName, prometheusExporterPort);

        var functionWorkerServiceNameSuffix = PulsarTestBase.randomName();
        var functionWorkerOtelServiceName = "function-worker-" + functionWorkerServiceNameSuffix;
        var functionWorkerCollectorProps = getCollectorProps(functionWorkerOtelServiceName, prometheusExporterPort);

        var spec = PulsarClusterSpec.builder()
                .clusterName(clusterName)
                .brokerEnvs(brokerCollectorProps)
                .brokerAdditionalPorts(List.of(prometheusExporterPort))
                .proxyEnvs(proxyCollectorProps)
                .proxyAdditionalPorts(List.of(prometheusExporterPort))
                .functionWorkerEnv(functionWorkerServiceNameSuffix, functionWorkerCollectorProps)
                .functionWorkerAdditionalPort(functionWorkerServiceNameSuffix, List.of(prometheusExporterPort))
                .build();
        @Cleanup("stop")
        var pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        pulsarCluster.setupFunctionWorkers(functionWorkerServiceNameSuffix, FunctionRuntimeType.PROCESS, 1);
        var functionWorkerCommand = getFunctionWorkerCommand(pulsarCluster, functionWorkerServiceNameSuffix);
        var workerContainer = pulsarCluster.getAnyWorker();
        workerContainer.execCmdAsync("sh", "-c", functionWorkerCommand);

        var metricName = "target_info"; // Sent automatically by the OpenTelemetry SDK.
        Awaitility.waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).until(() -> {
            var prometheusClient = createMetricsClient(pulsarCluster.getAnyBroker(), prometheusExporterPort);
            var metrics = prometheusClient.getMetrics();
            var expectedMetrics = metrics.findByNameAndLabels(metricName,
                    Pair.of("pulsar_cluster", clusterName), Pair.of("service_name", brokerOtelServiceName));
            return !expectedMetrics.isEmpty();
        });

        Awaitility.waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).until(() -> {
            var prometheusClient = createMetricsClient(pulsarCluster.getProxy(), prometheusExporterPort);
            var metrics = prometheusClient.getMetrics();
            var expectedMetrics = metrics.findByNameAndLabels(metricName,
                    Pair.of("pulsar_cluster", clusterName), Pair.of("service_name", proxyOtelServiceName));
            return !expectedMetrics.isEmpty();
        });

        Awaitility.waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).until(() -> {
            var prometheusClient = createMetricsClient(workerContainer, prometheusExporterPort);
            var metrics = prometheusClient.getMetrics();
            var expectedMetrics = metrics.findByNameAndLabels(metricName,
                    Pair.of("pulsar_cluster", clusterName), Pair.of("service_name", functionWorkerOtelServiceName));
            return !expectedMetrics.isEmpty();
        });
    }

    private static PrometheusMetricsClient createMetricsClient(PulsarContainer container, int port) {
        return new PrometheusMetricsClient(container.getHost(), container.getMappedPort(port));
    }

    private static Map<String, String> getCollectorProps(
            String serviceName, OpenTelemetryCollectorContainer openTelemetryCollectorContainer) {
        return Map.of(
                "OTEL_SDK_DISABLED", "false",
                "OTEL_SERVICE_NAME", serviceName,
                "OTEL_METRICS_EXPORTER", "otlp",
                "OTEL_EXPORTER_OTLP_ENDPOINT", openTelemetryCollectorContainer.getOtlpEndpoint(),
                "OTEL_METRIC_EXPORT_INTERVAL", "1000"
        );
    }

    private static Map<String, String> getCollectorProps(String serviceName, int prometheusExporterPort,
                                                         Pair<String, String> ... extraProps) {
        var defaultProps = Map.of(
                "OTEL_SDK_DISABLED", "false",
                "OTEL_SERVICE_NAME", serviceName,
                "OTEL_METRICS_EXPORTER", "prometheus",
                "OTEL_EXPORTER_PROMETHEUS_PORT", Integer.toString(prometheusExporterPort),
                "OTEL_METRIC_EXPORT_INTERVAL", "1000"
        );
        var props = new HashMap<>(defaultProps);
        Arrays.stream(extraProps).forEach(p -> props.put(p.getKey(), p.getValue()));
        return props;
    }

    private static String getFunctionWorkerCommand(PulsarCluster pulsarCluster, String suffix)
            throws Exception {
        var namespace = NamespaceName.get("public", "default");
        var sourceTopicName = TopicName.get(TopicDomain.persistent.toString(), namespace, "metricTestSource-" + suffix);
        var sinkTopicName = TopicName.get(TopicDomain.persistent.toString(), namespace, "metricTestSink-" + suffix);

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(sourceTopicName.toString());
            admin.topics().createNonPartitionedTopic(sinkTopicName.toString());
        }

        var commandGenerator = new CommandGenerator();
        commandGenerator.setSourceTopic(sourceTopicName.toString());
        commandGenerator.setSinkTopic(sinkTopicName.toString());
        commandGenerator.setRuntime(CommandGenerator.Runtime.JAVA);
        commandGenerator.setFunctionName("metricsTestLocalRunTest-" + suffix);
        commandGenerator.setFunctionClassName(PulsarFunctionsTest.EXCLAMATION_JAVA_CLASS);
        return commandGenerator.generateCreateFunctionCommand();
    }
}
