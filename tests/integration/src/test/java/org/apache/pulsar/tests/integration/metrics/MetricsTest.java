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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.tests.integration.containers.OpenTelemetryCollectorContainer;
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

    @Test(timeOut = 300_000)
    public void testOpenTelemetryMetricsPresent() throws Exception {
        var clusterName = "testOpenTelemetryMetrics-" + UUID.randomUUID();
        var openTelemetryCollectorContainer = new OpenTelemetryCollectorContainer(clusterName);

        var brokerOtelServiceName = clusterName + "-broker";
        var brokerCollectorProps = getCollectorProps(openTelemetryCollectorContainer, brokerOtelServiceName);

        var proxyOtelServiceName = clusterName + "-proxy";
        var proxyCollectorProps = getCollectorProps(openTelemetryCollectorContainer, proxyOtelServiceName);

        var functionWorkerServiceNameSuffix = PulsarTestBase.randomName();
        var functionWorkerOtelServiceName = "function-worker-" + functionWorkerServiceNameSuffix;
        var functionWorkerCollectorProps =
                getCollectorProps(openTelemetryCollectorContainer, functionWorkerOtelServiceName);

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
        var serviceUrl = pulsarCluster.getPlainTextServiceUrl();
        var functionWorkerCommand = getFunctionWorkerCommand(serviceUrl, functionWorkerServiceNameSuffix);
        pulsarCluster.getAnyWorker().execCmdAsync(functionWorkerCommand.split(" "));

        Awaitility.waitAtMost(90, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            var metricName = "queueSize_ratio"; // Sent automatically by the OpenTelemetry SDK.
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

    private static Map<String, String> getCollectorProps(
            OpenTelemetryCollectorContainer openTelemetryCollectorContainer, String functionWorkerOtelServiceName) {
        return Map.of(
                "OTEL_SDK_DISABLED", "false",
                "OTEL_METRICS_EXPORTER", "otlp",
                "OTEL_METRIC_EXPORT_INTERVAL", "1000",
                "OTEL_EXPORTER_OTLP_ENDPOINT", openTelemetryCollectorContainer.getOtlpEndpoint(),
                "OTEL_SERVICE_NAME", functionWorkerOtelServiceName
        );
    }

    private static String getFunctionWorkerCommand(String serviceUrl, String suffix) {
        var namespace = NamespaceName.get("public", "default");
        var sourceTopicName = TopicName.get(TopicDomain.persistent.toString(), namespace, "metricTestSource-" + suffix);
        var sinkTopicName = TopicName.get(TopicDomain.persistent.toString(), namespace, "metricTestSink-" + suffix);

        var commandGenerator = new CommandGenerator();
        commandGenerator.setAdminUrl(serviceUrl);
        commandGenerator.setSourceTopic(sourceTopicName.toString());
        commandGenerator.setSinkTopic(sinkTopicName.toString());
        commandGenerator.setFunctionName("metricsTestLocalRunTest-" + suffix);
        commandGenerator.setRuntime(CommandGenerator.Runtime.JAVA);
        commandGenerator.setFunctionClassName(PulsarFunctionsTest.EXCLAMATION_JAVA_CLASS);
        return commandGenerator.generateLocalRunCommand(null);
    }
}
