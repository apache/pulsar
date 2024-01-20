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
import org.apache.pulsar.tests.integration.containers.OpenTelemetryCollectorContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarTestBase;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Slf4j
public class MetricsTest {

    @Test
    public void testOpenTelemetryMetrics() throws Exception {
        var clusterName = "testOpenTelemetryMetrics-" + UUID.randomUUID();
        var openTelemetryCollectorContainer = new OpenTelemetryCollectorContainer(clusterName);

        var brokerOtelServiceName = clusterName + "-broker";
        var brokerCollectorProps = Map.of(
                "OTEL_SDK_DISABLED", "false",
                "OTEL_METRICS_EXPORTER", "otlp",
                "OTEL_METRIC_EXPORT_INTERVAL", "1000",
                "OTEL_EXPORTER_OTLP_ENDPOINT", openTelemetryCollectorContainer.getOtlpEndpoint(),
                "OTEL_SERVICE_NAME", brokerOtelServiceName
        );

        var proxyOtelServiceName = clusterName + "-proxy";
        var proxyCollectorProps = Map.of(
                "OTEL_SDK_DISABLED", "false",
                "OTEL_METRICS_EXPORTER", "otlp",
                "OTEL_METRIC_EXPORT_INTERVAL", "1000",
                "OTEL_EXPORTER_OTLP_ENDPOINT", openTelemetryCollectorContainer.getOtlpEndpoint(),
                "OTEL_SERVICE_NAME", proxyOtelServiceName
        );

        var functionWorkerServiceNameSuffix = PulsarTestBase.randomName();
        var functionWorkerCollectorProps = Map.of(
                "OTEL_SDK_DISABLED", "false",
                "OTEL_METRICS_EXPORTER", "otlp",
                "OTEL_METRIC_EXPORT_INTERVAL", "1000",
                "OTEL_EXPORTER_OTLP_ENDPOINT", openTelemetryCollectorContainer.getOtlpEndpoint(),
                "OTEL_SERVICE_NAME", functionWorkerServiceNameSuffix
        );

        var spec = PulsarClusterSpec.builder()
                .clusterName(clusterName)
                .numBookies(1)
                .numBrokers(1)
                .brokerEnvs(brokerCollectorProps)
                .numProxies(1)
                .proxyEnvs(proxyCollectorProps)
                .externalService("otel-collector", openTelemetryCollectorContainer)
                .functionWorkerEnv(functionWorkerServiceNameSuffix, functionWorkerCollectorProps)
                .build();
        @Cleanup("stop")
        var pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();
        // pulsarCluster.setupFunctionWorkers(functionWorkerServiceNameSuffix, FunctionRuntimeType.PROCESS, 1);

        Awaitility.waitAtMost(180, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            var metricName = "queueSize_ratio"; // Sent automatically by the OpenTelemetry SDK.
            var metrics = openTelemetryCollectorContainer.getMetricsClient().getMetrics();
            var brokerMetrics = metrics.findByNameAndLabels(metricName, Pair.of("job", brokerOtelServiceName));
            var proxyMetrics = metrics.findByNameAndLabels(metricName, Pair.of("job", proxyOtelServiceName));
            var functionWorkerMetrics =
                    metrics.findByNameAndLabels(metricName, Pair.of("job", functionWorkerServiceNameSuffix));
            return !brokerMetrics.isEmpty() && !proxyMetrics.isEmpty();
        });
    }
}
