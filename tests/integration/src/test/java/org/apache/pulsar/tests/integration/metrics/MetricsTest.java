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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.OpenTelemetryCollectorContainer;
import org.apache.pulsar.tests.integration.containers.PrometheusContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Slf4j
public class MetricsTest {

    @Test
    public void testBrokerMetrics() throws Exception {
        var clusterName = MetricsTest.class.getSimpleName() + UUID.randomUUID();
        var spec = PulsarClusterSpec.builder()
                .numBookies(1)
                .numBrokers(1)
                .numProxies(1)
                .externalService("otel-collector", new OpenTelemetryCollectorContainer(clusterName))
                .externalService("prometheus", new PrometheusContainer(clusterName))
                .build();
        @Cleanup("stop")
        var pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        var start = LocalDateTime.now();
        Awaitility.waitAtMost(Duration.ofMinutes(30)).pollDelay(Duration.ofSeconds(30)).until(() -> {
            var duration = Duration.between(LocalDateTime.now(), start);
            log.info("Time since start: {}", duration);
            return false;
        });
    }
}
