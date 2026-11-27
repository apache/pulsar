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

import static org.apache.pulsar.tests.integration.containers.PulsarContainer.BROKER_HTTP_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarTestBase;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.testng.annotations.Test;

/**
 * Checks the pending lookup and topic load gauges of brokers that run in JVMs of their own, as they do in a
 * deployment.
 */
public class BrokerPendingRequestGaugesTest {

    private static final List<String> PENDING_REQUEST_GAUGES =
            List.of("pulsar_broker_lookup_pending_requests", "pulsar_broker_topic_load_pending_requests");

    @Test(timeOut = 360_000)
    public void testEachBrokerExportsItsPendingRequestGaugesOnce() throws Exception {
        var spec = PulsarClusterSpec.builder()
                .clusterName(newClusterName())
                .numBrokers(2)
                .build();
        @Cleanup("stop")
        var pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();
        Collection<BrokerContainer> brokers = pulsarCluster.getBrokers();

        // Lookups and topic loads that the start of the brokers caused may still be pending at first
        waitAtMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
            for (BrokerContainer broker : brokers) {
                var metrics = scrape(broker);
                for (String gauge : PENDING_REQUEST_GAUGES) {
                    assertThat(metrics.getNameToDataPoints().get(gauge))
                            .as("samples of %s in the metrics of the idle broker %s", gauge, broker.getHostname())
                            .singleElement()
                            .extracting(sample -> sample.value, InstanceOfAssertFactories.DOUBLE)
                            .isZero();
                }
            }
        });

        // Creating producers on new topics makes the brokers run lookups and topic loads. The brokers are scraped
        // once the creation has started, until the producers are created. A scrape does not have to overlap a
        // pending lookup or topic load.
        @Cleanup
        var client = PulsarClient.builder().serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build();
        List<CompletableFuture<Producer<byte[]>>> producers = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            producers.add(client.newProducer()
                    .topic("persistent://public/default/" + PulsarTestBase.randomName())
                    .createAsync());
        }
        CompletableFuture<Void> producersCreated = FutureUtil.waitForAll(producers);
        do {
            for (BrokerContainer broker : brokers) {
                var metrics = scrape(broker);
                for (String gauge : PENDING_REQUEST_GAUGES) {
                    assertThat(metrics.getNameToDataPoints().get(gauge))
                            .as("samples of %s in the metrics of the broker %s after the creation of the producers"
                                    + " has started", gauge, broker.getHostname())
                            .singleElement()
                            .extracting(sample -> sample.value, InstanceOfAssertFactories.DOUBLE)
                            .isNotNegative();
                }
            }
        } while (!producersCreated.isDone());
        producersCreated.get(60, TimeUnit.SECONDS);
    }

    private static PrometheusMetricsClient.Metrics scrape(BrokerContainer broker) {
        return new PrometheusMetricsClient(broker.getHost(), broker.getMappedPort(BROKER_HTTP_PORT)).getMetrics();
    }

    // Container names are "<cluster>-<hostname>", and Docker's DNS gives no reverse lookup answer for a name longer
    // than a DNS label (63 characters), so the cluster name is kept short
    private static String newClusterName() {
        return "pendingRequestGauges-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
