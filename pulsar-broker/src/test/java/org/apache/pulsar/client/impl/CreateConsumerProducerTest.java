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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.google.common.util.concurrent.Uninterruptibles;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.tests.ThreadDumpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class CreateConsumerProducerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(CreateConsumerProducerTest.class);
    private String binaryServiceUrlWithUnavailableNodes;
    private String httpServiceUrlWithUnavailableNodes;
    private PulsarClientImpl pulsarClientWithBinaryServiceUrl;
    private PulsarClientImpl pulsarClientWithHttpServiceUrl;
    private static final int BROKER_SERVICE_PORT = 6666;
    private static final int WEB_SERVICE_PORT = 8888;
    private static final int UNAVAILABLE_NODES = 100;

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        // Create a Pulsar client with some unavailable nodes
        StringBuilder binaryServiceUrlBuilder = new StringBuilder(pulsar.getBrokerServiceUrl());
        StringBuilder httpServiceUrlBuilder = new StringBuilder(pulsar.getWebServiceAddress());
        for (int i = 0; i < UNAVAILABLE_NODES; i++) {
            binaryServiceUrlBuilder.append(",127.0.0.1:").append(ThreadLocalRandom.current().nextInt(100, 1000));
            httpServiceUrlBuilder.append(",127.0.0.1:").append(ThreadLocalRandom.current().nextInt(100, 1000));
        }
        this.binaryServiceUrlWithUnavailableNodes = binaryServiceUrlBuilder.toString();
        this.httpServiceUrlWithUnavailableNodes = httpServiceUrlBuilder.toString();
        this.pulsarClientWithBinaryServiceUrl =
                (PulsarClientImpl) newPulsarClient(binaryServiceUrlWithUnavailableNodes, 0);
        this.pulsarClientWithHttpServiceUrl = (PulsarClientImpl) newPulsarClient(
                httpServiceUrlWithUnavailableNodes, 0);
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setBrokerServicePort(Optional.of(BROKER_SERVICE_PORT));
        this.conf.setWebServicePort(Optional.of(WEB_SERVICE_PORT));
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        clientBuilder.operationTimeout(5, TimeUnit.SECONDS)
                .lookupTimeout(5, TimeUnit.SECONDS);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanupAfterMethod() throws Exception {
        try {
            pulsar.getConfiguration().setForceDeleteTenantAllowed(true);
            pulsar.getConfiguration().setForceDeleteNamespaceAllowed(true);

            for (String tenant : admin.tenants().getTenants()) {
                for (String namespace : admin.namespaces().getNamespaces(tenant)) {
                    deleteNamespaceWithRetry(namespace, true);
                }
                admin.tenants().deleteTenant(tenant, true);
            }

            for (String cluster : admin.clusters().getClusters()) {
                admin.clusters().deleteCluster(cluster);
            }

            pulsar.getConfiguration().setForceDeleteTenantAllowed(false);
            pulsar.getConfiguration().setForceDeleteNamespaceAllowed(false);
            super.producerBaseSetup();
        } catch (Exception | AssertionError e) {
            log.warn("Failed to clean up state. Restarting broker.", e);
            log.warn("Thread dump:\n{}", ThreadDumpUtil.buildThreadDiagnosticString());
            cleanup();
            setup();
        }
    }

    @DataProvider
    public static Object[][] variationsForExpectedPos() {
        return new Object[][]{
                // batching / start-inclusive / num-of-messages
                {true, true, 10},
                {true, false, 10},
                {false, true, 10},
                {false, false, 10},

                {true, true, 100},
                {true, false, 100},
                {false, true, 100},
                {false, false, 100},
        };
    }

    @DataProvider(name = "ackReceiptEnabled")
    public Object[][] ackReceiptEnabled() {
        return new Object[][]{{true}, {false}};
    }

    @DataProvider(name = "ackReceiptEnabledAndSubscriptionTypes")
    public Object[][] ackReceiptEnabledAndSubscriptionTypes() {
        return new Object[][]{
                {true, SubscriptionType.Shared},
                {true, SubscriptionType.Key_Shared},
                {false, SubscriptionType.Shared},
                {false, SubscriptionType.Key_Shared},
        };
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        if (pulsarClientWithBinaryServiceUrl != null) {
            pulsarClientWithBinaryServiceUrl.close();
        }
    }

    @Test
    public void testCreateConsumerProducerWithUnavailableBrokerNodes() throws Exception {
        pulsarClientWithBinaryServiceUrl.updateServiceUrl(binaryServiceUrlWithUnavailableNodes);
        pulsarClientWithHttpServiceUrl.updateServiceUrl(httpServiceUrlWithUnavailableNodes);
        String topic = "persistent://my-property/my-ns/topic" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        int createCount = 100;
        // 1. test binary service url
        // trigger unhealthy address removal by creating consumers and producers
        int successCount = creatConsumerAndProducers(pulsarClientWithBinaryServiceUrl, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");
        // all unavailable nodes should have been removed
        successCount = creatConsumerAndProducers(pulsarClientWithBinaryServiceUrl, createCount, topic);
        assertEquals(successCount, createCount,
                "Expected all subscription creations to succeed, but only " + successCount + " succeeded.");

        // 2. test http service url
        // trigger unhealthy address removal by creating consumers and producers
        successCount = creatConsumerAndProducers(pulsarClientWithHttpServiceUrl, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");
        // all unavailable nodes should have been removed
        successCount = creatConsumerAndProducers(pulsarClientWithHttpServiceUrl, createCount, topic);
        assertEquals(successCount, createCount,
                "Expected all subscription creations to succeed, but only " + successCount + " succeeded.");
    }

    private int creatConsumerAndProducers(PulsarClientImpl pulsarClient, int createCount, String topic) {
        int successCount = 0;
        for (int i = 0; i < createCount; i++) {
            String subName = "my-sub" + UUID.randomUUID();
            try {
                Consumer<byte[]> consumer = pulsarClient.newConsumer()
                        .subscriptionMode(SubscriptionMode.Durable)
                        .topic(topic).receiverQueueSize(1).subscriptionName(subName)
                        .subscribe();
                consumer.close();
                Producer<byte[]> producer = pulsarClient.newProducer()
                        .topic(topic)
                        .create();
                producer.close();
                successCount++;
            } catch (Exception e) {
                log.warn("Failed to create consumer and producer {} for topic {}: {}", subName, topic, e.getMessage());
            }
        }
        return successCount;
    }

    @Test
    public void testServiceUrlHealthCheck() throws Exception {
        doTestServiceUrlHealthCheck(pulsarClientWithBinaryServiceUrl,
                "pulsar+ssl://host1:6651,host2:6651,127.0.0.1:" + BROKER_SERVICE_PORT, BROKER_SERVICE_PORT);
        doTestServiceUrlHealthCheck(pulsarClientWithHttpServiceUrl,
                "http://host1:6651,host2:6651,127.0.0.1:" + WEB_SERVICE_PORT, WEB_SERVICE_PORT);
    }

    private void doTestServiceUrlHealthCheck(PulsarClientImpl pulsarClient, String serviceUrl, int healthyPort)
            throws Exception {
        LookupService resolver = pulsarClient.getLookup();
        resolver.updateServiceUrl(serviceUrl);
        assertEquals(serviceUrl, resolver.getServiceUrl());

        Set<InetSocketAddress> expectedAddresses = new HashSet<>();
        expectedAddresses.add(InetSocketAddress.createUnresolved("host1", 6651));
        expectedAddresses.add(InetSocketAddress.createUnresolved("host2", 6651));
        expectedAddresses.add(InetSocketAddress.createUnresolved("127.0.0.1", healthyPort));

        for (int i = 0; i < 10; i++) {
            assertTrue(expectedAddresses.contains(resolver.resolveHost()));
        }
        String topic = "persistent://my-property/my-ns/topic" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        // Create consumers to trigger unhealthy address removal
        for (int i = 0; i < 10; i++) {
            String subName = "my-sub" + UUID.randomUUID();
            pulsarClient.newConsumer()
                    .subscriptionMode(SubscriptionMode.Durable)
                    .topic(topic).receiverQueueSize(1).subscriptionName(subName)
                    .subscribeAsync()
                    .thenAccept(Consumer::closeAsync);
        }
        Uninterruptibles.sleepUninterruptibly(10, java.util.concurrent.TimeUnit.SECONDS);
        // check if the unhealthy address is removed
        Set<InetSocketAddress> expectedHealthyAddresses = new HashSet<>();
        expectedHealthyAddresses.add(InetSocketAddress.createUnresolved("127.0.0.1", healthyPort));

        for (int i = 0; i < 100; i++) {
            assertTrue(expectedHealthyAddresses.contains(resolver.resolveHost()));
        }
    }
}