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
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.net.ServiceURI;
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
    private PulsarClientImpl pulsarClientWithBinaryServiceUrlDisableBackoff;
    private PulsarClientImpl pulsarClientWithHttpServiceUrl;
    private PulsarClientImpl pulsarClientWithHttpServiceUrlDisableBackoff;
    private static final int BROKER_SERVICE_PORT = 6666;
    private static final int WEB_SERVICE_PORT = 8888;
    private static final int UNAVAILABLE_NODES = 20;
    private static final int TIMEOUT_SECONDS = 1;

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
        this.pulsarClientWithBinaryServiceUrlDisableBackoff =
                (PulsarClientImpl)
                        PulsarClient.builder()
                                .serviceUrl(binaryServiceUrlWithUnavailableNodes)
                                .serviceUrlRecoveryInitBackoffInterval(0, TimeUnit.MILLISECONDS)
                                .serviceUrlRecoveryMaxBackoffInterval(0, TimeUnit.MILLISECONDS)
                                .operationTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                .lookupTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                .build();
        this.pulsarClientWithHttpServiceUrlDisableBackoff =
                (PulsarClientImpl) PulsarClient.builder()
                        .serviceUrl(httpServiceUrlWithUnavailableNodes)
                        .serviceUrlRecoveryInitBackoffInterval(0, TimeUnit.MILLISECONDS)
                        .serviceUrlRecoveryMaxBackoffInterval(0, TimeUnit.MILLISECONDS)
                        .operationTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .lookupTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .build();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setBrokerServicePort(Optional.of(BROKER_SERVICE_PORT));
        this.conf.setWebServicePort(Optional.of(WEB_SERVICE_PORT));
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        clientBuilder.operationTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .lookupTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
        int createCount = 20;
        // 1. test binary service url
        // trigger unhealthy address removal by creating consumers and producers
        int successCount = creatConsumerAndProducers(pulsarClientWithBinaryServiceUrl, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");
        // all unavailable nodes should have been removed
        successCount = creatConsumerAndProducers(pulsarClientWithBinaryServiceUrl, createCount, topic);
        assertEquals(successCount, createCount,
                "Expected all subscription creations to succeed, but only " + successCount + " succeeded.");

        // test binary service url with disable backoff
        successCount = creatConsumerAndProducers(pulsarClientWithBinaryServiceUrlDisableBackoff, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");
        // no unavailable nodes should be removed since backoff is disabled
        successCount = creatConsumerAndProducers(pulsarClientWithBinaryServiceUrlDisableBackoff, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected all subscription creations to succeed, but only " + successCount + " succeeded.");

        // 2. test http service url
        // trigger unhealthy address removal by creating consumers and producers
        successCount = creatConsumerAndProducers(pulsarClientWithHttpServiceUrl, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");
        // all unavailable nodes should have been removed
        successCount = creatConsumerAndProducers(pulsarClientWithHttpServiceUrl, createCount, topic);
        assertEquals(successCount, createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");

        // test http service url with disable backoff
        successCount = creatConsumerAndProducers(pulsarClientWithHttpServiceUrlDisableBackoff, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");
        // no unavailable nodes should be removed since backoff is disabled
        successCount = creatConsumerAndProducers(pulsarClientWithHttpServiceUrlDisableBackoff, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");
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
        doTestServiceUrlResolve(pulsarClientWithBinaryServiceUrl,
                "pulsar+ssl://host1:6651,host2:6651,127.0.0.1:" + BROKER_SERVICE_PORT,
                InetSocketAddress.createUnresolved("127.0.0.1", BROKER_SERVICE_PORT), true);
        doTestServiceUrlResolve(pulsarClientWithHttpServiceUrl,
                "http://host1:6651,host2:6651,127.0.0.1:" + WEB_SERVICE_PORT,
                InetSocketAddress.createUnresolved("127.0.0.1", WEB_SERVICE_PORT), true);

        doTestServiceUrlResolve(pulsarClientWithBinaryServiceUrlDisableBackoff,
                "pulsar+ssl://host1:6651,host2:6651,127.0.0.1:" + BROKER_SERVICE_PORT,
                InetSocketAddress.createUnresolved("127.0.0.1", BROKER_SERVICE_PORT), false);
        doTestServiceUrlResolve(pulsarClientWithHttpServiceUrlDisableBackoff,
                "http://host1:6651,host2:6651,127.0.0.1:" + WEB_SERVICE_PORT,
                InetSocketAddress.createUnresolved("127.0.0.1", WEB_SERVICE_PORT), false);
    }

    private void doTestServiceUrlResolve(PulsarClientImpl pulsarClient, String serviceUrl,
                                         InetSocketAddress healthyAddress, boolean enableBackoff)
            throws Exception {
        LookupService resolver = pulsarClient.getLookup();
        resolver.updateServiceUrl(serviceUrl);
        assertEquals(serviceUrl, resolver.getServiceUrl());

        ServiceURI uri;
        try {
            uri = ServiceURI.create(serviceUrl);
        } catch (IllegalArgumentException iae) {
            log.error("Invalid service-url {} provided {}", serviceUrl, iae.getMessage(), iae);
            throw new PulsarClientException.InvalidServiceURL(iae);
        }
        String[] hosts = uri.getServiceHosts();
        Set<InetSocketAddress> originAllAddresses = new HashSet<>(hosts.length);
        for (String host : hosts) {
            String hostUrl = uri.getServiceScheme() + "://" + host;
            try {
                URI hostUri = new URI(hostUrl);
                originAllAddresses.add(InetSocketAddress.createUnresolved(hostUri.getHost(), hostUri.getPort()));
            } catch (URISyntaxException e) {
                log.error("Invalid host provided {}", hostUrl, e);
                throw new PulsarClientException.InvalidServiceURL(e);
            }
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(originAllAddresses.contains(resolver.resolveHost()));
        }
        String topic = "persistent://my-property/my-ns/topic" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        // Create consumers to trigger unhealthy address removal
        for (int i = 0; i < originAllAddresses.size(); i++) {
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
        expectedHealthyAddresses.add(healthyAddress);

        Set<InetSocketAddress> resolvedAddresses = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            if (enableBackoff) {
                assertTrue(expectedHealthyAddresses.contains(resolver.resolveHost()));
            } else {
                resolvedAddresses.add(resolver.resolveHost());
            }
        }

        if (!enableBackoff) {
            assertEquals(resolvedAddresses, originAllAddresses,
                    "Expected all addresses to be healthy, but found: " + resolvedAddresses);
        }

        if (enableBackoff) {
            // Wait for some time to allow the unhealthy addresses to recover
            Uninterruptibles.sleepUninterruptibly(60, java.util.concurrent.TimeUnit.SECONDS);
            String subName = "my-sub" + UUID.randomUUID();
            pulsarClient.newConsumer()
                    .subscriptionMode(SubscriptionMode.Durable)
                    .topic(topic).receiverQueueSize(1).subscriptionName(subName)
                    .subscribeAsync()
                    .thenAccept(Consumer::closeAsync);
            for (int i = 0; i < 10; i++) {
                resolvedAddresses.add(resolver.resolveHost());
            }
            assertEquals(resolvedAddresses, originAllAddresses);
        }
    }
}