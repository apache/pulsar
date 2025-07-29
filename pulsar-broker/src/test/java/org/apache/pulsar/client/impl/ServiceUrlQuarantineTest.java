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

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.common.net.ServiceURI;
import org.apache.pulsar.common.util.PortManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ServiceUrlQuarantineTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ServiceUrlQuarantineTest.class);
    private String binaryServiceUrlWithUnavailableNodes;
    private String httpServiceUrlWithUnavailableNodes;
    private PulsarClientImpl pulsarClientWithBinaryServiceUrl;
    private PulsarClientImpl pulsarClientWithBinaryServiceUrlDisableQuarantine;
    private PulsarClientImpl pulsarClientWithHttpServiceUrl;
    private PulsarClientImpl pulsarClientWithHttpServiceUrlDisableQuarantine;
    private int brokerServicePort;
    private int webServicePort;
    private final Set<Integer> lockedFreePortSet = new HashSet<>();
    private static final int UNAVAILABLE_NODES = 20;
    private static final int TIMEOUT_MS = 500;

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        this.brokerServicePort = nextLockedFreePort();
        this.webServicePort = nextLockedFreePort();
        super.internalSetup();
        super.producerBaseSetup();
        // Create a Pulsar client with some unavailable nodes
        StringBuilder binaryServiceUrlBuilder = new StringBuilder(pulsar.getBrokerServiceUrl());
        StringBuilder httpServiceUrlBuilder = new StringBuilder(pulsar.getWebServiceAddress());
        for (int i = 0; i < UNAVAILABLE_NODES; i++) {
            binaryServiceUrlBuilder.append(",127.0.0.1:").append(nextLockedFreePort());
            httpServiceUrlBuilder.append(",127.0.0.1:").append(nextLockedFreePort());
        }
        this.binaryServiceUrlWithUnavailableNodes = binaryServiceUrlBuilder.toString();
        this.httpServiceUrlWithUnavailableNodes = httpServiceUrlBuilder.toString();
        this.pulsarClientWithBinaryServiceUrl =
                (PulsarClientImpl) newPulsarClient(binaryServiceUrlWithUnavailableNodes, 0);
        this.pulsarClientWithHttpServiceUrl = (PulsarClientImpl) newPulsarClient(
                httpServiceUrlWithUnavailableNodes, 0);
        this.pulsarClientWithBinaryServiceUrlDisableQuarantine =
                (PulsarClientImpl)
                        PulsarClient.builder()
                                .serviceUrl(binaryServiceUrlWithUnavailableNodes)
                                .serviceUrlQuarantineInitDuration(0, TimeUnit.MILLISECONDS)
                                .serviceUrlQuarantineMaxDuration(0, TimeUnit.MILLISECONDS)
                                .operationTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                                .lookupTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                                .build();
        this.pulsarClientWithHttpServiceUrlDisableQuarantine =
                (PulsarClientImpl) PulsarClient.builder()
                        .serviceUrl(httpServiceUrlWithUnavailableNodes)
                        .serviceUrlQuarantineInitDuration(0, TimeUnit.MILLISECONDS)
                        .serviceUrlQuarantineMaxDuration(0, TimeUnit.MILLISECONDS)
                        .operationTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .lookupTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .build();
    }

    private int nextLockedFreePort() {
        int newLockedFreePort = PortManager.nextLockedFreePort();
        this.lockedFreePortSet.add(newLockedFreePort);
        return newLockedFreePort;
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setBrokerServicePort(Optional.of(brokerServicePort));
        this.conf.setWebServicePort(Optional.of(webServicePort));
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        clientBuilder.operationTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .lookupTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }


    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        if (pulsarClientWithBinaryServiceUrl != null) {
            pulsarClientWithBinaryServiceUrl.close();
        }
        if (pulsarClientWithBinaryServiceUrlDisableQuarantine != null) {
            pulsarClientWithBinaryServiceUrlDisableQuarantine.close();
        }
        if (pulsarClientWithHttpServiceUrl != null) {
            pulsarClientWithHttpServiceUrl.close();
        }
        if (pulsarClientWithHttpServiceUrlDisableQuarantine != null) {
            pulsarClientWithHttpServiceUrlDisableQuarantine.close();
        }
        for (Integer port : lockedFreePortSet) {
            PortManager.releaseLockedPort(port);
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
        int successCount = createConsumerAndProducers(pulsarClientWithBinaryServiceUrl, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");
        // all unavailable nodes should have been removed
        successCount = createConsumerAndProducers(pulsarClientWithBinaryServiceUrl, createCount, topic);
        assertEquals(successCount, createCount,
                "Expected all subscription creations to succeed, but only " + successCount + " succeeded.");

        // test binary service url with disable quarantine
        successCount =
                createConsumerAndProducers(pulsarClientWithBinaryServiceUrlDisableQuarantine, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");
        // no unavailable nodes should be removed since backoff is disabled
        successCount =
                createConsumerAndProducers(pulsarClientWithBinaryServiceUrlDisableQuarantine, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected all subscription creations to succeed, but only " + successCount + " succeeded.");

        // 2. test http service url
        // trigger unhealthy address removal by creating consumers and producers
        successCount = createConsumerAndProducers(pulsarClientWithHttpServiceUrl, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");
        // all unavailable nodes should have been removed
        successCount = createConsumerAndProducers(pulsarClientWithHttpServiceUrl, createCount, topic);
        assertEquals(successCount, createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");

        // test http service url with disable quarantine
        successCount = createConsumerAndProducers(pulsarClientWithHttpServiceUrlDisableQuarantine, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");
        // no unavailable nodes should be removed since backoff is disabled
        successCount = createConsumerAndProducers(pulsarClientWithHttpServiceUrlDisableQuarantine, createCount, topic);
        assertTrue(successCount < createCount,
                "Expected some creations to fail due to unavailable nodes, but all succeeded.");
    }

    private int createConsumerAndProducers(PulsarClientImpl pulsarClient, int createCount, String topic) {
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

    @Test(invocationCount = 10)
    public void testServiceUrlHealthCheck() throws Exception {
        doTestServiceUrlResolve(pulsarClientWithBinaryServiceUrl,
                "pulsar+ssl://host1:6651,host2:6651,127.0.0.1:" + brokerServicePort,
                InetSocketAddress.createUnresolved("127.0.0.1", brokerServicePort), true);
        doTestServiceUrlResolve(pulsarClientWithHttpServiceUrl,
                "http://host1:6651,host2:6651,127.0.0.1:" + webServicePort,
                InetSocketAddress.createUnresolved("127.0.0.1", webServicePort), true);

        doTestServiceUrlResolve(pulsarClientWithBinaryServiceUrlDisableQuarantine,
                "pulsar+ssl://host1:6651,host2:6651,127.0.0.1:" + brokerServicePort,
                InetSocketAddress.createUnresolved("127.0.0.1", brokerServicePort), false);
        doTestServiceUrlResolve(pulsarClientWithHttpServiceUrlDisableQuarantine,
                "http://host1:6651,host2:6651,127.0.0.1:" + webServicePort,
                InetSocketAddress.createUnresolved("127.0.0.1", webServicePort), false);
    }

    private void doTestServiceUrlResolve(PulsarClientImpl pulsarClient, String serviceUrl,
                                         InetSocketAddress healthyAddress, boolean enableQuarantine)
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
            try {
                Consumer<byte[]> consumer = pulsarClient.newConsumer()
                        .subscriptionMode(SubscriptionMode.Durable)
                        .topic(topic).receiverQueueSize(1).subscriptionName(subName)
                        .subscribe();
                consumer.closeAsync();
            } catch (PulsarClientException e) {
                log.warn("Failed to create consumer {} for topic {}: {}", subName, topic, e.getMessage());
            }
        }
        // check if the unhealthy address is removed
        Set<InetSocketAddress> expectedHealthyAddresses = new HashSet<>();
        expectedHealthyAddresses.add(healthyAddress);

        Set<InetSocketAddress> resolvedAddresses = new HashSet<>();
        for (int i = 0; i < hosts.length; i++) {
            if (enableQuarantine) {
                assertThat(expectedHealthyAddresses).contains(resolver.resolveHost());
            } else {
                resolvedAddresses.add(resolver.resolveHost());
            }
        }

        if (!enableQuarantine) {
            assertEquals(resolvedAddresses, originAllAddresses,
                    "Expected all addresses to be healthy, but found: " + resolvedAddresses);
        }
    }
}