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

package org.apache.pulsar.proxy.server;

import static org.mockito.Mockito.doReturn;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BinaryProtoLookupService;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.SocatContainer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProxyStuckConnectionTest extends MockedPulsarServiceBaseTest {

    private static final Logger log = LoggerFactory.getLogger(ProxyStuckConnectionTest.class);

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig;
    private Authentication proxyClientAuthentication;
    private SocatContainer socatContainer;

    private String brokerServiceUriSocat;
    private volatile boolean useBrokerSocatProxy = true;

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        useBrokerSocatProxy = true;
        internalSetup();

        int brokerPort = pulsar.getBrokerService().getListenPort().get();
        Testcontainers.exposeHostPorts(brokerPort);

        socatContainer = new SocatContainer();
        socatContainer.withTarget(brokerPort, "host.testcontainers.internal", brokerPort);
        socatContainer.start();
        brokerServiceUriSocat = "pulsar://" + socatContainer.getHost() + ":" + socatContainer.getMappedPort(brokerPort);

        proxyConfig = new ProxyConfiguration();
        proxyConfig.setServicePort(Optional.ofNullable(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setClusterName(configClusterName);

        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        startProxyService();
        // use the same port for subsequent restarts
        proxyConfig.setServicePort(proxyService.getListenPort());
    }

    private void startProxyService() throws Exception {
        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig)), proxyClientAuthentication) {
            @Override
            protected LookupProxyHandler newLookupProxyHandler(ProxyConnection proxyConnection) {
                return new TestLookupProxyHandler(this, proxyConnection);
            }
        });
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeper))).when(proxyService).createLocalMetadataStore();
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(proxyService)
                .createConfigurationMetadataStore();
        proxyService.start();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        if (proxyService != null) {
            proxyService.close();
        }
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
        if (socatContainer != null) {
            socatContainer.close();
        }
    }

    private final class TestLookupProxyHandler extends LookupProxyHandler {
        public TestLookupProxyHandler(ProxyService proxy, ProxyConnection proxyConnection) {
            super(proxy, proxyConnection);
        }

        @Override
        protected String resolveBrokerUrlFromLookupDataResult(BinaryProtoLookupService.LookupDataResult r) {
            return useBrokerSocatProxy ? brokerServiceUriSocat : super.resolveBrokerUrlFromLookupDataResult(r);
        }
    }

    @Test
    public void testKeySharedStickyWithStuckConnection() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                // keep alive is set to 2 seconds to detect the dead connection on the client side
                // the main focus of the test is to verify that the broker and proxy doesn't get stuck forever
                // when there's a hanging connection from the proxy to the broker and that it doesn't cause issues
                // such as hash range conflicts
                .keepAliveInterval(2, TimeUnit.SECONDS)
                .build();
        String topicName = BrokerTestUtil.newUniqueName("persistent://sample/test/local/test-topic");

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName("test-subscription")
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(KeySharedPolicy.stickyHashRange()
                        .ranges(Range.of(0, 65535)))
                .receiverQueueSize(2)
                .isAckReceiptEnabled(true)
                .subscribe();

        Set<String> messages = new HashSet<>();

        try (Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .accessMode(ProducerAccessMode.Shared)
                .enableBatching(false)
                .create()) {
            for (int i = 0; i < 10; i++) {
                String message = "test" + i;
                producer.newMessage().value(message.getBytes())
                        .key("A")
                        .send();
                messages.add(message);
            }
        }

        int counter = 0;
        while (true) {
            counter++;
            Message<byte[]> msg = consumer.receive(15, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            String msgString = new String(msg.getData());
            log.info("Received message {}", msgString);
            try {
                consumer.acknowledge(msg);
            } catch (PulsarClientException e) {
                log.error("Failed to ack message {}", msgString, e);
            }
            messages.remove(msgString);
            log.info("Remaining messages {}", messages.size());
            if (messages.size() == 0) {
                break;
            }
            if (counter == 2) {
                log.info(
                        "Pausing connection between proxy and broker and making further connections from proxy "
                                + "directly to broker");
                useBrokerSocatProxy = false;
                socatContainer.getDockerClient().pauseContainerCmd(socatContainer.getContainerId()).exec();
            }
        }

        Assert.assertEquals(messages.size(), 0);
        Assert.assertEquals(consumer.receive(1, TimeUnit.MILLISECONDS), null);
    }
}
