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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class URLRegexLookupProxyHandlerTest extends MockedPulsarServiceBaseTest {

    private Authentication proxyClientAuthentication;

    protected ProxyService proxyService;
    protected ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);

        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
            PulsarConfigurationLoader.convertFrom(proxyConfig)), proxyClientAuthentication));
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(proxyService).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(proxyService).createConfigurationMetadataStore();

        proxyService.start();
    }

    @Override
    protected void cleanup() throws Exception {
        internalCleanup();

        proxyService.close();
    }

    @Test
    void testMatchingRegex() throws Exception {
        ProxyConfiguration redirectProxyConfig = new ProxyConfiguration();
        redirectProxyConfig.setServicePort(Optional.of(0));
        redirectProxyConfig.setBrokerProxyAllowedTargetPorts("*");
        redirectProxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        redirectProxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        redirectProxyConfig.setLookupHandler("org.apache.pulsar.proxy.server.URLRegexLookupProxyHandler");
        redirectProxyConfig.getProperties().setProperty("urlRegexLookupProxyHandlerRegex", "pulsar:\\/\\/(\\w+):\\d+");
        redirectProxyConfig.getProperties()
            .setProperty("urlRegexLookupProxyHandlerReplacement", proxyService.getServiceUrl());

        @Cleanup
        ProxyService redirectProxyService = Mockito.spy(new ProxyService(redirectProxyConfig, new AuthenticationService(
            PulsarConfigurationLoader.convertFrom(redirectProxyConfig)), proxyClientAuthentication));
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(redirectProxyService).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(redirectProxyService).createConfigurationMetadataStore();

        redirectProxyService.start();

        // Check that the target proxy is not connected to any broker at the moment
        assertEquals(proxyService.getClientCnxs().size(), 0);

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(redirectProxyService.getServiceUrl())
            .lookupTimeout(5, TimeUnit.SECONDS)
            .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
            .topic("persistent://sample/test/local/producer-consumer-topic")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // Create a consumer directly attached to broker
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic("persistent://sample/test/local/producer-consumer-topic").subscriptionName("my-sub").subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            assertNotNull(msg);
            consumer.acknowledge(msg);
        }

        Message<byte[]> msg = consumer.receive(0, TimeUnit.SECONDS);
        assertNull(msg);

        // Check that the target proxy now has connections to the broker
        assertTrue(proxyService.getClientCnxs().size() > 0);
    }

    @Test
    void testNotMatchingRegex() throws Exception {
        ProxyConfiguration redirectProxyConfig = new ProxyConfiguration();
        redirectProxyConfig.setServicePort(Optional.of(0));
        redirectProxyConfig.setBrokerProxyAllowedTargetPorts("*");
        redirectProxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        redirectProxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        redirectProxyConfig.setLookupHandler("org.apache.pulsar.proxy.server.URLRegexLookupProxyHandler");
        redirectProxyConfig.getProperties().setProperty("urlRegexLookupProxyHandlerRegex", "invalid");
        redirectProxyConfig.getProperties().setProperty("urlRegexLookupProxyHandlerReplacement", proxyService.getServiceUrl());

        @Cleanup
        ProxyService redirectProxyService = Mockito.spy(new ProxyService(redirectProxyConfig, new AuthenticationService(
            PulsarConfigurationLoader.convertFrom(redirectProxyConfig)), proxyClientAuthentication));
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(redirectProxyService).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(redirectProxyService).createConfigurationMetadataStore();

        redirectProxyService.start();

        // Check that the target proxy is not connected to any broker at the moment
        assertEquals(proxyService.getClientCnxs().size(), 0);

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(redirectProxyService.getServiceUrl())
            .lookupTimeout(5, TimeUnit.SECONDS)
            .build();

        assertThrows(PulsarClientException.LookupException.class, () -> client.newProducer(Schema.BYTES)
            .topic("persistent://sample/test/local/producer-consumer-topic")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create());
    }

}
