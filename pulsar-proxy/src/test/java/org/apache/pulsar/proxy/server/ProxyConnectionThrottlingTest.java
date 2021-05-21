/**
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

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyConnectionThrottlingTest extends MockedPulsarServiceBaseTest {

    private final int NUM_CONCURRENT_LOOKUP = 3;
    private final int NUM_CONCURRENT_INBOUND_CONNECTION = 2;
    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setZookeeperServers(DUMMY_VALUE);
        proxyConfig.setConfigurationStoreServers(GLOBAL_DUMMY_VALUE);
        proxyConfig.setMaxConcurrentLookupRequests(NUM_CONCURRENT_LOOKUP);
        proxyConfig.setMaxConcurrentInboundConnections(NUM_CONCURRENT_INBOUND_CONNECTION);
        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                                                            PulsarConfigurationLoader.convertFrom(proxyConfig))));
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(proxyService).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(proxyService).createConfigurationMetadataStore();

        proxyService.start();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        proxyService.close();
    }

    @Test
    public void testInboundConnection() throws Exception {
        LOG.info("Creating producer 1");
        @Cleanup
        PulsarClient client1 = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrl())
                .operationTimeout(1000, TimeUnit.MILLISECONDS)
                .build();

        @Cleanup
        Producer<byte[]> producer1 = client1.newProducer(Schema.BYTES).topic("persistent://sample/test/local/producer-topic-1").create();

        LOG.info("Creating producer 2");
        @Cleanup
        PulsarClient client2 = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrl())
                .operationTimeout(1000, TimeUnit.MILLISECONDS)
                .build();

        Assert.assertEquals(ProxyService.rejectedConnections.get(), 0.0d);
        try {
            @Cleanup
            Producer<byte[]> producer2 = client2.newProducer(Schema.BYTES).topic("persistent://sample/test/local/producer-topic-1").create();
            producer2.send("Message 1".getBytes());
            Assert.fail("Should have failed since max num of connections is 2 and the first producer used them all up - one for discovery and other for producing.");
        } catch (Exception ex) {
            // OK
        }
        // should add retry count since retry every 100ms and operation timeout is set to 1000ms
        Assert.assertEquals(ProxyService.rejectedConnections.get(), 5.0d);
    }

    private static final Logger LOG = LoggerFactory.getLogger(ProxyConnectionThrottlingTest.class);
}
