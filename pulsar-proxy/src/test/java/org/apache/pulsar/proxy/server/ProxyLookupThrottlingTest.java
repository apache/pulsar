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
import static org.testng.Assert.assertTrue;

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
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProxyLookupThrottlingTest extends MockedPulsarServiceBaseTest {

    private final int NUM_CONCURRENT_LOOKUP = 3;
    private final int NUM_CONCURRENT_INBOUND_CONNECTION = 5;
    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeMethod(alwaysRun = true)
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setZookeeperServers(DUMMY_VALUE);
        proxyConfig.setConfigurationStoreServers(GLOBAL_DUMMY_VALUE);
        proxyConfig.setMaxConcurrentLookupRequests(NUM_CONCURRENT_LOOKUP);
        proxyConfig.setMaxConcurrentInboundConnections(NUM_CONCURRENT_INBOUND_CONNECTION);

        AuthenticationService authenticationService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));
        proxyService = Mockito.spy(new ProxyService(proxyConfig, authenticationService));
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(proxyService).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(proxyService).createConfigurationMetadataStore();

        proxyService.start();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        if (proxyService != null) {
            proxyService.close();
        }
    }

    @Test(groups = "quarantine")
    public void testLookup() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrl())
                .connectionsPerBroker(5)
                .ioThreads(5)
                .operationTimeout(1000, TimeUnit.MILLISECONDS)
                .build();
        assertTrue(proxyService.getLookupRequestSemaphore().tryAcquire());
        assertTrue(proxyService.getLookupRequestSemaphore().tryAcquire());

        @Cleanup
        Producer<byte[]> producer1 = client.newProducer(Schema.BYTES).topic("persistent://sample/test/local/producer-topic")
                .create();
        assertTrue(proxyService.getLookupRequestSemaphore().tryAcquire());
        try {
            @Cleanup
            Producer<byte[]> producer2 = client.newProducer(Schema.BYTES).topic("persistent://sample/test/local/producer-topic")
                    .create();
            Assert.fail("Should have failed since can't acquire LookupRequestSemaphore");
        } catch (Exception ex) {
            // Ignore
        }
        Assert.assertEquals(LookupProxyHandler.rejectedPartitionsMetadataRequests.get(), 5.0d);
        proxyService.getLookupRequestSemaphore().release();
        try {
            @Cleanup
            Producer<byte[]> producer3 = client.newProducer(Schema.BYTES).topic("persistent://sample/test/local/producer-topic")
                    .create();
        } catch (Exception ex) {
            Assert.fail("Should not have failed since can acquire LookupRequestSemaphore");
        }

        Assert.assertEquals(LookupProxyHandler.rejectedPartitionsMetadataRequests.get(), 5.0d);
    }
}
