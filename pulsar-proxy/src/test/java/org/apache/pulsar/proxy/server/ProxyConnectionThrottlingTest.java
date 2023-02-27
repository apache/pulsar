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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.limiter.ConnectionController;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class ProxyConnectionThrottlingTest extends MockedPulsarServiceBaseTest {

    private final int NUM_CONCURRENT_LOOKUP = 3;
    private final int NUM_CONCURRENT_INBOUND_CONNECTION = 4;
    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setMaxConcurrentLookupRequests(NUM_CONCURRENT_LOOKUP);
        proxyConfig.setMaxConcurrentInboundConnections(NUM_CONCURRENT_INBOUND_CONNECTION);
        proxyConfig.setMaxConcurrentInboundConnectionsPerIp(NUM_CONCURRENT_INBOUND_CONNECTION);
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
        log.info("Creating producer 1");
        PulsarClient client1 = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrl())
                .operationTimeout(1000, TimeUnit.MILLISECONDS)
                .build();

        Producer<byte[]> producer1 = client1.newProducer(Schema.BYTES)
                .topic("persistent://sample/test/local/producer-topic-1").create();

        log.info("Creating producer 2");
        PulsarClient client2 = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrl())
                .operationTimeout(1000, TimeUnit.MILLISECONDS)
                .build();

        Producer<byte[]> producer2 = client2.newProducer(Schema.BYTES)
                .topic("persistent://sample/test/local/producer-topic-1").create();

        log.info("Creating producer 3");
        @Cleanup
        PulsarClient client3 = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrl())
                .operationTimeout(1000, TimeUnit.MILLISECONDS)
                .build();
        try {
            Producer<byte[]> producer3 = client3.newProducer(Schema.BYTES)
                    .topic("persistent://sample/test/local/producer-topic-1").create();
            producer3.send("Message 1".getBytes());
            Assert.fail("Should have failed since max num of connections is 2 and the first" +
                    " producer used them all up - one for discovery and other for producing.");
        } catch (Exception ex) {
            // OK
        }
        Awaitility.await().untilAsserted(() ->{
            Assert.assertEquals(ConnectionController.DefaultConnectionController.getTotalConnectionNum(), 4);
        });
        Assert.assertEquals(ConnectionController.DefaultConnectionController.getConnections().size(), 1);
        Set<String> keys = ConnectionController.DefaultConnectionController.getConnections().keySet();
        for (String key : keys) {
            Assert.assertEquals((int)ConnectionController.DefaultConnectionController
                    .getConnections().get(key).toInteger(), 4);
        }
        Assert.assertEquals(ProxyService.ACTIVE_CONNECTIONS.get(), 4.0d);

        client1.close();

        Awaitility.await().untilAsserted(() ->{
            Assert.assertEquals(ConnectionController.DefaultConnectionController.getTotalConnectionNum(), 2);
        });
        Assert.assertEquals(ConnectionController.DefaultConnectionController.getConnections().size(), 1);
        keys = ConnectionController.DefaultConnectionController.getConnections().keySet();
        for (String key : keys) {
            Assert.assertEquals((int)ConnectionController.DefaultConnectionController
                    .getConnections().get(key).toInteger(), 2);
        }
        Assert.assertEquals(ProxyService.ACTIVE_CONNECTIONS.get(), 2.0d);

        client2.close();
        Awaitility.await().untilAsserted(() ->{
            Assert.assertEquals(ConnectionController.DefaultConnectionController.getTotalConnectionNum(), 0);
        });
        Assert.assertEquals(ConnectionController.DefaultConnectionController.getConnections().size(), 0);
        Assert.assertEquals(ProxyService.ACTIVE_CONNECTIONS.get(), 0.0d);
    }
}
