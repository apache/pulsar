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

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyConnectionThrottlingTest extends MockedPulsarServiceBaseTest {

    private final String DUMMY_VALUE = "DUMMY_VALUE";
    private final int NUM_CONCURRENT_LOOKUP = 3;
    private final int NUM_CONCURRENT_INBOUND_CONNECTION = 1;
    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(PortManager.nextFreePort());
        proxyConfig.setZookeeperServers(DUMMY_VALUE);
        proxyConfig.setGlobalZookeeperServers(DUMMY_VALUE);
        proxyConfig.setMaxConcurrentLookupRequests(NUM_CONCURRENT_LOOKUP);
        proxyConfig.setMaxConcurrentInboundConnections(NUM_CONCURRENT_INBOUND_CONNECTION);
        proxyService = Mockito.spy(new ProxyService(proxyConfig));
        doReturn(mockZooKeeperClientFactory).when(proxyService).getZooKeeperClientFactory();

        proxyService.start();
    }

    @Override
    @AfterClass
    protected void cleanup() throws Exception {
        internalCleanup();
        proxyService.close();
    }

    @Test
    public void testInboundConnection() throws Exception {
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:" + proxyConfig.getServicePort())
                .build();
        Producer<byte[]> producer1;

        try {
            producer1 = client.newProducer().topic("persistent://sample/test/local/producer-topic-1").create();
            producer1.send("Message 1".getBytes());
            Assert.fail("Should have failed since max num of connections is 1 and the lookup connection pool is a persistent connection.");
        } catch (Exception ex) {
            // OK
        }

        client.close();
    }

}
