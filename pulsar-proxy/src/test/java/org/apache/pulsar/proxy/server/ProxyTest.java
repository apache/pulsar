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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.mockito.Mockito.doReturn;

import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyTest extends MockedPulsarServiceBaseTest {

    private final String DUMMY_VALUE = "DUMMY_VALUE";

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(PortManager.nextFreePort());
        proxyConfig.setZookeeperServers(DUMMY_VALUE);
        proxyConfig.setConfigurationStoreServers(DUMMY_VALUE);

        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                                                            PulsarConfigurationLoader.convertFrom(proxyConfig))));
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
    public void testProducer() throws Exception {
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:" + proxyConfig.getServicePort())
                .build();
        Producer<byte[]> producer = client.newProducer().topic("persistent://sample/test/local/producer-topic")
                .create();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        client.close();
    }

    @Test
    public void testProducerConsumer() throws Exception {
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:" + proxyConfig.getServicePort())
                .build();
        Producer<byte[]> producer = client.newProducer()
            .topic("persistent://sample/test/local/producer-consumer-topic")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // Create a consumer directly attached to broker
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://sample/test/local/producer-consumer-topic").subscriptionName("my-sub").subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            checkNotNull(msg);
            consumer.acknowledge(msg);
        }

        Message<byte[]> msg = consumer.receive(0, TimeUnit.SECONDS);
        checkArgument(msg == null);

        consumer.close();
        client.close();
    }

    @Test
    public void testPartitions() throws Exception {
        admin.tenants().createTenant("sample", new TenantInfo());
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:" + proxyConfig.getServicePort())
                .build();
        admin.topics().createPartitionedTopic("persistent://sample/test/local/partitioned-topic", 2);

        Producer<byte[]> producer = client.newProducer()
            .topic("persistent://sample/test/local/partitioned-topic")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        // Create a consumer directly attached to broker
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://sample/test/local/partitioned-topic")
                .subscriptionName("my-sub").subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            checkNotNull(msg);
        }

        client.close();
    }

}
