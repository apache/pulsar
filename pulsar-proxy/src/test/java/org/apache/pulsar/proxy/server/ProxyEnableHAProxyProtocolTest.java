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

import io.netty.buffer.Unpooled;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.doReturn;

public class ProxyEnableHAProxyProtocolTest extends MockedPulsarServiceBaseTest {

    private static final Logger log = LoggerFactory.getLogger(ProxyEnableHAProxyProtocolTest.class);

    private final String DUMMY_VALUE = "DUMMY_VALUE";

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        conf.setProxyProtocolEnabled(true);
        internalSetup();

        proxyConfig.setServicePort(Optional.ofNullable(0));
        proxyConfig.setZookeeperServers(DUMMY_VALUE);
        proxyConfig.setConfigurationStoreServers(DUMMY_VALUE);
        proxyConfig.setProxyProtocolEnabled(true);

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
    public void testSimpleProduceAndConsume() throws PulsarClientException, PulsarAdminException {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();

        final String topicName = "persistent://sample/test/local/testSimpleProduceAndConsume";
        final String subName = "my-subscriber-name";
        final int messages = 100;

        @Cleanup
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = client.newConsumer().topic(topicName).subscriptionName(subName)
                .subscribe();

        @Cleanup
        org.apache.pulsar.client.api.Producer<byte[]> producer = client.newProducer().topic(topicName).create();
        for (int i = 0; i < messages; i++) {
            producer.send(("Message-" + i).getBytes());
        }

        int received = 0;
        for (int i = 0; i < messages; i++) {
            consumer.acknowledge(consumer.receive());
            received++;
        }

        Assert.assertEquals(received, messages);

        TopicStats topicStats = admin.topics().getStats(topicName);
        Assert.assertEquals(topicStats.subscriptions.size(), 1);
        SubscriptionStats subscriptionStats = topicStats.subscriptions.get(subName);
        Assert.assertEquals(subscriptionStats.consumers.size(), 1);
        Assert.assertEquals(subscriptionStats.consumers.get(0).getAddress(),
                ((ConsumerImpl) consumer).getClientCnx().ctx().channel().localAddress().toString().replaceFirst("/", ""));

        topicStats = admin.topics().getStats(topicName);
        Assert.assertEquals(topicStats.publishers.size(), 1);
        Assert.assertEquals(topicStats.publishers.get(0).getAddress(),
                ((ProducerImpl) producer).getClientCnx().ctx().channel().localAddress().toString().replaceFirst("/", ""));
    }

    @Test
    public void testExternalProxy() throws PulsarClientException, ExecutionException, InterruptedException, PulsarAdminException {
        final String topicName = "persistent://sample/test/local/testExternalProxy";
        final String subName = "my-subscriber-name";
        @Cleanup
        PulsarClient proxyClient = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl()).connectionsPerBroker(1)
                .build();
        PulsarClientImpl client = (PulsarClientImpl) proxyClient;
        CompletableFuture<ClientCnx> cnx = client.getCnxPool().getConnection(
                InetSocketAddress.createUnresolved("localhost", proxyService.getListenPort().get()),InetSocketAddress.createUnresolved("localhost", pulsar.getBrokerService().getListenPort().get()));
        // Simulate the proxy protcol message
        cnx.get().ctx().channel().writeAndFlush(Unpooled.copiedBuffer("PROXY TCP4 198.51.100.22 203.0.113.7 35646 80\r\n".getBytes()));

        proxyClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscribe();

        TopicStats topicStats = admin.topics().getStats(topicName);
        Assert.assertEquals(topicStats.subscriptions.size(), 1);
        SubscriptionStats subscriptionStats = topicStats.subscriptions.get(subName);
        Assert.assertEquals(subscriptionStats.consumers.size(), 1);
        Assert.assertEquals(subscriptionStats.consumers.get(0).getAddress(), "198.51.100.22:35646");

        proxyClient.newProducer().topic(topicName).create();
        topicStats = admin.topics().getStats(topicName);
        Assert.assertEquals(topicStats.publishers.size(), 1);
        Assert.assertEquals(topicStats.publishers.get(0).getAddress(), "198.51.100.22:35646");
    }

}
