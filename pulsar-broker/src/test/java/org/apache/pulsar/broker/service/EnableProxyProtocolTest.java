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
package org.apache.pulsar.broker.service;

import io.netty.buffer.Unpooled;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Test(groups = "broker")
public class EnableProxyProtocolTest extends BrokerTestBase  {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setHaProxyProtocolEnabled(true);
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSimpleProduceAndConsume() throws PulsarClientException {
        final String namespace = "prop/ns-abc";
        final String topicName = "persistent://" + namespace + "/testSimpleProduceAndConsume";
        final String subName = "my-subscriber-name";
        final int messages = 100;

        @Cleanup
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscribe();

        @Cleanup
        org.apache.pulsar.client.api.Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        for (int i = 0; i < messages; i++) {
            producer.send(("Message-" + i).getBytes());
        }

        int received = 0;
        for (int i = 0; i < messages; i++) {
            consumer.acknowledge(consumer.receive());
            received++;
        }

        Assert.assertEquals(received, messages);
    }

    @Test
    public void testProxyProtocol() throws PulsarClientException, ExecutionException, InterruptedException, PulsarAdminException {
        final String namespace = "prop/ns-abc";
        final String topicName = "persistent://" + namespace + "/testProxyProtocol";
        final String subName = "my-subscriber-name";
        PulsarClientImpl client = (PulsarClientImpl) pulsarClient;
        CompletableFuture<ClientCnx> cnx = client.getCnxPool().getConnection(InetSocketAddress.createUnresolved("localhost", pulsar.getBrokerListenPort().get()));
        // Simulate the proxy protcol message
        cnx.get().ctx().channel().writeAndFlush(Unpooled.copiedBuffer("PROXY TCP4 198.51.100.22 203.0.113.7 35646 80\r\n".getBytes()));
        pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscribe();
        org.apache.pulsar.broker.service.Consumer c = pulsar.getBrokerService().getTopicReference(topicName).get().getSubscription(subName).getConsumers().get(0);
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(c.cnx().hasHAProxyMessage()));
        TopicStats topicStats = admin.topics().getStats(topicName);
        Assert.assertEquals(topicStats.getSubscriptions().size(), 1);
        SubscriptionStats subscriptionStats = topicStats.getSubscriptions().get(subName);
        Assert.assertEquals(subscriptionStats.getConsumers().size(), 1);
        Assert.assertEquals(subscriptionStats.getConsumers().get(0).getAddress(), "198.51.100.22:35646");

        pulsarClient.newProducer().topic(topicName).create();
        topicStats = admin.topics().getStats(topicName);
        Assert.assertEquals(topicStats.getPublishers().size(), 1);
        Assert.assertEquals(topicStats.getPublishers().get(0).getAddress(), "198.51.100.22:35646");
    }
}
