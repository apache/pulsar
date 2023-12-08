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
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class EnableProxyProtocolTest extends BrokerTestBase  {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setHaProxyProtocolEnabled(true);
        super.baseSetup();
    }

    protected PulsarClient newPulsarClient(String url, int intervalInSecs) throws PulsarClientException {
        ClientBuilder clientBuilder =
                PulsarClient.builder()
                        .serviceUrl(url)
                        .statsInterval(intervalInSecs, TimeUnit.SECONDS);
        customizeNewPulsarClientBuilder(clientBuilder);
        return createNewPulsarClient(clientBuilder);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSimpleProduceAndConsume() throws Exception {
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

        // cleanup.
        org.apache.pulsar.broker.service.Consumer serverConsumer = pulsar.getBrokerService().getTopicReference(topicName)
                .get().getSubscription(subName).getConsumers().get(0);
        ((ServerCnx) serverConsumer.cnx()).close();
        consumer.close();
        producer.close();
        admin.topics().delete(topicName);
    }

    @Test
    public void testProxyProtocol() throws Exception {
        final String namespace = "prop/ns-abc";
        final String topicName = "persistent://" + namespace + "/testProxyProtocol";
        final String subName = "my-subscriber-name";

        // Create a client that injected the protocol implementation.
        ClientBuilderImpl clientBuilder = (ClientBuilderImpl) PulsarClient.builder().serviceUrl(lookupUrl.toString());
        PulsarClientImpl protocolClient = InjectedClientCnxClientBuilder.create(clientBuilder,
                (conf, eventLoopGroup) -> new ClientCnx(conf, eventLoopGroup) {
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        byte[] bs = "PROXY TCP4 198.51.100.22 203.0.113.7 35646 80\r\n".getBytes();
                        ctx.writeAndFlush(Unpooled.copiedBuffer(bs));
                        super.channelActive(ctx);
                    }
                });

        // Verify the addr can be handled correctly.
        testPubAndSub(topicName, subName, "198.51.100.22:35646", protocolClient);

        // cleanup.
        admin.topics().delete(topicName);
    }

    @Test(timeOut = 10000)
    public void testPubSubWhenSlowNetwork() throws Exception {
        final String namespace = "prop/ns-abc";
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/tp");
        final String subName = "my-subscriber-name";

        // Create a client that injected the protocol implementation.
        ClientBuilderImpl clientBuilder = (ClientBuilderImpl) PulsarClient.builder().serviceUrl(lookupUrl.toString());
        PulsarClientImpl protocolClient = InjectedClientCnxClientBuilder.create(clientBuilder,
                (conf, eventLoopGroup) -> new ClientCnx(conf, eventLoopGroup) {
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        Thread task = new Thread(() -> {
                            try {
                                byte[] bs1 = "PROXY".getBytes();
                                byte[] bs2 = " TCP4 198.51.100.22 203.0.113.7 35646 80\r\n".getBytes();
                                ctx.writeAndFlush(Unpooled.copiedBuffer(bs1));
                                Thread.sleep(100);
                                ctx.writeAndFlush(Unpooled.copiedBuffer(bs2));
                                super.channelActive(ctx);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
                        task.start();
                    }
                });

        // Verify the addr can be handled correctly.
        testPubAndSub(topicName, subName, "198.51.100.22:35646", protocolClient);

        // cleanup.
        admin.topics().delete(topicName);
    }

    private void testPubAndSub(String topicName, String subName, String expectedHostAndPort,
                               PulsarClientImpl pulsarClient) throws Exception {
        // Verify: subscribe
        org.apache.pulsar.client.api.Consumer<String> clientConsumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionName(subName).subscribe();
        org.apache.pulsar.broker.service.Consumer serverConsumer = pulsar.getBrokerService()
                .getTopicReference(topicName).get().getSubscription(subName).getConsumers().get(0);
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(serverConsumer.cnx().hasHAProxyMessage()));
        TopicStats topicStats = admin.topics().getStats(topicName);
        Assert.assertEquals(topicStats.getSubscriptions().size(), 1);
        SubscriptionStats subscriptionStats = topicStats.getSubscriptions().get(subName);
        Assert.assertEquals(subscriptionStats.getConsumers().size(), 1);
        Assert.assertEquals(subscriptionStats.getConsumers().get(0).getAddress(), expectedHostAndPort);

        // Verify: producer register.
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        TopicStats topicStats2 = admin.topics().getStats(topicName);
        Assert.assertEquals(topicStats2.getPublishers().size(), 1);
        Assert.assertEquals(topicStats2.getPublishers().get(0).getAddress(), expectedHostAndPort);

        // Verify: Pub & Sub
        producer.send("1");
        Message<String> msg = clientConsumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNotNull(msg);
        Assert.assertEquals(msg.getValue(), "1");
        clientConsumer.acknowledge(msg);

        // cleanup.
        ((ServerCnx) serverConsumer.cnx()).close();
        producer.close();
        clientConsumer.close();
    }
}
