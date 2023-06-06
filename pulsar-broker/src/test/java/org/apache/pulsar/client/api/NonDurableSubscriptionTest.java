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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.PulsarChannelInitializer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.api.proto.CommandFlow;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
@Slf4j
public class NonDurableSubscriptionTest  extends ProducerConsumerBase {

    private final AtomicInteger numFlow = new AtomicInteger(0);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setSystemTopicEnabled(false);
        conf.setSubscriptionExpirationTimeMinutes(1);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected BrokerService customizeNewBrokerService(BrokerService brokerService) {
        brokerService.setPulsarChannelInitializerFactory((_pulsar, opts) -> {
            return new PulsarChannelInitializer(_pulsar, opts) {
                @Override
                protected ServerCnx newServerCnx(PulsarService pulsar, String listenerName) throws Exception {
                    return new ServerCnx(pulsar) {

                        @Override
                        protected void handleFlow(CommandFlow flow) {
                            super.handleFlow(flow);
                            numFlow.incrementAndGet();
                        }
                    };
                }
            };
        });
        return brokerService;
    }

    @Test
    public void testNonDurableSubscription() throws Exception {
        String topicName = "persistent://my-property/my-ns/nonDurable-topic1";
        // 1 setup producer、consumer
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .readCompacted(true)
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("my-nonDurable-subscriber")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        // 2 send message
        int messageNum = 10;
        for (int i = 0; i < messageNum; i++) {
            producer.send("message" + i);
        }
        // 3 receive the first 5 messages
        for (int i = 0; i < 5; i++) {
            Message<String> message = consumer.receive();
            assertNotNull(message);
            Assert.assertEquals(message.getValue(), "message" + i);
            consumer.acknowledge(message);
        }
        // 4 trigger reconnect
        ((ConsumerImpl)consumer).getClientCnx().close();
        // 5 for non-durable we are going to restart from the next entry
        for (int i = 5; i < messageNum; i++) {
            Message<String> message = consumer.receive();
            assertNotNull(message);
            Assert.assertEquals(message.getValue(), "message" + i);
        }

    }

    @Test
    public void testSameSubscriptionNameForDurableAndNonDurableSubscription() throws Exception {
        String topicName = "persistent://my-property/my-ns/same-sub-name-topic";
        // first test for create Durable subscription and then create NonDurable subscription
        // 1. create a subscription with SubscriptionMode.Durable
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .readCompacted(true)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("mix-subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        consumer.close();

        // 2. create a subscription with SubscriptionMode.NonDurable
        try {
            @Cleanup
            Consumer<String> consumerNoDurable =
                    pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                    .readCompacted(true)
                    .subscriptionMode(SubscriptionMode.NonDurable)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionName("mix-subscription")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
            Assert.fail("should fail since durable subscription already exist.");
        } catch (PulsarClientException.NotAllowedException exception) {
            //ignore
        }

        // second test for create NonDurable subscription and then create Durable subscription
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .create();
        // 1. create a subscription with SubscriptionMode.NonDurable
        @Cleanup
        Consumer<String> noDurableConsumer =
                pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                        .subscriptionMode(SubscriptionMode.NonDurable)
                        .subscriptionType(SubscriptionType.Shared)
                        .subscriptionName("mix-subscription-01")
                        .receiverQueueSize(1)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe();

        // 2. create a subscription with SubscriptionMode.Durable
        try {
            @Cleanup
            Consumer<String> durableConsumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                    .subscriptionMode(SubscriptionMode.Durable)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName("mix-subscription-01")
                    .receiverQueueSize(1)
                    .startMessageIdInclusive()
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
        } catch (PulsarClientException.NotAllowedException exception) {
            //ignore
        }
    }

    @DataProvider(name = "subscriptionTypes")
    public static Object[][] subscriptionTypes() {
        Object[][] result = new Object[SubscriptionType.values().length][];
        int i = 0;
        for (SubscriptionType type : SubscriptionType.values()) {
            result[i++] = new Object[] {type};
        }
        return result;
    }

    @Test(dataProvider = "subscriptionTypes")
    public void testNonDurableSubscriptionRecovery(SubscriptionType subscriptionType) throws Exception {
        log.info("testing {}", subscriptionType);
        String topicName = "persistent://my-property/my-ns/nonDurable-sub-recorvery-"+subscriptionType;
        // 1 setup producer、consumer
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscriptionType(subscriptionType)
                .subscriptionName("my-nonDurable-subscriber")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        // 2 send messages
        int messageNum = 15;
        for (int i = 0; i < messageNum; i++) {
            producer.send("message" + i);
        }
        // 3 receive the first 5 messages
        for (int i = 0; i < 5; i++) {
            Message<String> message = consumer.receive();
            assertNotNull(message);
            Assert.assertEquals(message.getValue(), "message" + i);
            consumer.acknowledge(message);
        }
        // 4 trigger reconnect
        ((ConsumerImpl)consumer).getClientCnx().close();

        // 5 for non-durable we are going to restart from the next entry
        for (int i = 5; i < 10; i++) {
            Message<String> message = consumer.receive();
            assertNotNull(message);
            Assert.assertEquals(message.getValue(), "message" + i);
        }

        // 6 restart broker
        restartBroker();

        // 7 for non-durable we are going to restart from the next entry
        for (int i = 10; i < messageNum; i++) {
            Message<String> message = consumer.receive();
            assertNotNull(message);
            Assert.assertEquals(message.getValue(), "message" + i);
        }

    }

    @Test
    public void testFlowCountForMultiTopics() throws Exception {
        String topicName = "persistent://my-property/my-ns/test-flow-count";
        int numPartitions = 5;
        admin.topics().createPartitionedTopic(topicName, numPartitions);
        numFlow.set(0);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-nonDurable-subscriber")
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscribe();
        consumer.receive(1, TimeUnit.SECONDS);
        consumer.close();

        assertEquals(numFlow.get(), numPartitions);
    }
}
