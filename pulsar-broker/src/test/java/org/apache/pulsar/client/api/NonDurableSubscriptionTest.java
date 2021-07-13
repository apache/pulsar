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
package org.apache.pulsar.client.api;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentSubscription;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "broker-api")
@Slf4j
public class NonDurableSubscriptionTest  extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setSubscriptionExpirationTimeMinutes(1);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
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

    @Test(timeOut = 10000)
    public void testDeleteInactiveNonPersistentSubscription() throws Exception {
        final String topic = "non-persistent://my-property/my-ns/topic-" + UUID.randomUUID();
        final String subName = "my-subscriber";
        admin.topics().createNonPartitionedTopic(topic);
        // 1 setup consumer
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionName(subName).subscribe();
        // 3 due to the existence of consumers, subscriptions will not be cleaned up
        NonPersistentTopic nonPersistentTopic = (NonPersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        NonPersistentSubscription nonPersistentSubscription = (NonPersistentSubscription) nonPersistentTopic.getSubscription(subName);
        assertNotNull(nonPersistentSubscription);
        assertNotNull(nonPersistentSubscription.getDispatcher());
        assertTrue(nonPersistentSubscription.getDispatcher().isConsumerConnected());
        assertFalse(nonPersistentSubscription.isReplicated());

        nonPersistentTopic.checkInactiveSubscriptions();
        Thread.sleep(500);
        nonPersistentSubscription = (NonPersistentSubscription) nonPersistentTopic.getSubscription(subName);
        assertNotNull(nonPersistentSubscription);
        // remove consumer and wait for cleanup
        consumer.close();
        Thread.sleep(500);

        //change last active time to 5 minutes ago
        Field f = NonPersistentSubscription.class.getDeclaredField("lastActive");
        f.setAccessible(true);
        f.set(nonPersistentTopic.getSubscription(subName), System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5));
        //without consumers and last active time is 5 minutes ago, subscription should be cleaned up
        nonPersistentTopic.checkInactiveSubscriptions();
        Thread.sleep(500);
        nonPersistentSubscription = (NonPersistentSubscription) nonPersistentTopic.getSubscription(subName);
        assertNull(nonPersistentSubscription);

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
}
