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
package org.apache.pulsar.broker.service.persistent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;

import org.apache.bookkeeper.client.BKException;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class DelayedDeliveryTest extends ProducerConsumerBase {

    @Override
    @BeforeClass
    public void setup() throws Exception {
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setDelayedDeliveryTickTimeMillis(1024);
        conf.setDispatcherReadFailureBackoffInitialTimeInMs(1000);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testDelayedDelivery() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("testNegativeAcks");

        @Cleanup
        Consumer<String> failoverConsumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("failover-sub")
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        @Cleanup
        Consumer<String> sharedConsumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("shared-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .value("msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .sendAsync();
        }

        producer.flush();

        // Failover consumer will receive the messages immediately while
        // the shared consumer will get them after the delay
        Message<String> msg = sharedConsumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);

        for (int i = 0; i < 10; i++) {
            msg = failoverConsumer.receive(100, TimeUnit.MILLISECONDS);
            assertEquals(msg.getValue(), "msg-" + i);
        }

        Set<String> receivedMsgs = new TreeSet<>();
        for (int i = 0; i < 10; i++) {
            msg = sharedConsumer.receive(10, TimeUnit.SECONDS);
            receivedMsgs.add(msg.getValue());
        }

        assertEquals(receivedMsgs.size(), 10);
        for (int i = 0; i < 10; i++) {
            assertTrue(receivedMsgs.contains("msg-" + i));
        }
    }

    @Test
    public void testInterleavedMessages()
            throws Exception {
        String topic = BrokerTestUtil.newUniqueName("testInterleavedMessages");

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("shared-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 10; i++) {
            // Publish 1 message without delay and 1 with delay
            producer.newMessage()
                    .value("immediate-msg-" + i)
                    .sendAsync();

            producer.newMessage()
                    .value("delayed-msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .sendAsync();
        }

        producer.flush();

        // Failover consumer will receive the messages immediately while
        // the shared consumer will get them after the delay
        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertEquals(msg.getValue(), "immediate-msg-" + i);
            consumer.acknowledge(msg);
        }

        // Delayed messages might not come in same exact order
        Set<String> delayedMessages = new TreeSet<>();
        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive(10, TimeUnit.SECONDS);
            delayedMessages.add(msg.getValue());
            consumer.acknowledge(msg);
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(delayedMessages.contains("delayed-msg-" + i));
        }
    }

    @Test
    public void testEverythingFilteredInMultipleReads()
            throws Exception {
        String topic = BrokerTestUtil.newUniqueName("testEverythingFilteredInMultipleReads");

        @Cleanup
        Consumer<String> sharedConsumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("shared-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .value("msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .send();
        }

        Thread.sleep(1000);

        // Write a 2nd batch of messages
        for (int i = 10; i < 20; i++) {
            producer.newMessage()
                    .value("msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .send();
        }

        Message<String> msg = sharedConsumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);

        Set<String> receivedMsgs = new TreeSet<>();
        for (int i = 0; i < 20; i++) {
            msg = sharedConsumer.receive(10, TimeUnit.SECONDS);
            receivedMsgs.add(msg.getValue());
        }

        assertEquals(receivedMsgs.size(), 20);
        for (int i = 0; i < 10; i++) {
            assertTrue(receivedMsgs.contains("msg-" + i));
        }
    }

    @Test
    public void testDelayedDeliveryWithMultipleConcurrentReadEntries()
            throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://public/default/testDelayedDelivery");

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("shared-sub")
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(1) // Use small prefecthing to simulate the multiple read batches
                .subscribe();

        // Simulate race condition with high frequency of calls to dispatcher.readMoreEntries()
        PersistentDispatcherMultipleConsumers d = (PersistentDispatcherMultipleConsumers) ((PersistentTopic) pulsar
                .getBrokerService().getTopicReference(topic).get()).getSubscription("shared-sub").getDispatcher();
        Thread t = new Thread(() -> {
            while (true) {
                synchronized (d) {
                    d.readMoreEntries();
                }

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        t.start();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        final int N = 1000;

        for (int i = 0; i < N; i++) {
            producer.newMessage()
                    .value("msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .sendAsync();
        }

        producer.flush();

        Message<String> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);

        Set<String> receivedMsgs = new TreeSet<>();
        for (int i = 0; i < N; i++) {
            msg = consumer.receive(10, TimeUnit.SECONDS);
            receivedMsgs.add(msg.getValue());
        }

        assertEquals(receivedMsgs.size(), N);
        for (int i = 0; i < N; i++) {
            assertTrue(receivedMsgs.contains("msg-" + i));
        }
        t.interrupt();
    }

    @Test
    public void testOrderingDispatch() throws PulsarClientException {
        String topic = BrokerTestUtil.newUniqueName("persistent://public/default/testOrderingDispatch");

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("shared-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        final int N = 1000;

        for (int i = 0; i < N; i++) {
            producer.newMessage()
                    .value("msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .send();
        }

        List<Message<String>> receives = new ArrayList<>(N);
        for (int i = 0; i < N; i++) {
            Message<String> received = consumer.receive();
            receives.add(received);
            consumer.acknowledge(received);
        }

        assertEquals(receives.size(), N);

        for (int i = 0; i < N; i++) {
            if (i < N - 1) {
                assertTrue(receives.get(i).getMessageId().compareTo(receives.get(i + 1).getMessageId()) < 0);
            }
        }
    }

    @Test(timeOut = 20000)
    public void testEnableAndDisableTopicDelayedDelivery() throws Exception {
        String topicName = "persistent://public/default/topic-" + UUID.randomUUID();

        admin.topics().createPartitionedTopic(topicName, 3);
        pulsarClient.newProducer().topic(topicName).create().close();
        assertNull(admin.topics().getDelayedDeliveryPolicy(topicName));
        DelayedDeliveryPolicies delayedDeliveryPolicies = DelayedDeliveryPolicies.builder()
                .tickTime(2000)
                .active(false)
                .build();
        admin.topics().setDelayedDeliveryPolicy(topicName, delayedDeliveryPolicies);
        //wait for update
        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            if (admin.topics().getDelayedDeliveryPolicy(topicName) != null) {
                break;
            }
        }

        assertFalse(admin.topics().getDelayedDeliveryPolicy(topicName).isActive());
        assertEquals(2000, admin.topics().getDelayedDeliveryPolicy(topicName).getTickTime());

        admin.topics().removeDelayedDeliveryPolicy(topicName);
        //wait for update
        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            if (admin.topics().getDelayedDeliveryPolicy(topicName) == null) {
                break;
            }
        }
        assertNull(admin.topics().getDelayedDeliveryPolicy(topicName));
    }

    @Test(timeOut = 20000)
    public void testEnableTopicDelayedDelivery() throws Exception {
        final String topicName = "persistent://public/default/test" + UUID.randomUUID().toString();

        admin.topics().createPartitionedTopic(topicName, 3);
        pulsarClient.newProducer().topic(topicName).create().close();
        assertNull(admin.topics().getDelayedDeliveryPolicy(topicName));
        //1 Set topic policy
        DelayedDeliveryPolicies delayedDeliveryPolicies = DelayedDeliveryPolicies.builder()
                .tickTime(2000)
                .active(true)
                .build();
        admin.topics().setDelayedDeliveryPolicy(topicName, delayedDeliveryPolicies);
        //wait for update
        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            if (admin.topics().getDelayedDeliveryPolicy(topicName) != null) {
                break;
            }
        }
        //2 Setup consumer and producer
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub" + System.currentTimeMillis())
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName).create();
        //3 Send delay message
        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .value("delayed-msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .sendAsync();
        }
        producer.flush();

        //4 There will be no message in the first 3 seconds
        assertNull(consumer.receive(3, TimeUnit.SECONDS));

        Set<String> delayedMessages = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive(4, TimeUnit.SECONDS);
            delayedMessages.add(msg.getValue());
            consumer.acknowledge(msg);
        }
        for (int i = 0; i < 10; i++) {
            assertTrue(delayedMessages.contains("delayed-msg-" + i));
        }
        //5 Disable delayed delivery
        delayedDeliveryPolicies = DelayedDeliveryPolicies.builder()
                .tickTime(2000)
                .active(false)
                .build();
        admin.topics().setDelayedDeliveryPolicy(topicName, delayedDeliveryPolicies);
        //wait for update
        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            if (!admin.topics().getDelayedDeliveryPolicy(topicName).isActive()) {
                break;
            }
        }
        producer.newMessage().value("disabled-msg").deliverAfter(5, TimeUnit.SECONDS).send();
        //6 Delay deliver is disabled, so we can receive message immediately
        Message<String> msg = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(msg);
        consumer.acknowledge(msg);
        //7 Set a very long tick time, so that trackDelayedDelivery will fail. we can receive msg immediately.
        delayedDeliveryPolicies = DelayedDeliveryPolicies.builder()
                .tickTime(Integer.MAX_VALUE)
                .active(true)
                .build();
        admin.topics().setDelayedDeliveryPolicy(topicName, delayedDeliveryPolicies);
        //wait for update
        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            if (admin.topics().getDelayedDeliveryPolicy(topicName).isActive()) {
                break;
            }
        }
        producer.newMessage().value("long-tick-msg").deliverAfter(5, TimeUnit.SECONDS).send();
        msg = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(msg);
        consumer.acknowledge(msg);
        //8 remove topic policy, it will use namespace level policy
        admin.topics().removeDelayedDeliveryPolicy(topicName);
        //wait for update
        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            if (admin.topics().getDelayedDeliveryPolicy(topicName) == null) {
                break;
            }
        }
        producer.newMessage().value("long-tick-msg").deliverAfter(2, TimeUnit.SECONDS).send();
        msg = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(msg);
        msg = consumer.receive(3, TimeUnit.SECONDS);
        assertNotNull(msg);
    }

    @Test
    public void testClearDelayedMessagesWhenClearBacklog() throws PulsarClientException, PulsarAdminException {
        final String topic = "persistent://public/default/testClearDelayedMessagesWhenClearBacklog-" + UUID.randomUUID().toString();
        final String subName = "my-sub";
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic).create();

        final int messages = 100;
        for (int i = 0; i < messages; i++) {
            producer.newMessage().deliverAfter(1, TimeUnit.HOURS).value("Delayed Message - " + i).send();
        }

        Dispatcher dispatcher = pulsar.getBrokerService().getTopicReference(topic).get().getSubscription(subName).getDispatcher();
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(dispatcher.getNumberOfDelayedMessages(), messages));

        admin.topics().skipAllMessages(topic, subName);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(dispatcher.getNumberOfDelayedMessages(), 0));
    }

    @Test
    public void testDelayedDeliveryWithAllConsumersDisconnecting() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://public/default/testDelays");

        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        producer.newMessage()
                    .value("msg")
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .send();

        Dispatcher dispatcher = pulsar.getBrokerService().getTopicReference(topic).get().getSubscription("sub").getDispatcher();
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(dispatcher.getNumberOfDelayedMessages(), 1));

        c1.close();

        // Attach a new consumer. Since there are no consumers connected, this will trigger the cursor rewind
        @Cleanup
        Consumer<String> c2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(1)
                .subscribe();

        Awaitility.await().untilAsserted(() -> Assert.assertEquals(dispatcher.getNumberOfDelayedMessages(), 1));

        Message<String> msg = c2.receive(10, TimeUnit.SECONDS);
        assertNotNull(msg);

        // No more messages
        msg = c2.receive(1, TimeUnit.SECONDS);
        assertNull(msg);

        Awaitility.await().untilAsserted(() -> Assert.assertEquals(dispatcher.getNumberOfDelayedMessages(), 0));
    }

    @Test
    public void testInterleavedMessagesOnKeySharedSubscription() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("testInterleavedMessagesOnKeySharedSubscription");

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("key-shared-sub")
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        Random random = new Random(0);
        for (int i = 0; i < 10; i++) {
            // Publish 1 message without delay and 1 with delay
            producer.newMessage()
                    .value("immediate-msg-" + i)
                    .sendAsync();

            int delayMillis = 1000 + random.nextInt(1000);
            producer.newMessage()
                    .value("delayed-msg-" + i)
                    .deliverAfter(delayMillis, TimeUnit.MILLISECONDS)
                    .sendAsync();
            Thread.sleep(1000);
        }

        producer.flush();

        Set<String> receivedMessages = new HashSet<>();

        while (receivedMessages.size() < 20) {
            Message<String> msg = consumer.receive(3, TimeUnit.SECONDS);
            receivedMessages.add(msg.getValue());
            consumer.acknowledge(msg);
        }
    }

    @Test
    public void testDispatcherReadFailure() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("testDispatcherReadFailure");

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("shared-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .value("msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .sendAsync();
        }

        producer.flush();

        Message<String> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);

        // Inject failure in BK read
        this.mockBookKeeper.failNow(BKException.Code.ReadException);

        Set<String> receivedMsgs = new TreeSet<>();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(10, TimeUnit.SECONDS);
            receivedMsgs.add(msg.getValue());
        }

        assertEquals(receivedMsgs.size(), 10);
        for (int i = 0; i < 10; i++) {
            assertTrue(receivedMsgs.contains("msg-" + i));
        }
    }

}
