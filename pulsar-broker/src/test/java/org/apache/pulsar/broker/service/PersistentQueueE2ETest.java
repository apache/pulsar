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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

@Test(groups = "broker")
public class PersistentQueueE2ETest extends BrokerTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentQueueE2ETest.class);

    private void deleteTopic(String topicName) {
        try {
            admin.topics().delete(topicName);
        } catch (PulsarAdminException pae) {
            // it is okay to get exception if it is cleaning up.
        }
    }

    @Test
    public void testSimpleConsumerEvents() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/shared-topic1";
        final String subName = "sub1";
        final int numMsgs = 100;

        // 1. two consumers on the same subscription
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared).subscribe();

        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer2 = newPulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared).subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        assertNotNull(topicRef);
        assertNotNull(subRef);

        // 2. validate basic dispatcher state
        assertTrue(subRef.getDispatcher().isConsumerConnected());
        assertEquals(subRef.getDispatcher().getType(), SubType.Shared);

        List<CompletableFuture<MessageId>> futures = Lists.newArrayListWithCapacity(numMsgs * 2);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        for (int i = 0; i < numMsgs * 2; i++) {
            String message = "my-message-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();

        rolloverPerIntervalStats();

        assertEquals(subRef.getNumberOfEntriesInBacklog(false), numMsgs * 2);
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        // both consumers will together consumer all messages
        Message<byte[]> msg;
        Consumer<byte[]> c = consumer1;
        while (true) {
            try {
                msg = c.receive(1, TimeUnit.SECONDS);
                c.acknowledge(msg);
            } catch (PulsarClientException e) {
                if (c.equals(consumer1)) {
                    consumer1.close();
                    c = consumer2;
                } else {
                    break;
                }
            }
        }

        rolloverPerIntervalStats();

        // 3. messages deleted on individual acks
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(subRef.getNumberOfEntriesInBacklog(false), 0);

        // 4. shared consumer unsubscribe not allowed
        try {
            consumer1.unsubscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            // ok
        }

        // 5. cumulative acks disabled
        consumer1.close();
        producer.send("message".getBytes());
        msg = consumer2.receive();

        try {
            consumer2.acknowledgeCumulative(msg);
            fail("Should fail");
        } catch (PulsarClientException e) {
            assertTrue(e instanceof PulsarClientException.InvalidConfigurationException);
        }

        // 6. unsubscribe allowed if this is the lone consumer
        try {
            consumer2.unsubscribe();
        } catch (PulsarClientException e) {
            fail("Should not fail");
        }

        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        subRef = topicRef.getSubscription(subName);
        assertNull(subRef);

        producer.close();
        consumer2.close();
        newPulsarClient.close();

        deleteTopic(topicName);
    }

    @Test
    public void testReplayOnConsumerDisconnect() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/shared-topic3";
        final String subName = "sub3";
        final int numMsgs = 100;

        final List<String> messagesProduced = Lists.newArrayListWithCapacity(numMsgs);
        final List<String> messagesConsumed = new BlockingArrayQueue<>(numMsgs);

        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared).messageListener((consumer, msg) -> {
                    try {
                        consumer.acknowledge(msg);
                        messagesConsumed.add(new String(msg.getData()));
                    } catch (Exception e) {
                        fail("Should not fail");
                    }
                }).subscribe();

        // consumer2 does not ack messages
        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer2 = newPulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared).messageListener((consumer, msg) -> {
                    // do nothing
                }).subscribe();

        List<CompletableFuture<MessageId>> futures = Lists.newArrayListWithCapacity(numMsgs * 2);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        for (int i = 0; i < numMsgs; i++) {
            String message = "msg-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
            messagesProduced.add(message);
        }
        FutureUtil.waitForAll(futures).get();
        producer.close();

        consumer2.close();

        for (int n = 0; n < 10 && messagesConsumed.size() < numMsgs; n++) {
            Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        }

        // 1. consumer1 gets all messages
        assertTrue(CollectionUtils.subtract(messagesProduced, messagesConsumed).isEmpty());

        consumer1.close();
        newPulsarClient.close();

        deleteTopic(topicName);
    }

    // this test is good to have to see the distribution, but every now and then it gets slightly different than the
    // expected numbers. keeping this disabled to not break the build, but nevertheless this gives good insight into
    // how the round robin distribution algorithm is behaving
    @Test(enabled = false)
    public void testRoundRobinBatchDistribution() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/shared-topic5";
        final String subName = "sub5";
        final int numMsgs = 137; /* some random number different than default batch size of 100 */

        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);
        final AtomicInteger counter3 = new AtomicInteger(0);

        final CountDownLatch latch = new CountDownLatch(numMsgs * 3);

        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .receiverQueueSize(10).subscriptionType(SubscriptionType.Shared);

        Consumer<byte[]> consumer1 = consumerBuilder.clone().messageListener((consumer, msg) -> {
            try {
                counter1.incrementAndGet();
                consumer.acknowledge(msg);
                latch.countDown();
            } catch (Exception e) {
                fail("Should not fail");
            }
        }).subscribe();

        Consumer<byte[]> consumer2 = consumerBuilder.clone().messageListener((consumer, msg) -> {
            try {
                counter2.incrementAndGet();
                consumer.acknowledge(msg);
                latch.countDown();
            } catch (Exception e) {
                fail("Should not fail");
            }
        }).subscribe();

        Consumer<byte[]> consumer3 = consumerBuilder.clone().messageListener((consumer, msg) -> {
            try {
                counter1.incrementAndGet();
                consumer.acknowledge(msg);
                latch.countDown();
            } catch (Exception e) {
                fail("Should not fail");
            }
        }).subscribe();

        List<CompletableFuture<MessageId>> futures = Lists.newArrayListWithCapacity(numMsgs);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        for (int i = 0; i < numMsgs * 3; i++) {
            String message = "msg-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();
        producer.close();

        latch.await(1, TimeUnit.SECONDS);

        /*
         * total messages = 137 * 3 = 411 Each consumer has 10 permits. There will be 411 / 3*10 = 13 full distributions
         * i.e. each consumer will get 130 messages. In the 14th round, the balance is 411 - 130*3 = 21. Two consumers
         * will get another batch of 10 messages (Total: 140) and the 3rd one will get the last one (Total: 131)
         */
        assertTrue(CollectionUtils.subtract(Lists.newArrayList(140, 140, 131),
                Lists.newArrayList(counter1.get(), counter2.get(), counter3.get())).isEmpty());

        consumer1.close();
        consumer2.close();
        consumer3.close();

        deleteTopic(topicName);
    }

    @Test(timeOut = 300000)
    public void testSharedSingleAckedNormalTopic() throws Exception {
        String key = "test1";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-shared-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 50;

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // 2. Create consumer
        ConsumerBuilder<byte[]> consumerBuilder1 = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(10).subscriptionType(SubscriptionType.Shared);
        Consumer<byte[]> consumer1 = consumerBuilder1.subscribe();

        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        ConsumerBuilder<byte[]> consumerBuilder2 = newPulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(10).subscriptionType(SubscriptionType.Shared);
        Consumer<byte[]> consumer2 = consumerBuilder2.subscribe();

        // 3. Producer publishes messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
            log.info("Producer produced " + message);
        }

        // 4. Receive messages
        int receivedConsumer1 = 0, receivedConsumer2 = 0;
        Message<byte[]> message1 = consumer1.receive();
        Message<byte[]> message2 = consumer2.receive();
        do {
            if (message1 != null) {
                log.info("Consumer 1 Received: " + new String(message1.getData()));
                receivedConsumer1 += 1;
            }
            if (message2 != null) {
                log.info("Consumer 2 Received: " + new String(message2.getData()));
                receivedConsumer2 += 1;
            }
            message1 = consumer1.receive(10000, TimeUnit.MILLISECONDS);
            message2 = consumer2.receive(10000, TimeUnit.MILLISECONDS);
        } while (message1 != null || message2 != null);

        log.info("Total receives = " + (receivedConsumer2 + receivedConsumer1));
        assertEquals(receivedConsumer2 + receivedConsumer1, totalMessages);

        // 5. Close Consumer 1
        log.info("Consumer 1 closed");
        consumer1.close();

        // 6. Consumer 1's unAcked messages should be sent to Consumer 2
        for (int i = 0; i < totalMessages; i++) {
            message2 = consumer2.receive(100, TimeUnit.MILLISECONDS);
            if (message2 == null) {
                log.info("Consumer 2 - No Message in Incoming Message Queue, will try again");
                continue;
            }
            log.info("Consumer 2 Received: " + new String(message2.getData()));
            receivedConsumer2 += 1;
        }

        newPulsarClient.close();
        log.info("Total receives by Consumer 2 = " + receivedConsumer2);
        assertEquals(receivedConsumer2, totalMessages);
    }

    @Test(timeOut = 60000)
    public void testCancelReadRequestOnLastDisconnect() throws Exception {
        String key = "testCancelReadRequestOnLastDisconnect";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-shared-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // 2. Create consumer
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(1000).subscriptionType(SubscriptionType.Shared);
        Consumer<byte[]> consumer1 = consumerBuilder.subscribe();
        Consumer<byte[]> consumer2 = consumerBuilder.subscribe();

        // 3. Producer publishes messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
            log.info("Producer produced " + message);
        }

        // 4. Receive messages
        int receivedConsumer1 = 0, receivedConsumer2 = 0;
        Message<byte[]> message1 = consumer1.receive();
        Message<byte[]> message2 = consumer2.receive();
        do {
            if (message1 != null) {
                log.info("Consumer 1 Received: " + new String(message1.getData()));
                receivedConsumer1 += 1;
                consumer1.acknowledge(message1);
            }
            if (message2 != null) {
                log.info("Consumer 2 Received: " + new String(message2.getData()));
                receivedConsumer2 += 1;
                consumer2.acknowledge(message2);
            }
            message1 = consumer1.receive(5000, TimeUnit.MILLISECONDS);
            message2 = consumer2.receive(5000, TimeUnit.MILLISECONDS);
        } while (message1 != null || message2 != null);

        log.info("Total receives = " + (receivedConsumer2 + receivedConsumer1));
        assertEquals(receivedConsumer2 + receivedConsumer1, totalMessages);

        // 5. Close Consumer 1 and 2
        log.info("Consumer 1 closed");
        log.info("Consumer 2 closed");
        consumer1.close();
        consumer2.close();

        // 6. Producer produces more messages
        for (int i = totalMessages; i < 2 * totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
            log.info("Producer produced " + message);
        }

        // 7. Consumer reconnects
        consumer1 = consumerBuilder.subscribe();

        // 8. Check number of messages received
        receivedConsumer1 = 0;
        message1 = consumer1.receive();
        while (message1 != null) {
            log.info("Consumer 1 Received: " + new String(message1.getData()));
            receivedConsumer1++;
            message1 = consumer1.receive(5000, TimeUnit.MILLISECONDS);
        }
        log.info("Total receives by Consumer 2 = " + receivedConsumer2);
        assertEquals(receivedConsumer1, totalMessages);
    }

    @Test
    public void testUnackedCountWithRedeliveries() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testUnackedCountWithRedeliveries";
        final String subName = "sub3";
        final int numMsgs = 10;

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .receiverQueueSize(10).subscriptionType(SubscriptionType.Shared)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS);
        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) consumerBuilder.subscribe();

        for (int i = 0; i < numMsgs; i++) {
            producer.send(("hello-" + i).getBytes());
        }

        Set<MessageId> c1_receivedMessages = new HashSet<>();

        // C-1 gets all messages but doesn't ack
        for (int i = 0; i < numMsgs; i++) {
            c1_receivedMessages.add(consumer1.receive().getMessageId());
        }

        // C-2 will not get any message initially, since everything went to C-1 already
        Consumer<byte[]> consumer2 = consumerBuilder.subscribe();

        // Trigger C-1 to redeliver everything, half will go C-1 again and the other half to C-2
        consumer1.redeliverUnacknowledgedMessages(c1_receivedMessages);

        // Consumer 2 will also receive all message but not ack
        for (int i = 0; i < numMsgs; i++) {
            consumer2.receive();
        }

        for (MessageId msgId : c1_receivedMessages) {
            consumer1.acknowledge(msgId);
        }

        Awaitility.await().untilAsserted(() -> {
            TopicStats stats = admin.topics().getStats(topicName);
            // Unacked messages count should be 0 for both consumers at this point
            SubscriptionStats subStats = stats.getSubscriptions().get(subName);
            assertEquals(subStats.getMsgBacklog(), 0);
            for (ConsumerStats cs : subStats.getConsumers()) {
                assertEquals(cs.getUnackedMessages(), 0);
            }
        });

        producer.close();
        consumer1.close();
        consumer2.close();

        deleteTopic(topicName);
    }
}
