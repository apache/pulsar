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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.util.FutureUtil;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

/**
 */
public class PersistentQueueE2ETest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentQueueE2ETest.class);

    @Test
    public void testSimpleConsumerEvents() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/shared-topic1";
        final String subName = "sub1";
        final int numMsgs = 100;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);

        // 1. two consumers on the same subscription
        Consumer consumer1 = pulsarClient.subscribe(topicName, subName, conf);
        Consumer consumer2 = pulsarClient.subscribe(topicName, subName, conf);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        assertNotNull(topicRef);
        assertNotNull(subRef);

        // 2. validate basic dispatcher state
        assertTrue(subRef.getDispatcher().isConsumerConnected());
        assertEquals(subRef.getDispatcher().getType(), SubType.Shared);

        List<CompletableFuture<MessageId>> futures = Lists.newArrayListWithCapacity(numMsgs * 2);
        Producer producer = pulsarClient.createProducer(topicName);
        for (int i = 0; i < numMsgs * 2; i++) {
            String message = "my-message-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();

        rolloverPerIntervalStats();

        assertEquals(subRef.getNumberOfEntriesInBacklog(), numMsgs * 2);
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        // both consumers will together consumer all messages
        Message msg;
        Consumer c = consumer1;
        while (true) {
            try {
                msg = c.receive(1, TimeUnit.SECONDS);
                c.acknowledge(msg);
            } catch (PulsarClientException e) {
                if (c.equals(consumer1)) {
                    c = consumer2;
                } else {
                    break;
                }
            }
        }

        rolloverPerIntervalStats();

        // 3. messages deleted on individual acks
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(subRef.getNumberOfEntriesInBacklog(), 0);

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

        admin.persistentTopics().delete(topicName);
    }

    @Test
    public void testReplayOnConsumerDisconnect() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/shared-topic3";
        final String subName = "sub3";
        final int numMsgs = 100;

        final List<String> messagesProduced = Lists.newArrayListWithCapacity(numMsgs);
        final List<String> messagesConsumed = new BlockingArrayQueue<>(numMsgs);

        ConsumerConfiguration conf1 = new ConsumerConfiguration();
        conf1.setSubscriptionType(SubscriptionType.Shared);
        conf1.setMessageListener((consumer, msg) -> {
            try {
                consumer.acknowledge(msg);
                messagesConsumed.add(new String(msg.getData()));
            } catch (Exception e) {
                fail("Should not fail");
            }
        });

        ConsumerConfiguration conf2 = new ConsumerConfiguration();
        conf2.setSubscriptionType(SubscriptionType.Shared);
        conf2.setMessageListener((consumer, msg) -> {
            try {
                // do nothing
            } catch (Exception e) {
                fail("Should not fail");
            }
        });

        Consumer consumer1 = pulsarClient.subscribe(topicName, subName, conf1);

        // consumer2 does not ack messages
        Consumer consumer2 = pulsarClient.subscribe(topicName, subName, conf2);

        List<CompletableFuture<MessageId>> futures = Lists.newArrayListWithCapacity(numMsgs * 2);
        Producer producer = pulsarClient.createProducer(topicName);
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
        admin.persistentTopics().delete(topicName);
    }

    @Test
    public void testConsumersWithDifferentPermits() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/shared-topic4";
        final String subName = "sub4";
        final int numMsgs = 10000;

        final AtomicInteger msgCountConsumer1 = new AtomicInteger(0);
        final AtomicInteger msgCountConsumer2 = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(numMsgs);

        int recvQ1 = 10;
        ConsumerConfiguration conf1 = new ConsumerConfiguration();
        conf1.setSubscriptionType(SubscriptionType.Shared);
        conf1.setReceiverQueueSize(recvQ1);
        conf1.setMessageListener((consumer, msg) -> {
            msgCountConsumer1.incrementAndGet();
            try {
                consumer.acknowledge(msg);
                latch.countDown();
            } catch (PulsarClientException e) {
                fail("Should not fail");
            }
        });

        int recvQ2 = 1;
        ConsumerConfiguration conf2 = new ConsumerConfiguration();
        conf2.setSubscriptionType(SubscriptionType.Shared);
        conf2.setReceiverQueueSize(recvQ2);
        conf2.setMessageListener((consumer, msg) -> {
            msgCountConsumer2.incrementAndGet();
            try {
                consumer.acknowledge(msg);
                latch.countDown();
            } catch (PulsarClientException e) {
                fail("Should not fail");
            }
        });

        Consumer consumer1 = pulsarClient.subscribe(topicName, subName, conf1);
        Consumer consumer2 = pulsarClient.subscribe(topicName, subName, conf2);

        List<CompletableFuture<MessageId>> futures = Lists.newArrayListWithCapacity(numMsgs);
        ProducerConfiguration conf = new ProducerConfiguration();
        conf.setMaxPendingMessages(numMsgs + 1);
        Producer producer = pulsarClient.createProducer(topicName, conf);
        for (int i = 0; i < numMsgs; i++) {
            String message = "msg-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();
        producer.close();

        latch.await(5, TimeUnit.SECONDS);

        assertEquals(msgCountConsumer1.get(), numMsgs - numMsgs / (recvQ1 + recvQ2), numMsgs * 0.1);
        assertEquals(msgCountConsumer2.get(), numMsgs / (recvQ1 + recvQ2), numMsgs * 0.1);

        consumer1.close();
        consumer2.close();
        admin.persistentTopics().delete(topicName);
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

        ConsumerConfiguration conf1 = new ConsumerConfiguration();
        conf1.setSubscriptionType(SubscriptionType.Shared);
        conf1.setReceiverQueueSize(10);
        conf1.setMessageListener((consumer, msg) -> {
            try {
                counter1.incrementAndGet();
                consumer.acknowledge(msg);
                latch.countDown();
            } catch (Exception e) {
                fail("Should not fail");
            }
        });

        ConsumerConfiguration conf2 = new ConsumerConfiguration();
        conf2.setSubscriptionType(SubscriptionType.Shared);
        conf2.setReceiverQueueSize(10);
        conf2.setMessageListener((consumer, msg) -> {
            try {
                counter2.incrementAndGet();
                consumer.acknowledge(msg);
                latch.countDown();
            } catch (Exception e) {
                fail("Should not fail");
            }
        });

        ConsumerConfiguration conf3 = new ConsumerConfiguration();
        conf3.setSubscriptionType(SubscriptionType.Shared);
        conf3.setReceiverQueueSize(10);
        conf3.setMessageListener((consumer, msg) -> {
            try {
                counter3.incrementAndGet();
                consumer.acknowledge(msg);
                latch.countDown();
            } catch (Exception e) {
                fail("Should not fail");
            }
        });

        // subscribe and close, so that distribution can be checked after
        // all messages are published
        Consumer consumer1 = pulsarClient.subscribe(topicName, subName, conf1);
        Consumer consumer2 = pulsarClient.subscribe(topicName, subName, conf2);
        Consumer consumer3 = pulsarClient.subscribe(topicName, subName, conf3);

        List<CompletableFuture<MessageId>> futures = Lists.newArrayListWithCapacity(numMsgs);
        Producer producer = pulsarClient.createProducer(topicName);
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
        admin.persistentTopics().delete(topicName);
    }

    @Test(timeOut = 300000)
    public void testSharedSingleAckedNormalTopic() throws Exception {
        String key = "test1";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-shared-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 50;

        // 1. producer connect
        Producer producer = pulsarClient.createProducer(topicName);
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // 2. Create consumer
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(10);
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer1 = pulsarClient.subscribe(topicName, subscriptionName, conf);
        Consumer consumer2 = pulsarClient.subscribe(topicName, subscriptionName, conf);

        // 3. Producer publishes messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
            log.info("Producer produced " + message);
        }

        // 4. Receive messages
        int receivedConsumer1 = 0, receivedConsumer2 = 0;
        Message message1 = consumer1.receive();
        Message message2 = consumer2.receive();
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
        Producer producer = pulsarClient.createProducer(topicName);
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // 2. Create consumer
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(1000);
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer1 = pulsarClient.subscribe(topicName, subscriptionName, conf);
        Consumer consumer2 = pulsarClient.subscribe(topicName, subscriptionName, conf);

        // 3. Producer publishes messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
            log.info("Producer produced " + message);
        }

        // 4. Receive messages
        int receivedConsumer1 = 0, receivedConsumer2 = 0;
        Message message1 = consumer1.receive();
        Message message2 = consumer2.receive();
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
        consumer1 = pulsarClient.subscribe(topicName, subscriptionName, conf);

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

}
