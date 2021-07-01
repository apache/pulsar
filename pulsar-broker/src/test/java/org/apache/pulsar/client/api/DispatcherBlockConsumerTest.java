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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;


import lombok.Cleanup;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

@Test(groups = "flaky")
public class DispatcherBlockConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(DispatcherBlockConsumerTest.class);

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "gracefulUnload")
    public Object[][] bundleUnloading() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    /**
     * Verifies broker blocks dispatching after unack-msgs reaches to max-limit and start dispatching back once client
     * ack messages.
     *
     * @throws Exception
     */
    @Test(enabled = false) // See https://github.com/apache/pulsar/issues/5438
    public void testConsumerBlockingWithUnAckedMessagesAtDispatcher() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerSubscription();
        try {
            stopBroker();
            startBroker();
            final int unackMsgAllowed = 100;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 200;
            final String topicName = "persistent://my-property/my-ns/unacked-topic";
            final String subscriberName = "subscriber-1";

            pulsar.getConfiguration().setMaxUnackedMessagesPerSubscription(unackMsgAllowed);
            ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriberName).receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared);
            Consumer<byte[]> consumer1 = consumerBuilder.subscribe();
            Consumer<byte[]> consumer2 = consumerBuilder.subscribe();
            Consumer<byte[]> consumer3 = consumerBuilder.subscribe();
            List<Consumer<?>> consumers = Lists.newArrayList(consumer1, consumer2, consumer3);

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/my-ns/unacked-topic").create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unackMsgAllowed
            Message<?> msg = null;
            Map<Message<?>, Consumer<?>> messages = Maps.newHashMap();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers.get(i).receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(msg, consumers.get(i));
                    } else {
                        break;
                    }
                }
            }

            // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages: check
            // delta as 3 consumers with receiverQueueSize = 10
            assertEquals(messages.size(), unackMsgAllowed, receiverQueueSize * 3);

            // start acknowledging messages
            messages.forEach((m, c) -> {
                try {
                    c.acknowledge(m);
                } catch (PulsarClientException e) {
                    fail("ack failed", e);
                }
            });

            // try to consume remaining messages: broker may take time to deliver so, retry multiple time to consume
            // all messages
            Set<MessageId> result = ConcurrentHashMap.newKeySet();
            // expecting messages which are not received
            int expectedRemainingMessages = totalProducedMsgs - messages.size();
            CountDownLatch latch = new CountDownLatch(expectedRemainingMessages);
            for (int i = 0; i < consumers.size(); i++) {
                final int consumerCount = i;
                for (int j = 0; j < totalProducedMsgs; j++) {
                    consumers.get(i).receiveAsync().thenAccept(m -> {
                        result.add(m.getMessageId());
                        try {
                            consumers.get(consumerCount).acknowledge(m);
                        } catch (PulsarClientException e) {
                            fail("failed to ack msg", e);
                        }
                        latch.countDown();
                    });
                }
            }

            latch.await(10, TimeUnit.SECONDS);

            // total received-messages should match to produced messages (it may have duplicate messages)
            assertEquals(result.size(), expectedRemainingMessages);

            producer.close();
            consumers.forEach(c -> {
                try {
                    c.close();
                } catch (PulsarClientException e) {
                }
            });
            log.info("-- Exiting {} test --", methodName);
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }

    /**
     *
     * Verifies: broker blocks dispatching once unack-msg reaches to max-limit. However, on redelivery it redelivers
     * those already delivered-unacked messages again
     *
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test(enabled = false) // See https://github.com/apache/pulsar/issues/5438
    public void testConsumerBlockingWithUnAckedMessagesAndRedelivery() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerSubscription();
        try {
            final int unackMsgAllowed = 100;
            final int totalProducedMsgs = 150;
            final int receiverQueueSize = 10;
            final String topicName = "persistent://my-property/my-ns/unacked-topic-" + UUID.randomUUID().toString();
            final String subscriberName = "subscriber-1";

            pulsar.getConfiguration().setMaxUnackedMessagesPerSubscription(unackMsgAllowed);
            ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer()
                    .topic(topicName)
                    .subscriptionName(subscriberName)
                    .receiverQueueSize(receiverQueueSize)
                    .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                    .subscriptionType(SubscriptionType.Shared);
            ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) consumerBuilder.subscribe();
            ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) consumerBuilder.subscribe();
            ConsumerImpl<byte[]> consumer3 = (ConsumerImpl<byte[]>) consumerBuilder.subscribe();
            List<ConsumerImpl<byte[]>> consumers = Lists.newArrayList(consumer1, consumer2, consumer3);

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic(topicName)
                    .create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unackMsgAllowed
            Message<?> msg = null;
            Multimap<ConsumerImpl<?>, MessageId> messages = ArrayListMultimap.create();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers.get(i).receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(consumers.get(i), msg.getMessageId());
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }

            // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages
            assertNotEquals(messages.size(), totalProducedMsgs);

            // (3) trigger redelivery
            messages.asMap().forEach((c, msgs) -> {
                c.redeliverUnacknowledgedMessages(
                        msgs.stream().map(m -> (MessageIdImpl) m).collect(Collectors.toSet()));
            });

            // (4) try to consume remaining messages: broker may take time to deliver so, retry multiple time to consume
            // all messages
            Set<MessageId> result = ConcurrentHashMap.newKeySet();
            for (int i = 0; i < consumers.size(); i++) {
                final int consumerCount = i;
                for (int j = 0; j < totalProducedMsgs; j++) {
                    consumers.get(i).receiveAsync().thenAccept(m -> {
                        log.info("Received: {}", new String(m.getData()));
                        result.add(m.getMessageId());
                        try {
                            consumers.get(consumerCount).acknowledge(m);
                        } catch (PulsarClientException e) {
                            fail("failed to ack msg", e);
                        }
                    });
                }
            }

            while (result.size() < totalProducedMsgs) {
                Thread.sleep(100);
                log.info("Result Size: " + result.size());
            }

            assertEquals(result.size(), totalProducedMsgs);

            // total received-messages should match to produced messages (it may have duplicate messages)
            assertTrue(result.size() >= totalProducedMsgs);
            producer.close();
            consumers.forEach(c -> {
                try {
                    c.close();
                } catch (PulsarClientException e) {
                }
            });
            log.info("-- Exiting {} test --", methodName);
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }

    /**
     * It verifies that consumer1 attached to dispatcher will be blocked after reaching limit. But consumer2 connects
     * and consumer1 will be closed: makes broker to dispatch all those consumer1's unack messages back to consumer2.
     *
     * @throws Exception
     */
    @Test(enabled = false) // See https://github.com/apache/pulsar/issues/5438
    public void testCloseConsumerBlockedDispatcher() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topicName = "persistent://my-property/my-ns/unacked-topic-" + UUID.randomUUID().toString();

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerSubscription();
        try {
            final int unackMsgAllowed = 100;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 200;

            final String subscriberName = "subscriber-1";

            pulsar.getConfiguration().setMaxUnackedMessagesPerSubscription(unackMsgAllowed);
            Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                    .topic(topicName)
                    .subscriptionName(subscriberName)
                    .receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic(topicName)
                    .create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unackMsgAllowed
            Message<?> msg = null;
            Map<Message<?>, Consumer<?>> messages = Maps.newHashMap();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer1.receive(500, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    messages.put(msg, consumer1);
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages
            assertEquals(messages.size(), unackMsgAllowed, receiverQueueSize * 2);

            // close consumer1: all messages of consumer1 must be replayed and received by consumer2
            consumer1.close();
            // create consumer2
            Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                    .topic(topicName)
                    .subscriptionName(subscriberName)
                    .receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared)
                    .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                    .subscribe();
            Map<Message<?>, Consumer<?>> messages2 = Maps.newHashMap();
            // try to consume remaining messages: broker may take time to deliver so, retry multiple time to consume
            // all messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer2.receive(5, TimeUnit.SECONDS);
                if (msg != null) {
                    messages2.put(msg, consumer2);
                    consumer2.acknowledge(msg);
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            assertEquals(messages2.size(), totalProducedMsgs);
            log.info("-- Exiting {} test --", methodName);
            producer.close();
            consumer2.close();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }

    /**
     * Verifies: old-client which does redelivery of all messages makes broker to redeliver all unacked messages for
     * redelivery.
     *
     * @throws Exception
     */
    @Test(enabled = false) // See https://github.com/apache/pulsar/issues/5438
    public void testRedeliveryOnBlockedDispatcher() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerSubscription();
        try {
            final int unackMsgAllowed = 100;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 150;
            final String topicName = "persistent://my-property/my-ns/unacked-topic-" + UUID.randomUUID().toString();
            final String subscriberName = "subscriber-1";

            pulsar.getConfiguration().setMaxUnackedMessagesPerSubscription(unackMsgAllowed);
            ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriberName).receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared);
            ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) consumerBuilder.subscribe();
            ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) consumerBuilder.subscribe();
            ConsumerImpl<byte[]> consumer3 = (ConsumerImpl<byte[]>) consumerBuilder.subscribe();
            List<ConsumerImpl<?>> consumers = Lists.newArrayList(consumer1, consumer2, consumer3);

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic(topicName).create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unackMsgAllowed
            Message<?> msg = null;
            Set<MessageId> messages = Sets.newHashSet();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers.get(i).receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.add(msg.getMessageId());
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }

            int totalConsumedMsgs = messages.size();
            // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages
            assertEquals(totalConsumedMsgs, unackMsgAllowed, 3 * receiverQueueSize);

            // trigger redelivery
            consumers.forEach(c -> {
                c.redeliverUnacknowledgedMessages();
            });

            // wait for redelivery to be completed
            Thread.sleep(1000);

            // now, broker must have redelivered all unacked messages
            Map<ConsumerImpl<?>, Set<MessageId>> messages1 = Maps.newHashMap();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers.get(i).receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages1.putIfAbsent(consumers.get(i), Sets.newHashSet());
                        messages1.get(consumers.get(i)).add(msg.getMessageId());
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }

            Set<MessageId> result = Sets.newHashSet();
            messages1.values().forEach(result::addAll);

            // check all unacked messages have been redelivered

            assertEquals(totalConsumedMsgs, result.size(), 3 * receiverQueueSize);

            // start acknowledging messages
            messages1.forEach((c, msgs) -> {
                msgs.forEach(m -> {
                    try {
                        c.acknowledge(m);
                    } catch (PulsarClientException e) {
                        fail("ack failed", e);
                    }
                });
            });

            messages1.values().forEach(result::addAll);
            // try to consume remaining messages
            int remainingMessages = totalProducedMsgs - result.size();
            // try to consume remaining messages: broker may take time to deliver so, retry multiple time to consume
            // all messages
            CountDownLatch latch = new CountDownLatch(remainingMessages);
            Queue<MessageId> consumedMessages = Queues.newConcurrentLinkedQueue();
            for (int i = 0; i < consumers.size(); i++) {
                final int counsumerIndex = i;
                for (int j = 0; j < remainingMessages; j++) {
                    consumers.get(i).receiveAsync().thenAccept(m -> {
                        consumedMessages.add(m.getMessageId());
                        try {
                            consumers.get(counsumerIndex).acknowledge(m);
                        } catch (PulsarClientException e) {
                            fail("failed to ack", e);
                        }
                        latch.countDown();
                    });
                }
            }

            latch.await();
            // total received-messages should match remaining messages excluding duplicate
            assertTrue(consumedMessages.size() >= remainingMessages);
            producer.close();
            consumers.forEach(c -> {
                try {
                    c.close();
                } catch (PulsarClientException e) {
                }
            });
            log.info("-- Exiting {} test --", methodName);
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }

    @Test
    public void testBlockDispatcherStats() throws Exception {

        int orginalDispatcherLimit = conf.getMaxUnackedMessagesPerSubscription();
        try {
            final String topicName = "persistent://prop/use/ns-abc/blockDispatch";
            final String subName = "blockDispatch";
            final int timeWaitToSync = 100;

            TopicStats stats;
            SubscriptionStats subStats;

            // configure maxUnackMessagePerDispatcher then restart broker to get this change
            conf.setMaxUnackedMessagesPerSubscription(10);
            stopBroker();
            startBroker();

            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscriptionType(SubscriptionType.Shared).subscribe();
            Thread.sleep(timeWaitToSync);

            PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
            assertNotNull(topicRef);

            rolloverPerIntervalStats();
            stats = topicRef.getStats(false, false);
            subStats = stats.getSubscriptions().values().iterator().next();

            // subscription stats
            assertEquals(stats.getSubscriptions().keySet().size(), 1);
            assertEquals(subStats.getMsgBacklog(), 0);
            assertEquals(subStats.getConsumers().size(), 1);

            Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
            Thread.sleep(timeWaitToSync);

            for (int i = 0; i < 100; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }
            Thread.sleep(timeWaitToSync);

            rolloverPerIntervalStats();
            stats = topicRef.getStats(false, false);
            subStats = stats.getSubscriptions().values().iterator().next();

            assertTrue(subStats.getMsgBacklog() > 0);
            assertTrue(subStats.getUnackedMessages() > 0);
            assertTrue(subStats.isBlockedSubscriptionOnUnackedMsgs());
            assertEquals(subStats.getConsumers().get(0).getUnackedMessages(), subStats.getUnackedMessages());

            // consumer stats
            assertTrue(subStats.getConsumers().get(0).getMsgRateOut() > 0.0);
            assertTrue(subStats.getConsumers().get(0).getMsgThroughputOut() > 0.0);
            assertEquals(subStats.getMsgRateRedeliver(), 0.0);
            producer.close();
            consumer.close();

        } finally {
            conf.setMaxUnackedMessagesPerSubscription(orginalDispatcherLimit);
        }

    }

    /**
     * <pre>
     * It verifies that cursor-recovery
     * 1. recovers individualDeletedMessages
     * 2. sets readPosition with last acked-message
     * 3. replay all unack messages
     * </pre>
     *
     * @throws Exception
     */
    @Test(dataProvider = "gracefulUnload")
    public void testBrokerSubscriptionRecovery(boolean unloadBundleGracefully) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topicName = "persistent://my-property/my-ns/unacked-topic";
        final String subscriberName = "subscriber-1";
        final int totalProducedMsgs = 500;

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriberName)
                .subscriptionType(SubscriptionType.Shared).acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/unacked-topic")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        CountDownLatch latch = new CountDownLatch(totalProducedMsgs);
        // (1) Produced Messages
        for (int i = 0; i < totalProducedMsgs; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes()).thenAccept(msg -> latch.countDown());
        }
        latch.await();
        // (2) consume all messages except: unackMessages-set
        Set<Integer> unackMessages = Sets.newHashSet(5, 10, 20, 21, 22, 23, 25, 26, 30, 32, 40, 80, 160, 320);
        int receivedMsgCount = 0;
        for (int i = 0; i < totalProducedMsgs; i++) {
            Message<?> msg = consumer.receive(500, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            if (!unackMessages.contains(i)) {
                consumer.acknowledge(msg);
            }
            receivedMsgCount++;
        }
        assertEquals(totalProducedMsgs, receivedMsgCount);
        consumer.close();

        // if broker unload bundle gracefully then cursor metadata recovered from zk else from ledger
        if (unloadBundleGracefully) {
            // set clean namespace which will not let broker unload bundle gracefully: stop broker
            Supplier<NamespaceService> namespaceServiceSupplier = () -> spy(new NamespaceService(pulsar));
            doReturn(namespaceServiceSupplier).when(pulsar).getNamespaceServiceProvider();
        }
        stopBroker();

        // start broker which will recover topic-cursor from the ledger
        startBroker();
        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriberName)
                .subscriptionType(SubscriptionType.Shared).subscribe();

        // consumer should only receive unakced messages
        Set<String> unackMsgs = unackMessages.stream().map(i -> "my-message-" + i).collect(Collectors.toSet());
        Set<String> receivedMsgs = Sets.newHashSet();
        for (int i = 0; i < totalProducedMsgs; i++) {
            Message<?> msg = consumer.receive(500, TimeUnit.MILLISECONDS);
            if (msg == null) {
                break;
            }
            receivedMsgs.add(new String(msg.getData()));
        }
        // there is no guarantee when a messages is acknowledged when consumer.acknowledge is called.
        // consumer.acknowledge only guarantees that an ack request is sent to the wire. so we can
        // only check all unackMsgs will be redelivered.
        unackMsgs.forEach(msg -> assertTrue(receivedMsgs.contains(msg)));
    }

    /**
     * </pre>
     * verifies perBroker dispatching blocking. A. maxUnAckPerBroker = 200, maxUnAckPerDispatcher = 20 Now, it tests
     * with 3 subscriptions.
     *
     * 1. Subscription-1: try to consume without acking a. consumer will be blocked after 200 (maxUnAckPerBroker) msgs
     * b. even second consumer will not receive any new messages c. broker will have 1 blocked dispatcher 2.
     * Subscription-2: try to consume without acking a. as broker is already blocked it will block subscription after 20
     * msgs (maxUnAckPerDispatcher) b. broker will have 2 blocked dispatchers 3. Subscription-3: try to consume with
     * acking a. as consumer is acking not reached maxUnAckPerDispatcher=20 unack msg => consumes all produced msgs
     * 4.Subscription-1 : acks all pending msgs and consume by acking a. broker unblocks all dispatcher and sub-1
     * consumes all messages 5. Subscription-2 : it triggers redelivery and acks all messages so, it consumes all
     * produced messages
     * </pre>
     *
     */
    @Test(timeOut = 10000)
    public void testBlockBrokerDispatching() {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerBroker();
        double unAckedMessagePercentage = pulsar.getConfiguration()
                .getMaxUnackedMessagesPerSubscriptionOnBrokerBlocked();

        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        try {
            final int maxUnAckPerBroker = 200;
            final double unAckMsgPercentagePerDispatcher = 10;
            int maxUnAckPerDispatcher = (int) ((maxUnAckPerBroker * unAckMsgPercentagePerDispatcher) / 100); // 200 *
                                                                                                             // 10% = 20
                                                                                                             // messages
            pulsar.getConfiguration().setMaxUnackedMessagesPerBroker(maxUnAckPerBroker);
            pulsar.getConfiguration()
                    .setMaxUnackedMessagesPerSubscriptionOnBrokerBlocked(unAckMsgPercentagePerDispatcher);

            stopBroker();
            startBroker();

            Field field = BrokerService.class.getDeclaredField("blockedDispatchers");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            ConcurrentOpenHashSet<PersistentDispatcherMultipleConsumers> blockedDispatchers =
                    (ConcurrentOpenHashSet<PersistentDispatcherMultipleConsumers>) field.get(pulsar.getBrokerService());

            final int receiverQueueSize = 10;
            final int totalProducedMsgs = maxUnAckPerBroker * 3;
            final String topicName = "persistent://my-property/my-ns/unacked-topic";
            final String subscriberName1 = "subscriber-1";
            final String subscriberName2 = "subscriber-2";
            final String subscriberName3 = "subscriber-3";

            ConsumerImpl<byte[]> consumer1Sub1 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriberName1).receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared).acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
            // create subscription-2 and 3
            ConsumerImpl<byte[]> consumer1Sub2 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriberName2).receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared).acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
            consumer1Sub2.close();
            ConsumerImpl<byte[]> consumer1Sub3 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriberName3).receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared).acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
            consumer1Sub3.close();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/my-ns/unacked-topic").create();

            // continuously checks unack-message dispatching
            executor.scheduleAtFixedRate(() -> pulsar.getBrokerService().checkUnAckMessageDispatching(), 10, 10,
                    TimeUnit.MILLISECONDS);
            // Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            /*****
             * (1) try to consume messages: without acking messages and dispatcher will be blocked once it reaches
             * maxUnAckPerBroker limit
             ***/
            Message<byte[]> msg = null;
            Set<MessageId> messages1 = Sets.newHashSet();
            for (int j = 0; j < totalProducedMsgs; j++) {
                msg = consumer1Sub1.receive(100, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    messages1.add(msg.getMessageId());
                } else {
                    break;
                }
                // once consumer receives maxUnAckPerBroker-msgs then sleep to give a chance to scheduler to block the
                // subscription
                if (j == maxUnAckPerBroker) {
                    Thread.sleep(200);
                }
            }
            // client must receive number of messages = maxUnAckPerbroker rather all produced messages
            assertNotEquals(messages1.size(), totalProducedMsgs);
            @Cleanup
            PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
            // (1.b) consumer2 with same sub should not receive any more messages as subscription is blocked
            ConsumerImpl<byte[]> consumer2Sub1 = (ConsumerImpl<byte[]>) newPulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriberName1).receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared).acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
            int consumer2Msgs = 0;
            for (int j = 0; j < totalProducedMsgs; j++) {
                msg = consumer2Sub1.receive(100, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    consumer2Msgs++;
                } else {
                    break;
                }
            }
            // consumer should not consume any more messages as broker has blocked the dispatcher
            assertEquals(consumer2Msgs, 0);
            consumer2Sub1.close();
            // (1.c) verify that dispatcher is part of blocked dispatcher
            assertEquals(blockedDispatchers.size(), 1);
            String dispatcherName = blockedDispatchers.values().get(0).getName();
            String subName = dispatcherName.substring(dispatcherName.lastIndexOf("/") + 2, dispatcherName.length());
            assertEquals(subName, subscriberName1);

            /**
             * (2) However, other subscription2 should still be able to consume messages until it reaches to
             * maxUnAckPerDispatcher limit
             **/
            ConsumerImpl<byte[]> consumerSub2 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriberName2).receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared).acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
            Set<MessageId> messages2 = Sets.newHashSet();
            for (int j = 0; j < totalProducedMsgs; j++) {
                msg = consumerSub2.receive(100, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    messages2.add(msg.getMessageId());
                } else {
                    break;
                }
            }
            // (2.b) It should receive only messages with limit of maxUnackPerDispatcher
            assertEquals(messages2.size(), maxUnAckPerDispatcher, receiverQueueSize);
            assertEquals(blockedDispatchers.size(), 2);

            /** (3) if Subscription3 is acking then it shouldn't be blocked **/
            consumer1Sub3 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriberName3).receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared).acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
            int consumedMsgsSub3 = 0;
            for (int j = 0; j < totalProducedMsgs; j++) {
                msg = consumer1Sub3.receive(100, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    consumedMsgsSub3++;
                    consumer1Sub3.acknowledge(msg);
                } else {
                    break;
                }
            }
            assertEquals(consumedMsgsSub3, totalProducedMsgs);
            assertEquals(blockedDispatchers.size(), 2);

            /** (4) try to ack messages from sub1 which should unblock broker */
            messages1.forEach(consumer1Sub1::acknowledgeAsync);
            // sleep so, broker receives all ack back to unblock subscription
            Thread.sleep(1000);
            for (int j = 0; j < totalProducedMsgs; j++) {
                msg = consumer1Sub1.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages1.add(msg.getMessageId());
                    consumer1Sub1.acknowledge(msg);
                } else {
                    break;
                }
            }
            assertEquals(messages1.size(), totalProducedMsgs);
            // it unblocks all consumers
            assertEquals(blockedDispatchers.size(), 0);

            /** (5) try redelivery on sub2 consumer and verify to consume all messages */
            consumerSub2.redeliverUnacknowledgedMessages();
            AtomicInteger msgReceivedCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(totalProducedMsgs);
            for (int j = 0; j < totalProducedMsgs; j++) {
                consumerSub2.receiveAsync().thenAccept(m -> {
                    msgReceivedCount.incrementAndGet();
                    latch.countDown();
                    try {
                        consumerSub2.acknowledge(m);
                    } catch (PulsarClientException e) {
                        fail("failed to ack msg", e);
                    }
                });
            }
            latch.await();
            assertEquals(msgReceivedCount.get(), totalProducedMsgs);

            consumer1Sub1.close();
            consumerSub2.close();
            consumer1Sub3.close();

            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerBroker(unAckedMessages);
            pulsar.getConfiguration().setMaxUnackedMessagesPerSubscriptionOnBrokerBlocked(unAckedMessagePercentage);
        }
    }

    /**
     * Verifies if broker is already blocked multiple subscriptions if one of them acked back perBrokerDispatcherLimit
     * messages then that dispatcher gets unblocked and starts consuming messages
     *
     * <pre>
     * 1. subscription-1 consume messages and doesn't ack so it reaches maxUnAckPerBroker(200) and blocks sub-1
     * 2. subscription-2 can consume only dispatcherLimitWhenBrokerIsBlocked(20) and then sub-2 gets blocked
     * 3. subscription-2 acks back 10 messages (dispatcherLimitWhenBrokerIsBlocked/2) to gets unblock
     * 4. sub-2 starts acking once it gets unblocked and it consumes all published messages
     * </pre>
     *
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBrokerDispatchBlockAndSubAckBackRequiredMsgs() {

        log.info("-- Starting {} test --", methodName);

        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerBroker();
        double unAckedMessagePercentage = pulsar.getConfiguration()
                .getMaxUnackedMessagesPerSubscriptionOnBrokerBlocked();
        try {
            final int maxUnAckPerBroker = 200;
            final double unAckMsgPercentagePerDispatcher = 10;
            int maxUnAckPerDispatcher = (int) ((maxUnAckPerBroker * unAckMsgPercentagePerDispatcher) / 100); // 200 *
                                                                                                             // 10% = 20
                                                                                                             // messages
            pulsar.getConfiguration().setMaxUnackedMessagesPerBroker(maxUnAckPerBroker);
            pulsar.getConfiguration()
                    .setMaxUnackedMessagesPerSubscriptionOnBrokerBlocked(unAckMsgPercentagePerDispatcher);

            stopBroker();
            startBroker();

            Field field = BrokerService.class.getDeclaredField("blockedDispatchers");
            field.setAccessible(true);
            ConcurrentOpenHashSet<PersistentDispatcherMultipleConsumers> blockedDispatchers =
                    (ConcurrentOpenHashSet<PersistentDispatcherMultipleConsumers>) field.get(pulsar.getBrokerService());

            final int receiverQueueSize = 10;
            final int totalProducedMsgs = maxUnAckPerBroker * 3;
            final String topicName = "persistent://my-property/my-ns/unacked-topic";
            final String subscriberName1 = "subscriber-1";
            final String subscriberName2 = "subscriber-2";

            ConsumerImpl<byte[]> consumer1Sub1 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriberName1).receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared).subscribe();
            // create subscription-2 and 3
            ConsumerImpl<byte[]> consumer1Sub2 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriberName2).receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared).subscribe();
            consumer1Sub2.close();

            // continuously checks unack-message dispatching
            executor.scheduleAtFixedRate(() -> pulsar.getBrokerService().checkUnAckMessageDispatching(), 10, 10,
                    TimeUnit.MILLISECONDS);

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/my-ns/unacked-topic").create();

            // Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            /*****
             * (1) try to consume messages: without acking messages and dispatcher will be blocked once it reaches
             * maxUnAckPerBroker limit
             ***/
            Message<?> msg = null;
            Set<MessageId> messages1 = Sets.newHashSet();
            for (int j = 0; j < totalProducedMsgs; j++) {
                msg = consumer1Sub1.receive(100, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    messages1.add(msg.getMessageId());
                } else {
                    break;
                }
                // once consumer receives maxUnAckPerBroker-msgs then sleep to give a chance to scheduler to block the
                // subscription
                if (j == maxUnAckPerBroker) {
                    Thread.sleep(200);
                }
            }
            // client must receive number of messages = maxUnAckPerbroker rather all produced messages
            assertNotEquals(messages1.size(), totalProducedMsgs);
            // (1.b) consumer2 with same sub should not receive any more messages as subscription is blocked
            @Cleanup
            PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
            ConsumerImpl<byte[]> consumer2Sub1 = (ConsumerImpl<byte[]>) newPulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriberName1).receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared).subscribe();
            int consumer2Msgs = 0;
            for (int j = 0; j < totalProducedMsgs; j++) {
                msg = consumer2Sub1.receive(100, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    consumer2Msgs++;
                } else {
                    break;
                }
            }
            // consumer should not consume any more messages as broker has blocked the dispatcher
            assertEquals(consumer2Msgs, 0);
            consumer2Sub1.close();
            // (1.c) verify that dispatcher is part of blocked dispatcher
            assertEquals(blockedDispatchers.size(), 1);
            String dispatcherName = blockedDispatchers.values().get(0).getName();
            String subName = dispatcherName.substring(dispatcherName.lastIndexOf("/") + 2, dispatcherName.length());
            assertEquals(subName, subscriberName1);

            /**
             * (2) However, other subscription2 should still be able to consume messages until it reaches to
             * maxUnAckPerDispatcher limit
             **/
            consumer1Sub2 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriberName2).receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared).subscribe();
            Set<MessageId> messages2 = Sets.newHashSet();
            for (int j = 0; j < totalProducedMsgs; j++) {
                msg = consumer1Sub2.receive(100, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    messages2.add(msg.getMessageId());
                } else {
                    break;
                }
            }
            // (2.b) It should receive only messages with limit of maxUnackPerDispatcher
            assertEquals(messages2.size(), maxUnAckPerDispatcher, receiverQueueSize);
            assertEquals(blockedDispatchers.size(), 2);

            // (2.c) Now subscriber-2 is blocked: so acking back should unblock dispatcher
            Iterator<MessageId> itrMsgs = messages2.iterator();
            int additionalMsgConsumedAfterBlocked = messages2.size() - maxUnAckPerDispatcher + 1; // eg. 25 -20 = 5
            for (int i = 0; i < (additionalMsgConsumedAfterBlocked + (maxUnAckPerDispatcher / 2)); i++) {
                consumer1Sub2.acknowledge(itrMsgs.next());
            }
            // let ack completed
            Thread.sleep(1000);
            // verify subscriber2 is unblocked and ready to consume more messages
            assertEquals(blockedDispatchers.size(), 1);
            for (int j = 0; j < totalProducedMsgs; j++) {
                msg = consumer1Sub2.receive(200, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    messages2.add(msg.getMessageId());
                    consumer1Sub2.acknowledge(msg);
                } else {
                    break;
                }
            }
            // verify it consumed all messages now
            assertEquals(messages2.size(), totalProducedMsgs);

            consumer1Sub1.close();
            consumer1Sub2.close();

            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerBroker(unAckedMessages);
            pulsar.getConfiguration().setMaxUnackedMessagesPerSubscriptionOnBrokerBlocked(unAckedMessagePercentage);
        }
    }

    private void rolloverPerIntervalStats() {
        try {
            pulsar.getExecutor().submit(() -> pulsar.getBrokerService().updateRates()).get();
        } catch (Exception e) {
            log.error("Stats executor error", e);
        }
    }
}
