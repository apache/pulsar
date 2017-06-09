/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.api;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.yahoo.pulsar.broker.namespace.NamespaceService;
import com.yahoo.pulsar.broker.service.persistent.PersistentTopic;
import com.yahoo.pulsar.client.impl.ConsumerImpl;
import com.yahoo.pulsar.client.impl.MessageIdImpl;
import com.yahoo.pulsar.common.policies.data.PersistentSubscriptionStats;
import com.yahoo.pulsar.common.policies.data.PersistentTopicStats;

import jersey.repackaged.com.google.common.collect.Sets;

public class DispatcherBlockConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(DispatcherBlockConsumerTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
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
    @Test
    public void testConsumerBlockingWithUnAckedMessagesAtDispatcher() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerSubscription();
        try {
            stopBroker();
            startBroker();
            final int unackMsgAllowed = 100;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 200;
            final String topicName = "persistent://my-property/use/my-ns/unacked-topic";
            final String subscriberName = "subscriber-1";

            pulsar.getConfiguration().setMaxUnackedMessagesPerSubscription(unackMsgAllowed);
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setSubscriptionType(SubscriptionType.Shared);
            Consumer consumer1 = pulsarClient.subscribe(topicName, subscriberName, conf);
            Consumer consumer2 = pulsarClient.subscribe(topicName, subscriberName, conf);
            Consumer consumer3 = pulsarClient.subscribe(topicName, subscriberName, conf);
            Consumer[] consumers = { consumer1, consumer2, consumer3 };

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unackMsgAllowed
            Message msg = null;
            Map<Message, Consumer> messages = Maps.newHashMap();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(msg, consumers[i]);
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }

            // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages
            assertEquals(messages.size(), unackMsgAllowed, receiverQueueSize * 2);

            // start acknowledging messages
            messages.forEach((m, c) -> {
                try {
                    c.acknowledge(m);
                } catch (PulsarClientException e) {
                    fail("ack failed", e);
                }
            });
            // wait to start dispatching-async
            Thread.sleep(2000);
            // try to consume remaining messages
            int remainingMessages = totalProducedMsgs - messages.size();
            for (int i = 0; i < consumers.length; i++) {
                for (int j = 0; j < remainingMessages; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(msg, consumers[i]);
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }

            // total received-messages should match to produced messages
            assertEquals(totalProducedMsgs, messages.size());
            producer.close();
            Arrays.asList(consumers).forEach(c -> {
                try {
                    c.close();
                } catch (PulsarClientException e) {
                }
            });
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
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
    @Test
    public void testConsumerBlockingWithUnAckedMessagesAndRedelivery() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerSubscription();
        try {
            stopBroker();
            startBroker();
            final int unackMsgAllowed = 100;
            final int totalProducedMsgs = 200;
            final int receiverQueueSize = 10;
            final String topicName = "persistent://my-property/use/my-ns/unacked-topic";
            final String subscriberName = "subscriber-1";

            pulsar.getConfiguration().setMaxUnackedMessagesPerSubscription(unackMsgAllowed);
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setSubscriptionType(SubscriptionType.Shared);
            ConsumerImpl consumer1 = (ConsumerImpl) pulsarClient.subscribe(topicName, subscriberName, conf);
            ConsumerImpl consumer2 = (ConsumerImpl) pulsarClient.subscribe(topicName, subscriberName, conf);
            ConsumerImpl consumer3 = (ConsumerImpl) pulsarClient.subscribe(topicName, subscriberName, conf);
            ConsumerImpl[] consumers = { consumer1, consumer2, consumer3 };

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unackMsgAllowed
            Message msg = null;
            Multimap<ConsumerImpl, MessageId> messages = ArrayListMultimap.create();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(consumers[i], msg.getMessageId());
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }

            int totalConsumedMsgs = messages.size();
            // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages
            assertNotEquals(messages.size(), totalProducedMsgs);

            // trigger redelivery
            messages.asMap().forEach((c, msgs) -> {
                c.redeliverUnacknowledgedMessages(
                        msgs.stream().map(m -> (MessageIdImpl) m).collect(Collectors.toSet()));
            });

            // wait for redelivery to be completed
            Thread.sleep(1000);

            // now, broker must have redelivered all unacked messages
            messages.clear();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(consumers[i], msg.getMessageId());
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }

            // check all unacked messages have been redelivered
            Set<MessageId> result = Sets.newHashSet(messages.values());
            assertEquals(totalConsumedMsgs, result.size(), 2 * receiverQueueSize);

            // start acknowledging messages
            messages.asMap().forEach((c, msgs) -> {
                msgs.forEach(m -> {
                    try {
                        c.acknowledge(m);
                    } catch (PulsarClientException e) {
                        fail("ack failed", e);
                    }
                });
            });

            // now: dispatcher must be unblocked: wait to start dispatching-async
            Thread.sleep(1000);
            // try to consume remaining messages
            for (int i = 0; i < consumers.length; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages.put(consumers[i], msg.getMessageId());
                        consumers[i].acknowledge(msg);
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }

            result = Sets.newHashSet(messages.values());
            // total received-messages should match to produced messages
            assertEquals(totalProducedMsgs, result.size());
            producer.close();
            Arrays.asList(consumers).forEach(c -> {
                try {
                    c.close();
                } catch (PulsarClientException e) {
                }
            });
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
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
    @Test
    public void testCloseConsumerBlockedDispatcher() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerSubscription();
        try {
            stopBroker();
            startBroker();
            final int unackMsgAllowed = 100;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 200;
            final String topicName = "persistent://my-property/use/my-ns/unacked-topic";
            final String subscriberName = "subscriber-1";

            pulsar.getConfiguration().setMaxUnackedMessagesPerSubscription(unackMsgAllowed);
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setSubscriptionType(SubscriptionType.Shared);
            Consumer consumer1 = pulsarClient.subscribe(topicName, subscriberName, conf);

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unackMsgAllowed
            Message msg = null;
            Map<Message, Consumer> messages = Maps.newHashMap();
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

            // create consumer2
            Consumer consumer2 = pulsarClient.subscribe(topicName, subscriberName, conf);
            // close consumer1: all messages of consumer1 must be replayed and received by consumer2
            consumer1.close();
            Map<Message, Consumer> messages2 = Maps.newHashMap();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer2.receive(500, TimeUnit.MILLISECONDS);
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
        } catch (Exception e) {
            fail();
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
    @Test
    public void testRedeliveryOnBlockedDistpatcher() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerSubscription();
        try {
            stopBroker();
            startBroker();
            final int unackMsgAllowed = 100;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 200;
            final String topicName = "persistent://my-property/use/my-ns/unacked-topic";
            final String subscriberName = "subscriber-1";

            pulsar.getConfiguration().setMaxUnackedMessagesPerSubscription(unackMsgAllowed);
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setSubscriptionType(SubscriptionType.Shared);
            conf.setReceiverQueueSize(receiverQueueSize);
            ConsumerImpl consumer1 = (ConsumerImpl) pulsarClient.subscribe(topicName, subscriberName, conf);
            ConsumerImpl consumer2 = (ConsumerImpl) pulsarClient.subscribe(topicName, subscriberName, conf);
            ConsumerImpl consumer3 = (ConsumerImpl) pulsarClient.subscribe(topicName, subscriberName, conf);
            ConsumerImpl[] consumers = { consumer1, consumer2, consumer3 };

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unackMsgAllowed
            Message msg = null;
            Set<MessageId> messages = Sets.newHashSet();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
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
            assertEquals(totalConsumedMsgs, unackMsgAllowed, 2 * receiverQueueSize);

            // trigger redelivery
            Arrays.asList(consumers).forEach(c -> {
                c.redeliverUnacknowledgedMessages();
            });

            // wait for redelivery to be completed
            Thread.sleep(1000);

            // now, broker must have redelivered all unacked messages
            Map<ConsumerImpl, Set<MessageId>> messages1 = Maps.newHashMap();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < totalProducedMsgs; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages1.putIfAbsent(consumers[i], Sets.newHashSet());
                        messages1.get(consumers[i]).add(msg.getMessageId());
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }

            Set<MessageId> result = Sets.newHashSet();
            messages1.values().forEach(s -> result.addAll(s));

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

            // now: dispatcher must be unblocked: wait to start dispatching-async
            Thread.sleep(2000);
            // try to consume remaining messages
            int remainingMessages = totalProducedMsgs - messages1.size();
            for (int i = 0; i < consumers.length; i++) {
                for (int j = 0; j < remainingMessages; j++) {
                    msg = consumers[i].receive(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messages1.putIfAbsent(consumers[i], Sets.newHashSet());
                        messages1.get(consumers[i]).add(msg.getMessageId());
                        consumers[i].acknowledge(msg);
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
            }

            result.clear();
            messages1.values().forEach(s -> result.addAll(s));
            // total received-messages should match to produced messages
            assertEquals(totalProducedMsgs, result.size());
            producer.close();
            Arrays.asList(consumers).forEach(c -> {
                try {
                    c.close();
                } catch (PulsarClientException e) {
                }
            });
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
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

            PersistentTopicStats stats;
            PersistentSubscriptionStats subStats;

            // configure maxUnackMessagePerDispatcher then restart broker to get this change
            conf.setMaxUnackedMessagesPerSubscription(10);
            stopBroker();
            startBroker();

            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setSubscriptionType(SubscriptionType.Shared);
            Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);
            Thread.sleep(timeWaitToSync);

            PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
            assertNotNull(topicRef);

            rolloverPerIntervalStats();
            stats = topicRef.getStats();
            subStats = stats.subscriptions.values().iterator().next();

            // subscription stats
            assertEquals(stats.subscriptions.keySet().size(), 1);
            assertEquals(subStats.msgBacklog, 0);
            assertEquals(subStats.consumers.size(), 1);

            Producer producer = pulsarClient.createProducer(topicName);
            Thread.sleep(timeWaitToSync);

            for (int i = 0; i < 100; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }
            Thread.sleep(timeWaitToSync);

            rolloverPerIntervalStats();
            stats = topicRef.getStats();
            subStats = stats.subscriptions.values().iterator().next();

            assertTrue(subStats.msgBacklog > 0);
            assertTrue(subStats.unackedMessages > 0);
            assertTrue(subStats.blockedSubscriptionOnUnackedMsgs);
            assertEquals(subStats.consumers.get(0).unackedMessages, subStats.unackedMessages);

            // consumer stats
            assertTrue(subStats.consumers.get(0).msgRateOut > 0.0);
            assertTrue(subStats.consumers.get(0).msgThroughputOut > 0.0);
            assertEquals(subStats.msgRateRedeliver, 0.0);
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

        final String topicName = "persistent://my-property/use/my-ns/unacked-topic";
        final String subscriberName = "subscriber-1";
        final int totalProducedMsgs = 500;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer = pulsarClient.subscribe(topicName, subscriberName, conf);

        ProducerConfiguration producerConf = new ProducerConfiguration();

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                producerConf);

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
            Message msg = consumer.receive(100, TimeUnit.MILLISECONDS);
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
        consumer = pulsarClient.subscribe(topicName, subscriberName, conf);

        // consumer should only receive unakced messages
        Set<String> unackMsgs = unackMessages.stream().map(i -> "my-message-" + i).collect(Collectors.toSet());
        Set<String> receivedMsgs = Sets.newHashSet();
        for (int i = 0; i < totalProducedMsgs; i++) {
            Message msg = consumer.receive(500, TimeUnit.MILLISECONDS);
            if (msg == null) {
                break;
            }
            receivedMsgs.add(new String(msg.getData()));
        }
        receivedMsgs.removeAll(unackMsgs);
        assertTrue(receivedMsgs.isEmpty());
    }
    
    private void rolloverPerIntervalStats() {
        try {
            pulsar.getExecutor().submit(() -> pulsar.getBrokerService().updateRates()).get();
        } catch (Exception e) {
            log.error("Stats executor error", e);
        }
    }
}