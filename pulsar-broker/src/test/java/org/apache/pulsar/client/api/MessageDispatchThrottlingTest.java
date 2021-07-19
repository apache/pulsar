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

import static org.testng.Assert.assertNotNull;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "flaky")
public class MessageDispatchThrottlingTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(MessageDispatchThrottlingTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setClusterName("test");
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        super.resetConfig();
    }

    @DataProvider(name = "subscriptions")
    public Object[][] subscriptionsProvider() {
        return new Object[][] { new Object[] { SubscriptionType.Shared }, { SubscriptionType.Exclusive } };
    }

    @DataProvider(name = "dispatchRateType")
    public Object[][] dispatchRateProvider() {
        return new Object[][] { { DispatchRateType.messageRate }, { DispatchRateType.byteRate } };
    }

    @DataProvider(name = "subscriptionAndDispatchRateType")
    public Object[][] subDisTypeProvider() {
        List<Object[]> mergeList = new LinkedList<>();
        for (Object[] sub : subscriptionsProvider()) {
            for (Object[] dispatch : dispatchRateProvider()) {
                mergeList.add(merge(sub, dispatch));
            }
        }
        return mergeList.toArray(new Object[0][0]);
    }

    public static <T> T[] merge(T[] first, T[] last) {
        int totalLength = first.length + last.length;
        T[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        System.arraycopy(last, 0, result, offset, first.length);
        return result;
    }

    enum DispatchRateType {
        messageRate, byteRate;
    }

    /**
     * verifies: message-rate change gets reflected immediately into topic at runtime
     *
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testMessageRateDynamicallyChange() throws Exception {

        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName = "persistent://" + namespace + "/throttlingBlock";

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        // (1) verify message-rate is -1 initially
        Assert.assertFalse(topic.getDispatchRateLimiter().isPresent());

        // (2) change to 100
        int messageRate = 100;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(messageRate)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(360)
                .build();
        admin.namespaces().setDispatchRate(namespace, dispatchRate);
        boolean isDispatchRateUpdate = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (topic.getDispatchRateLimiter().isPresent()) {
                isDispatchRateUpdate = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(isDispatchRateUpdate);
        Assert.assertEquals(admin.namespaces().getDispatchRate(namespace), dispatchRate);
        Policies policies = admin.namespaces().getPolicies(namespace);
        Map<String, DispatchRate> dispatchRateMap = Maps.newHashMap();
        dispatchRateMap.put("test", dispatchRate);
        Assert.assertEquals(policies.clusterDispatchRate, dispatchRateMap);
        Assert.assertEquals(policies.topicDispatchRate, dispatchRateMap);

        // (3) change to 500
        messageRate = 500;
        dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(messageRate)
                .ratePeriodInSecond(360)
                .build();
        admin.namespaces().setDispatchRate(namespace, dispatchRate);
        isDispatchRateUpdate = false;
        for (int i = 0; i < retry; i++) {
            if (topic.getDispatchRateLimiter().get().getDispatchRateOnByte() == messageRate) {
                isDispatchRateUpdate = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(isDispatchRateUpdate);
        Assert.assertEquals(admin.namespaces().getDispatchRate(namespace), dispatchRate);
        policies = admin.namespaces().getPolicies(namespace);
        dispatchRateMap.put("test", dispatchRate);
        Assert.assertEquals(policies.clusterDispatchRate, dispatchRateMap);
        Assert.assertEquals(policies.topicDispatchRate, dispatchRateMap);

        producer.close();
    }

    /**
     * verify: consumer should not receive all messages due to message-rate throttling
     *
     * @param subscription
     * @throws Exception
     */
    @Test(dataProvider = "subscriptionAndDispatchRateType", timeOut = 5000)
    public void testMessageRateLimitingNotReceiveAllMessages(SubscriptionType subscription,
            DispatchRateType dispatchRateType) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName = "persistent://" + namespace + "/throttlingBlock";

        final int messageRate = 100;
        DispatchRate dispatchRate = null;
        if (DispatchRateType.messageRate.equals(dispatchRateType)) {
            dispatchRate = DispatchRate.builder()
                    .dispatchThrottlingRateInMsg(messageRate)
                    .dispatchThrottlingRateInByte(-1)
                    .ratePeriodInSecond(360)
                    .build();
        } else {
            dispatchRate = DispatchRate.builder()
                    .dispatchThrottlingRateInMsg(-1)
                    .dispatchThrottlingRateInByte(messageRate)
                    .ratePeriodInSecond(360)
                    .build();
        }

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setDispatchRate(namespace, dispatchRate);
        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        boolean isMessageRateUpdate = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (topic.getDispatchRateLimiter().get().getDispatchRateOnMsg() > 0
                    || topic.getDispatchRateLimiter().get().getDispatchRateOnByte() > 0) {
                isMessageRateUpdate = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(isMessageRateUpdate);
        Assert.assertEquals(admin.namespaces().getDispatchRate(namespace), dispatchRate);

        int numMessages = 500;

        final AtomicInteger totalReceived = new AtomicInteger(0);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscriptionType(subscription).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    totalReceived.incrementAndGet();
                }).subscribe();
        // deactive cursors
        deactiveCursors((ManagedLedgerImpl) topic.getManagedLedger());

        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            producer.send(new byte[80]);
        }

        // consumer should not have received all published message due to message-rate throttling
        Assert.assertTrue(totalReceived.get() < messageRate * 2);

        consumer.close();
        producer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * It verifies that dispatch-rate throttling with cluster-configuration
     *
     * @throws Exception
     */
    @Test
    public void testClusterMsgByteRateLimitingClusterConfig() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName = "persistent://" + namespace + "/throttlingBlock";
        final int messageRate = 5;
        final long byteRate = 1024 * 1024;// 1MB rate enough to let all msg to be delivered

        int initValue = pulsar.getConfiguration().getDispatchThrottlingRatePerTopicInMsg();
        // (1) Update message-dispatch-rate limit
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRatePerTopicInMsg",
                Integer.toString(messageRate));
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRatePerTopicInByte", Long.toString(byteRate));
        // sleep incrementally as zk-watch notification is async and may take some time
        for (int i = 0; i < 5; i++) {
            if (pulsar.getConfiguration().getDispatchThrottlingRatePerTopicInMsg() == initValue) {
                Thread.sleep(50 + (i * 10));
            }
        }
        Assert.assertNotEquals(pulsar.getConfiguration().getDispatchThrottlingRatePerTopicInMsg(), initValue);

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        int numMessages = 500;

        final AtomicInteger totalReceived = new AtomicInteger(0);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Shared).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    totalReceived.incrementAndGet();
                }).subscribe();

        // deactive cursors
        deactiveCursors((ManagedLedgerImpl) topic.getManagedLedger());

        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            final String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // it can make sure that consumer had enough time to consume message but couldn't consume due to throttling
        Thread.sleep(500);

        // consumer should not have received all published message due to message-rate throttling
        Assert.assertNotEquals(totalReceived.get(), numMessages);

        consumer.close();
        producer.close();
        pulsar.getConfiguration().setDispatchThrottlingRatePerTopicInMsg(initValue);
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * verify rate-limiting should throttle message-dispatching based on message-rate
     *
     * <pre>
     *  1. dispatch-msg-rate = 10 msg/sec
     *  2. send 20 msgs
     *  3. it should take up to 2 second to receive all messages
     * </pre>
     *
     * @param subscription
     * @throws Exception
     */
    @Test(dataProvider = "subscriptions", timeOut = 5000)
    public void testMessageRateLimitingReceiveAllMessagesAfterThrottling(SubscriptionType subscription)
            throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName = "persistent://" + namespace + "/throttlingAll";

        final int messageRate = 10;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(messageRate)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setDispatchRate(namespace, dispatchRate);
        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        boolean isMessageRateUpdate = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (topic.getDispatchRateLimiter().get().getDispatchRateOnMsg() > 0) {
                isMessageRateUpdate = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(isMessageRateUpdate);
        Assert.assertEquals(admin.namespaces().getDispatchRate(namespace), dispatchRate);

        final int numProducedMessages = 20;
        final CountDownLatch latch = new CountDownLatch(numProducedMessages);

        final AtomicInteger totalReceived = new AtomicInteger(0);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscriptionType(subscription).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    totalReceived.incrementAndGet();
                    latch.countDown();
                }).subscribe();
        // deactive cursors
        deactiveCursors((ManagedLedgerImpl) topic.getManagedLedger());

        // Asynchronously produce messages
        for (int i = 0; i < numProducedMessages; i++) {
            final String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        latch.await();
        Assert.assertEquals(totalReceived.get(), numProducedMessages);

        consumer.close();
        producer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * verify rate-limiting should throttle message-dispatching based on byte-rate
     *
     * <pre>
     *  1. dispatch-byte-rate = 100 bytes/sec
     *  2. send 20 msgs : each with 10 byte
     *  3. it should take up to 2 second to receive all messages
     * </pre>
     *
     * @param subscription
     * @throws Exception
     */
    @Test(dataProvider = "subscriptions", timeOut = 5000)
    public void testBytesRateLimitingReceiveAllMessagesAfterThrottling(SubscriptionType subscription) throws Exception {
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName = "persistent://" + namespace + "/throttlingAll";
        final String subscriptionName = "my-subscriber-name";

        //
        final int byteRate = 250;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(byteRate)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setDispatchRate(namespace, dispatchRate);
        admin.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);
        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableBatching(false).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        Awaitility.await().until(() -> topic.getDispatchRateLimiter().get().getDispatchRateOnByte() > 0);
        Assert.assertEquals(admin.namespaces().getDispatchRate(namespace), dispatchRate);

        final int numProducedMessages = 20;

        final AtomicInteger totalReceived = new AtomicInteger(0);

        for (int i = 0; i < numProducedMessages; i++) {
            producer.send(new byte[99]);
        }

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscriptionType(subscription).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    totalReceived.incrementAndGet();
                }).subscribe();

        Awaitility.await().atLeast(3, TimeUnit.SECONDS)
                .atMost(5, TimeUnit.SECONDS).until(() -> totalReceived.get() > 6 && totalReceived.get() < 10);

        consumer.close();
        producer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * verify message-rate on multiple consumers with shared-subscription
     *
     * @throws Exception
     */
    @Test(timeOut = 5000)
    public void testRateLimitingMultipleConsumers() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName = "persistent://" + namespace + "/throttlingMultipleConsumers";

        final int messageRate = 5;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(messageRate)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(360)
                .build();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setDispatchRate(namespace, dispatchRate);
        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        boolean isMessageRateUpdate = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (topic.getDispatchRateLimiter().get().getDispatchRateOnMsg() > 0) {
                isMessageRateUpdate = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(isMessageRateUpdate);
        Assert.assertEquals(admin.namespaces().getDispatchRate(namespace), dispatchRate);

        final int numProducedMessages = 500;

        final AtomicInteger totalReceived = new AtomicInteger(0);

        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    totalReceived.incrementAndGet();
                });
        Consumer<byte[]> consumer1 = consumerBuilder.subscribe();
        Consumer<byte[]> consumer2 = consumerBuilder.subscribe();
        Consumer<byte[]> consumer3 = consumerBuilder.subscribe();
        Consumer<byte[]> consumer4 = consumerBuilder.subscribe();
        Consumer<byte[]> consumer5 = consumerBuilder.subscribe();

        // deactive cursors
        deactiveCursors((ManagedLedgerImpl) topic.getManagedLedger());

        // Asynchronously produce messages
        for (int i = 0; i < numProducedMessages; i++) {
            final String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // it can make sure that consumer had enough time to consume message but couldn't consume due to throttling
        Thread.sleep(500);

        // consumer should not have received all published message due to message-rate throttling
        Assert.assertNotEquals(totalReceived.get(), numProducedMessages);

        consumer1.close();
        consumer2.close();
        consumer3.close();
        consumer4.close();
        consumer5.close();
        producer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "subscriptions", timeOut = 5000)
    public void testClusterRateLimitingConfiguration(SubscriptionType subscription) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName = "persistent://" + namespace + "/throttlingBlock";
        final int messageRate = 5;

        int initValue = pulsar.getConfiguration().getDispatchThrottlingRatePerTopicInMsg();
        // (1) Update message-dispatch-rate limit
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRatePerTopicInMsg",
                Integer.toString(messageRate));
        // sleep incrementally as zk-watch notification is async and may take some time
        for (int i = 0; i < 5; i++) {
            if (pulsar.getConfiguration().getDispatchThrottlingRatePerTopicInMsg() == initValue) {
                Thread.sleep(50 + (i * 10));
            }
        }
        Assert.assertNotEquals(pulsar.getConfiguration().getDispatchThrottlingRatePerTopicInMsg(), initValue);

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        int numMessages = 500;

        final AtomicInteger totalReceived = new AtomicInteger(0);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscriptionType(subscription).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    totalReceived.incrementAndGet();
                }).subscribe();
        // deactive cursors
        deactiveCursors((ManagedLedgerImpl) topic.getManagedLedger());

        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            final String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // it can make sure that consumer had enough time to consume message but couldn't consume due to throttling
        Thread.sleep(500);

        // consumer should not have received all published message due to message-rate throttling
        Assert.assertNotEquals(totalReceived.get(), numMessages);

        consumer.close();
        producer.close();
        pulsar.getConfiguration().setDispatchThrottlingRatePerTopicInMsg(initValue);
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * It verifies that that dispatch-throttling considers both msg/byte rate if both of them are configured together
     *
     * @param subscription
     * @throws Exception
     */
    @Test(dataProvider = "subscriptions", timeOut = 5000)
    public void testMessageByteRateThrottlingCombined(SubscriptionType subscription) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName = "persistent://" + namespace + "/throttlingAll";

        final int messageRate = 5; // 5 msgs per second
        final long byteRate = 10; // 10 bytes per second
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(messageRate)
                .dispatchThrottlingRateInByte(byteRate)
                .ratePeriodInSecond(360)
                .build();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setDispatchRate(namespace, dispatchRate);
        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        boolean isMessageRateUpdate = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (topic.getDispatchRateLimiter().get().getDispatchRateOnMsg() > 0
                    && topic.getDispatchRateLimiter().get().getDispatchRateOnByte() > 0) {
                isMessageRateUpdate = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(isMessageRateUpdate);
        Assert.assertEquals(admin.namespaces().getDispatchRate(namespace), dispatchRate);

        final int numProducedMessages = 200;

        final AtomicInteger totalReceived = new AtomicInteger(0);

        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscriber-name").subscriptionType(subscription).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    totalReceived.incrementAndGet();
                });
        Consumer<byte[]> consumer = consumerBuilder.subscribe();
        // deactive cursors
        deactiveCursors((ManagedLedgerImpl) topic.getManagedLedger());
        consumer.close();

        // Asynchronously produce messages
        final int dataSize = 50;
        final byte[] data = new byte[dataSize];
        for (int i = 0; i < numProducedMessages; i++) {
            producer.send(data);
        }

        consumer = consumerBuilder.subscribe();
        final int totalReceivedBytes = dataSize * totalReceived.get();
        Assert.assertNotEquals(totalReceivedBytes, byteRate * 2);

        consumer.close();
        producer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * <pre>
     * Verifies setting dispatch-rate on global namespace.
     * 1. It sets dispatch-rate for a local cluster into global-zk.policies
     * 2. Topic fetches dispatch-rate for the local cluster from policies
     * 3. applies dispatch rate
     *
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testGlobalNamespaceThrottling() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName = "persistent://" + namespace + "/throttlingBlock";

        final int messageRate = 5;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(messageRate)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(360)
                .build();

        admin.clusters().createCluster("global", ClusterData.builder().serviceUrl("http://global:8080").build());
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("test"));
        admin.namespaces().setDispatchRate(namespace, dispatchRate);

        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        boolean isMessageRateUpdate = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (topic.getDispatchRateLimiter().get().getDispatchRateOnMsg() > 0
                    || topic.getDispatchRateLimiter().get().getDispatchRateOnByte() > 0) {
                isMessageRateUpdate = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(isMessageRateUpdate);
        Assert.assertEquals(admin.namespaces().getDispatchRate(namespace), dispatchRate);

        int numMessages = 500;

        final AtomicInteger totalReceived = new AtomicInteger(0);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Shared).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    totalReceived.incrementAndGet();
                }).subscribe();
        // deactive cursors
        deactiveCursors((ManagedLedgerImpl) topic.getManagedLedger());

        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            producer.send(new byte[80]);
        }

        // it can make sure that consumer had enough time to consume message but couldn't consume due to throttling
        Thread.sleep(500);

        // consumer should not have received all published message due to message-rate throttling
        Assert.assertNotEquals(totalReceived.get(), numMessages);

        consumer.close();
        producer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * It verifies that broker throttles already caught-up consumer which doesn't have backlog if the flag is enabled
     *
     * @param subscription
     * @throws Exception
     */
    @Test(dataProvider = "subscriptions", timeOut = 5000)
    public void testNonBacklogConsumerWithThrottlingEnabled(SubscriptionType subscription) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName = "persistent://" + namespace + "/throttlingBlock";

        final int messageRate = 10;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(messageRate)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(360)
                .build();

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setDispatchRate(namespace, dispatchRate);
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingOnNonBacklogConsumerEnabled",
                Boolean.TRUE.toString());
        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        boolean isUpdated = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (topic.getDispatchRateLimiter().get().getDispatchRateOnMsg() > 0) {
                isUpdated = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(isUpdated);
        Assert.assertEquals(admin.namespaces().getDispatchRate(namespace), dispatchRate);

        // enable throttling for nonBacklog consumers
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);

        int numMessages = 500;

        final AtomicInteger totalReceived = new AtomicInteger(0);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscriptionType(subscription).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    totalReceived.incrementAndGet();
                }).subscribe();

        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            producer.send(new byte[80]);
        }

        // consumer should not have received all published message due to message-rate throttling
        Assert.assertTrue(totalReceived.get() < messageRate * 2);

        consumer.close();
        producer.close();
        // revert default value
        this.conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(false);
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * <pre>
     * It verifies that cluster-throttling value gets considered when namespace-policy throttling is disabled.
     *
     *  1. Update cluster-throttling-config: topic rate-limiter has cluster-config
     *  2. Update namespace-throttling-config: topic rate-limiter has namespace-config
     *  3. Disable namespace-throttling-config: topic rate-limiter has cluster-config
     *  4. Create new topic with disable namespace-config and enabled cluster-config: it takes cluster-config
     *
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testClusterPolicyOverrideConfiguration() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName1 = "persistent://" + namespace + "/throttlingOverride1";
        final String topicName2 = "persistent://" + namespace + "/throttlingOverride2";
        final int clusterMessageRate = 100;

        int initValue = pulsar.getConfiguration().getDispatchThrottlingRatePerTopicInMsg();
        // (1) Update message-dispatch-rate limit
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRatePerTopicInMsg",
                Integer.toString(clusterMessageRate));
        // sleep incrementally as zk-watch notification is async and may take some time
        for (int i = 0; i < 5; i++) {
            if (pulsar.getConfiguration().getDispatchThrottlingRatePerTopicInMsg() == initValue) {
                Thread.sleep(50 + (i * 10));
            }
        }
        Assert.assertNotEquals(pulsar.getConfiguration().getDispatchThrottlingRatePerTopicInMsg(), initValue);

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName1).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName1).get();

        // (1) Update dispatch rate on cluster-config update
        Assert.assertEquals(clusterMessageRate, topic.getDispatchRateLimiter().get().getDispatchRateOnMsg());

        // (2) Update namespace throttling limit
        int nsMessageRate = 500;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(nsMessageRate)
                .dispatchThrottlingRateInByte(0)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().setDispatchRate(namespace, dispatchRate);
        for (int i = 0; i < 5; i++) {
            if (topic.getDispatchRateLimiter().get().getDispatchRateOnMsg() != nsMessageRate) {
                Thread.sleep(50 + (i * 10));
            }
        }
        Assert.assertEquals(nsMessageRate, topic.getDispatchRateLimiter().get().getDispatchRateOnMsg());

        // (3) Disable namespace throttling limit will force to take cluster-config
        dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(0)
                .dispatchThrottlingRateInByte(0)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().setDispatchRate(namespace, dispatchRate);
        for (int i = 0; i < 5; i++) {
            if (topic.getDispatchRateLimiter().get().getDispatchRateOnMsg() == nsMessageRate) {
                Thread.sleep(50 + (i * 10));
            }
        }
        Assert.assertEquals(clusterMessageRate, topic.getDispatchRateLimiter().get().getDispatchRateOnMsg());

        // (5) Namespace throttling is disabled so, new topic should take cluster throttling limit
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2).create();
        PersistentTopic topic2 = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName2).get();
        Assert.assertEquals(clusterMessageRate, topic2.getDispatchRateLimiter().get().getDispatchRateOnMsg());

        producer.close();
        producer2.close();

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "subscriptions", timeOut = 10000)
    public void testClosingRateLimiter(SubscriptionType subscription) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName = "persistent://" + namespace + "/closingRateLimiter" + subscription.name();
        final String subName = "mySubscription" + subscription.name();

        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(1024)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setDispatchRate(namespace, dispatchRate);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(subscription).subscribe();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();

        final int numProducedMessages = 10;

        for (int i = 0; i < numProducedMessages; i++) {
            final String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        for (int i = 0; i < numProducedMessages; i++) {
            Message<byte[]> msg = consumer.receive();
            consumer.acknowledge(msg);
        }

        Assert.assertTrue(topic.getDispatchRateLimiter().isPresent());
        DispatchRateLimiter dispatchRateLimiter = topic.getDispatchRateLimiter().get();

        producer.close();
        consumer.unsubscribe();
        consumer.close();
        topic.close().get();

        // Make sure that the rate limiter is closed
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), -1);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), -1);

        log.info("-- Exiting {} test --", methodName);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDispatchRateCompatibility1() throws Exception {
        final String cluster = "test";

        Optional<Policies> policies = Optional.of(new Policies());
        DispatchRateImpl clusterDispatchRate = DispatchRateImpl.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(512)
                .ratePeriodInSecond(1)
                .build();
        DispatchRateImpl topicDispatchRate = DispatchRateImpl.builder()
                .dispatchThrottlingRateInMsg(200)
                .dispatchThrottlingRateInByte(1024)
                .ratePeriodInSecond(1)
                .build();

        // (1) If both clusterDispatchRate and topicDispatchRate are empty, dispatch throttling is disabled
        DispatchRateImpl dispatchRate = DispatchRateLimiter.getPoliciesDispatchRate(cluster, policies,
                DispatchRateLimiter.Type.TOPIC);
        Assert.assertNull(dispatchRate);

        // (2) If topicDispatchRate is empty, clusterDispatchRate is effective
        policies.get().clusterDispatchRate.put(cluster, clusterDispatchRate);
        dispatchRate = DispatchRateLimiter.getPoliciesDispatchRate(cluster, policies, DispatchRateLimiter.Type.TOPIC);
        Assert.assertEquals(dispatchRate, clusterDispatchRate);

        // (3) If topicDispatchRate is not empty, topicDispatchRate is effective
        policies.get().topicDispatchRate.put(cluster, topicDispatchRate);
        dispatchRate = DispatchRateLimiter.getPoliciesDispatchRate(cluster, policies, DispatchRateLimiter.Type.TOPIC);
        Assert.assertEquals(dispatchRate, topicDispatchRate);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDispatchRateCompatibility2() throws Exception {
        final String namespace = "my-property/dispatch-rate-compatibility";
        final String topicName = "persistent://" + namespace + "/t1";
        final String cluster = "test";
        admin.namespaces().createNamespace(namespace, Sets.newHashSet(cluster));
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        DispatchRateLimiter dispatchRateLimiter = new DispatchRateLimiter(topic, DispatchRateLimiter.Type.TOPIC);

        Policies policies = new Policies();
        DispatchRateImpl clusterDispatchRate = DispatchRateImpl.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(512)
                .ratePeriodInSecond(1)
                .build();
        DispatchRateImpl topicDispatchRate = DispatchRateImpl.builder()
                .dispatchThrottlingRateInMsg(200)
                .dispatchThrottlingRateInByte(1024)
                .ratePeriodInSecond(1)
                .build();

        // (1) If both clusterDispatchRate and topicDispatchRate are empty, dispatch throttling is disabled
        dispatchRateLimiter.onPoliciesUpdate(policies);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), -1);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), -1);

        // (2) If topicDispatchRate is empty, clusterDispatchRate is effective
        policies.clusterDispatchRate.put(cluster, clusterDispatchRate);
        dispatchRateLimiter.onPoliciesUpdate(policies);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), 100);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), 512);

        // (3) If topicDispatchRate is not empty, topicDispatchRate is effective
        policies.topicDispatchRate.put(cluster, topicDispatchRate);
        dispatchRateLimiter.onPoliciesUpdate(policies);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), 200);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), 1024);

        producer.close();
        topic.close().get();
    }

    protected void deactiveCursors(ManagedLedgerImpl ledger) throws Exception {
        Field statsUpdaterField = BrokerService.class.getDeclaredField("statsUpdater");
        statsUpdaterField.setAccessible(true);
        ScheduledExecutorService statsUpdater = (ScheduledExecutorService) statsUpdaterField
                .get(pulsar.getBrokerService());
        statsUpdater.shutdownNow();
        ledger.getCursors().forEach(cursor -> {
            ledger.deactivateCursor(cursor);
        });
    }

    /**
     * It verifies that relative throttling at least dispatch messages as publish-rate.
     *
     * @param subscription
     * @throws Exception
     */
    @Test(dataProvider = "subscriptions")
    public void testRelativeMessageRateLimitingThrottling(SubscriptionType subscription) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/relative_throttling_ns";
        final String topicName = "persistent://" + namespace + "/relative-throttle" + subscription;

        final int messageRate = 1;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(messageRate)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(1)
                .relativeToPublishRate(true)
                .build();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setDispatchRate(namespace, dispatchRate);
        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableBatching(false).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        boolean isMessageRateUpdate = false;
        int retry = 10;
        for (int i = 0; i < retry; i++) {
            if (topic.getDispatchRateLimiter().get().getDispatchRateOnMsg() > 0) {
                isMessageRateUpdate = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(isMessageRateUpdate);
        Assert.assertEquals(admin.namespaces().getDispatchRate(namespace), dispatchRate);
        Thread.sleep(2000);

        final int numProducedMessages = 1000;

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscriptionType(subscription).subscribe();
        // deactive cursors
        deactiveCursors((ManagedLedgerImpl) topic.getManagedLedger());

        // send a message, which will make dispatcher-ratelimiter initialize and schedule renew task
        producer.send("test".getBytes());
        assertNotNull(consumer.receive());

        Field lastUpdatedMsgRateIn = PersistentTopic.class.getDeclaredField("lastUpdatedAvgPublishRateInMsg");
        lastUpdatedMsgRateIn.setAccessible(true);
        lastUpdatedMsgRateIn.set(topic, numProducedMessages);

        for (int i = 0; i < numProducedMessages; i++) {
            final String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        int totalReceived = 0;
        // Relative throttling will let it drain immediately because it allows to dispatch = (publish-rate +
        // dispatch-rate)
        // All messages should be received in the next 1.1 seconds. 100 millis should be enough for the actual delivery,
        // while the previous call to receive above may have thrown the dispatcher into a read backoff, as nothing
        // may have been produced before the call to readNext() and the permits for dispatch had already been used.
        // The backoff is 1 second, so we expect to be able to receive all messages in at most 1.1 seconds, while the
        // basic dispatch rate limit would only allow one message in that time.
        long maxTimeNanos = TimeUnit.MILLISECONDS.toNanos(1100);
        long startNanos = System.nanoTime();
        for (int i = 0; i < numProducedMessages; i++) {
            Message<byte[]> msg = consumer.receive((int)maxTimeNanos, TimeUnit.NANOSECONDS);
            totalReceived++;
            assertNotNull(msg);
            long elapsedNanos = System.nanoTime() - startNanos;
            if (elapsedNanos > maxTimeNanos) { // fail fast
                log.info("Test has only received {} messages in {}ms, {} expected",
                         totalReceived, TimeUnit.NANOSECONDS.toMillis(elapsedNanos), numProducedMessages);
                Assert.fail("Messages not received in time");
            }
            log.info("Received {}-{}", msg.getMessageId(), new String(msg.getData()));
        }
        Assert.assertEquals(totalReceived, numProducedMessages);
        long elapsedNanos = System.nanoTime() - startNanos;
        Assert.assertTrue(elapsedNanos < maxTimeNanos);

        consumer.close();
        producer.close();
        log.info("-- Exiting {} test --", methodName);
    }
}
