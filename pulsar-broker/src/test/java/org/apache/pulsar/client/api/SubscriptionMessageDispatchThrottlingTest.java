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

import com.google.common.collect.Sets;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.awaitility.Awaitility.await;

@Test(groups = "flaky")
public class SubscriptionMessageDispatchThrottlingTest extends MessageDispatchThrottlingTest {
    private static final Logger log = LoggerFactory.getLogger(SubscriptionMessageDispatchThrottlingTest.class);

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
        final String subName = "my-subscriber-name";

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
        admin.namespaces().setSubscriptionDispatchRate(namespace, dispatchRate);
        // create producer, topic and consumer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();

        // enable throttling for nonBacklog consumers
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);

        final AtomicInteger totalReceived = new AtomicInteger(0);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
            .subscriptionType(subscription).messageListener((c1, msg) -> {
                Assert.assertNotNull(msg, "Message cannot be null");
                String receivedMessage = new String(msg.getData());
                log.debug("Received message [{}] in the listener", receivedMessage);
                totalReceived.incrementAndGet();
            }).subscribe();

        DispatchRateLimiter subRateLimiter = null;
        Dispatcher subDispatcher = topic.getSubscription(subName).getDispatcher();
        if (subDispatcher instanceof PersistentDispatcherMultipleConsumers) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else if (subDispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else {
            Assert.fail("Should only have PersistentDispatcher in this test");
        }

        boolean isMessageRateUpdate = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (subRateLimiter.getDispatchRateOnMsg() > 0
                || subRateLimiter.getDispatchRateOnByte() > 0) {
                isMessageRateUpdate = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(isMessageRateUpdate);
        Assert.assertEquals(admin.namespaces().getSubscriptionDispatchRate(namespace), dispatchRate);

        int numMessages = 500;
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
     * verify rate-limiting should throttle message-dispatching based on message-rate
     *
     * <pre>
     *  1. dispatch-msg-rate = 10 msg/sec
     *  2. send 30 msgs
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
        final String subName = "my-subscriber-name";

        final int messageRate = 10;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(messageRate)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setSubscriptionDispatchRate(namespace, dispatchRate);
        final int numProducedMessages = 30;
        final CountDownLatch latch = new CountDownLatch(numProducedMessages);
        final AtomicInteger totalReceived = new AtomicInteger(0);
        // enable throttling for nonBacklog consumers
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
            .subscriptionType(subscription).messageListener((c1, msg) -> {
                Assert.assertNotNull(msg, "Message cannot be null");
                String receivedMessage = new String(msg.getData());
                log.debug("Received message [{}] in the listener", receivedMessage);
                totalReceived.incrementAndGet();
                latch.countDown();
            }).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();

        DispatchRateLimiter subRateLimiter = null;
        Dispatcher subDispatcher = topic.getSubscription(subName).getDispatcher();
        if (subDispatcher instanceof PersistentDispatcherMultipleConsumers) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else if (subDispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else {
            Assert.fail("Should only have PersistentDispatcher in this test");
        }

        boolean isMessageRateUpdate = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (subRateLimiter.getDispatchRateOnMsg() > 0
                || subRateLimiter.getDispatchRateOnByte() > 0) {
                isMessageRateUpdate = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(isMessageRateUpdate);
        Assert.assertEquals(admin.namespaces().getSubscriptionDispatchRate(namespace), dispatchRate);

        // Asynchronously produce messages
        for (int i = 0; i < numProducedMessages; i++) {
            final String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        await().until(() -> latch.getCount() == 0);
        Assert.assertEquals(totalReceived.get(), numProducedMessages);

        consumer.close();
        producer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "subscriptions", timeOut = 30000, invocationCount = 15)
    private void testMessageNotDuplicated(SubscriptionType subscription) throws Exception {
        int brokerRate = 1000;
        int topicRate = 5000;
        int subRate = 10000;
        int expectRate = 1000;
        final String namespace = "my-property/throttling_ns_non_dup";
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/throttlingAll");
        final String subName = "my-subscriber-name-" + subscription;

        DispatchRate subscriptionDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(subRate)
                .ratePeriodInSecond(1)
                .build();
        DispatchRate topicDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(topicRate)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setSubscriptionDispatchRate(namespace, subscriptionDispatchRate);
        admin.namespaces().setDispatchRate(namespace, topicDispatchRate);
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRateInByte", "" + brokerRate);

        final int numProducedMessages = 30;
        final CountDownLatch latch = new CountDownLatch(numProducedMessages);
        final AtomicInteger totalReceived = new AtomicInteger(0);
        // enable throttling for nonBacklog consumers
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .receiverQueueSize(10)
                .subscriptionType(subscription).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    totalReceived.incrementAndGet();
                    latch.countDown();
                }).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();

        DispatchRateLimiter subRateLimiter = null;
        Dispatcher subDispatcher = topic.getSubscription(subName).getDispatcher();
        if (subDispatcher instanceof PersistentDispatcherMultipleConsumers) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else if (subDispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else {
            Assert.fail("Should only have PersistentDispatcher in this test");
        }
        final DispatchRateLimiter subDispatchRateLimiter = subRateLimiter;
        Awaitility.await().atMost(Duration.ofMillis(500)).untilAsserted(() -> {
            DispatchRateLimiter brokerDispatchRateLimiter = pulsar.getBrokerService().getBrokerDispatchRateLimiter();
            Assert.assertTrue(brokerDispatchRateLimiter != null
                    && brokerDispatchRateLimiter.getDispatchRateOnByte() > 0);
            DispatchRateLimiter topicDispatchRateLimiter = topic.getDispatchRateLimiter().orElse(null);
            Assert.assertTrue(topicDispatchRateLimiter != null
                    && topicDispatchRateLimiter.getDispatchRateOnByte() > 0);
            Assert.assertTrue(subDispatchRateLimiter != null
                    && subDispatchRateLimiter.getDispatchRateOnByte() > 0);
        });

        Assert.assertEquals(admin.namespaces().getSubscriptionDispatchRate(namespace)
                .getDispatchThrottlingRateInByte(), subRate);
        Assert.assertEquals(admin.namespaces().getDispatchRate(namespace)
                .getDispatchThrottlingRateInByte(), topicRate);

        for (int i = 0; i < numProducedMessages; i++) {
            producer.send(new byte[expectRate / 10]);
        }

        latch.await();
        // Wait 2000 milli sec to check if we can get more than 30 messages.
        Thread.sleep(2000);
        // If this assertion failed, please alert we may have some regression cause message dispatch was duplicated.
        Assert.assertEquals(totalReceived.get(), numProducedMessages, 10);

        consumer.close();
        producer.close();
        admin.topics().delete(topicName, true);
        admin.namespaces().deleteNamespace(namespace);
    }

    /**
     * verify rate-limiting should throttle message-dispatching based on byte-rate
     *
     * <pre>
     *  1. dispatch-byte-rate = 1000 bytes/sec
     *  2. send 30 msgs : each with 100 byte
     *  3. it should take up to 2 second to receive all messages
     * </pre>
     *
     * @param subscription
     * @throws Exception
     */
    @Test(dataProvider = "subscriptions", timeOut = 5000)
    public void testBytesRateLimitingReceiveAllMessagesAfterThrottling(SubscriptionType subscription) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/throttlingAll");
        final String subName = "my-subscriber-name-" + subscription;

        final int byteRate = 1000;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(byteRate)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setSubscriptionDispatchRate(namespace, dispatchRate);
        final int numProducedMessages = 30;
        final CountDownLatch latch = new CountDownLatch(numProducedMessages);
        final AtomicInteger totalReceived = new AtomicInteger(0);
        // enable throttling for nonBacklog consumers
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
            .receiverQueueSize(10)
            .subscriptionType(subscription).messageListener((c1, msg) -> {
                Assert.assertNotNull(msg, "Message cannot be null");
                String receivedMessage = new String(msg.getData());
                log.debug("Received message [{}] in the listener", receivedMessage);
                totalReceived.incrementAndGet();
                latch.countDown();
            }).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();

        DispatchRateLimiter subRateLimiter = null;
        Dispatcher subDispatcher = topic.getSubscription(subName).getDispatcher();
        if (subDispatcher instanceof PersistentDispatcherMultipleConsumers) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else if (subDispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else {
            Assert.fail("Should only have PersistentDispatcher in this test");
        }

        boolean isMessageRateUpdate = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (subRateLimiter.getDispatchRateOnMsg() > 0
                || subRateLimiter.getDispatchRateOnByte() > 0) {
                isMessageRateUpdate = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(isMessageRateUpdate);
        Assert.assertEquals(admin.namespaces().getSubscriptionDispatchRate(namespace), dispatchRate);

        long start = System.currentTimeMillis();
        // Asynchronously produce messages
        for (int i = 0; i < numProducedMessages; i++) {
            producer.send(new byte[byteRate / 10]);
        }
        latch.await();
        Assert.assertEquals(totalReceived.get(), numProducedMessages, 10);
        long end = System.currentTimeMillis();
        log.info("-- end - start: {} ", end - start);

        // first 10 messages, which equals receiverQueueSize, will not wait.
        Assert.assertTrue((end - start) >= 2000);

        consumer.close();
        producer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    private void testDispatchRate(SubscriptionType subscription,
                                  int brokerRate, int topicRate, int subRate, int expectRate) throws Exception {

        final String namespace = "my-property/throttling_ns";
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/throttlingAll");
        final String subName = "my-subscriber-name-" + subscription;

        DispatchRate subscriptionDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(subRate)
                .ratePeriodInSecond(1)
                .build();
        DispatchRate topicDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(topicRate)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setSubscriptionDispatchRate(namespace, subscriptionDispatchRate);
        admin.namespaces().setDispatchRate(namespace, topicDispatchRate);
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRateInByte", "" + brokerRate);

        final int numProducedMessages = 30;
        final CountDownLatch latch = new CountDownLatch(numProducedMessages);
        final AtomicInteger totalReceived = new AtomicInteger(0);
        // enable throttling for nonBacklog consumers
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .receiverQueueSize(10)
                .subscriptionType(subscription).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    totalReceived.incrementAndGet();
                    latch.countDown();
                }).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();

        DispatchRateLimiter subRateLimiter = null;
        Dispatcher subDispatcher = topic.getSubscription(subName).getDispatcher();
        if (subDispatcher instanceof PersistentDispatcherMultipleConsumers) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else if (subDispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else {
            Assert.fail("Should only have PersistentDispatcher in this test");
        }
        final DispatchRateLimiter subDispatchRateLimiter = subRateLimiter;
        Awaitility.await().atMost(Duration.ofMillis(500)).untilAsserted(() -> {
            DispatchRateLimiter brokerDispatchRateLimiter = pulsar.getBrokerService().getBrokerDispatchRateLimiter();
            Assert.assertTrue(brokerDispatchRateLimiter != null
                    && brokerDispatchRateLimiter.getDispatchRateOnByte() > 0);
            DispatchRateLimiter topicDispatchRateLimiter = topic.getDispatchRateLimiter().orElse(null);
            Assert.assertTrue(topicDispatchRateLimiter != null
                    && topicDispatchRateLimiter.getDispatchRateOnByte() > 0);
            Assert.assertTrue(subDispatchRateLimiter != null
                    && subDispatchRateLimiter.getDispatchRateOnByte() > 0);
        });

        Assert.assertEquals(admin.namespaces().getSubscriptionDispatchRate(namespace)
                .getDispatchThrottlingRateInByte(), subRate);
        Assert.assertEquals(admin.namespaces().getDispatchRate(namespace)
                .getDispatchThrottlingRateInByte(), topicRate);

        long start = System.currentTimeMillis();
        // Asynchronously produce messages
        for (int i = 0; i < numProducedMessages; i++) {
            producer.send(new byte[expectRate / 10]);
        }
        latch.await();
        Assert.assertEquals(totalReceived.get(), numProducedMessages, 10);
        long end = System.currentTimeMillis();
        log.info("-- end - start: {} ", end - start);

        // first 10 messages, which equals receiverQueueSize, will not wait.
        Assert.assertTrue((end - start) >= 2500);
        Assert.assertTrue((end - start) <= 8000);

        consumer.close();
        producer.close();
        admin.topics().delete(topicName, true);
        admin.namespaces().deleteNamespace(namespace);
    }

    /**
     * Verify whether rate-limiting works well when different levels rate-limiting enabled.
     *
     * <pre>
     *  1. Set broker level, topic level and subscription level dispatch-byte-rate with different limit rate value.
     *  2. Start one consumer for one topics.
     *  3. the expect dispatch rate should be the minimum value of different limit rate.
     * </pre>
     *
     * @param subscription
     * @throws Exception
     */
    @Test(dataProvider = "subscriptions")
    public void testMultiLevelDispatch(SubscriptionType subscription) throws Exception {
        log.info("-- Starting {} test --", methodName);

        testDispatchRate(subscription, 1000, 5000, 10000, 1000);

        testDispatchRate(subscription, 10000, 1000, 5000, 1000);

        testDispatchRate(subscription, 5000, 10000, 1000, 1000);

        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * Verify whether the broker level rate-limiting is throttle message-dispatching based on byte-rate or not
     *
     * <pre>
     *  1. Broker level dispatch-byte-rate is equal to 1000 bytes per second.
     *  2. Start two consumers for two topics.
     *  3. Send 15 msgs to each of the two topics. Each msgs with 100 bytes, thus 3000 bytes in total.
     *  4. It should take up to 2 seconds to receive all messages of the two topics.
     * </pre>
     *
     * @param subscription
     * @throws Exception
     */
    @Test(dataProvider = "subscriptions", timeOut = 8000)
    public void testBrokerBytesRateLimitingReceiveAllMessagesAfterThrottling(SubscriptionType subscription) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace1 = "my-property/throttling_ns1";
        final String topicName1 = BrokerTestUtil.newUniqueName("persistent://" + namespace1 + "/throttlingAll");
        final String namespace2 = "my-property/throttling_ns2";
        final String topicName2 = BrokerTestUtil.newUniqueName("persistent://" + namespace2 + "/throttlingAll");
        final String subName = "my-subscriber-name-" + subscription;

        final int byteRate = 1000;
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRateInByte", "" + byteRate);
        admin.namespaces().createNamespace(namespace1, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(namespace2, Sets.newHashSet("test"));

        final int numProducedMessagesEachTopic = 15;
        final int numProducedMessages = numProducedMessagesEachTopic * 2;
        final CountDownLatch latch = new CountDownLatch(numProducedMessages);
        final AtomicInteger totalReceived = new AtomicInteger(0);
        // enable throttling for nonBacklog consumers
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);

        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topicName1).subscriptionName(subName)
                .receiverQueueSize(10)
                .subscriptionType(subscription).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in topic1", receivedMessage);
                    totalReceived.incrementAndGet();
                    latch.countDown();
                }).subscribe();

        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic(topicName2).subscriptionName(subName)
                .receiverQueueSize(10)
                .subscriptionType(subscription).messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in topic2", receivedMessage);
                    totalReceived.incrementAndGet();
                    latch.countDown();
                }).subscribe();

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2).create();

        boolean isMessageRateUpdate = false;
        DispatchRateLimiter dispatchRateLimiter;

        Awaitility.await().atMost(Duration.ofMillis(500)).untilAsserted(() -> {
            DispatchRateLimiter rateLimiter = pulsar.getBrokerService().getBrokerDispatchRateLimiter();
            Assert.assertTrue(rateLimiter != null
                    && rateLimiter.getDispatchRateOnByte() > 0);
        });

        long start = System.currentTimeMillis();
        // Asynchronously produce messages
        for (int i = 0; i < numProducedMessagesEachTopic; i++) {
            producer1.send(new byte[byteRate / 10]);
            producer2.send(new byte[byteRate / 10]);
        }
        latch.await();
        Assert.assertEquals(totalReceived.get(), numProducedMessages, 10);
        long end = System.currentTimeMillis();
        log.info("-- time to receive all messages: {} ", end - start);

        // first 10 messages, which equals receiverQueueSize, will not wait.
        Assert.assertTrue((end - start) >= 2000);

        consumer1.close();
        consumer2.close();
        producer1.close();
        producer2.close();
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
        final String subName = "my-subscriber-name";

        final int messageRate = 5;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(messageRate)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(360)
                .build();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setSubscriptionDispatchRate(namespace, dispatchRate);

        final int numProducedMessages = 500;
        final AtomicInteger totalReceived = new AtomicInteger(0);
        // enable throttling for nonBacklog consumers
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);

        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName)
            .subscriptionName(subName).subscriptionType(SubscriptionType.Shared).messageListener((c1, msg) -> {
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

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();

        DispatchRateLimiter subRateLimiter = null;
        Dispatcher subDispatcher = topic.getSubscription(subName).getDispatcher();
        if (subDispatcher instanceof PersistentDispatcherMultipleConsumers) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else if (subDispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else {
            Assert.fail("Should only have PersistentDispatcher in this test");
        }

        boolean isMessageRateUpdate = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (subRateLimiter.getDispatchRateOnMsg() > 0
                || subRateLimiter.getDispatchRateOnByte() > 0) {
                isMessageRateUpdate = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(isMessageRateUpdate);
        Assert.assertEquals(admin.namespaces().getSubscriptionDispatchRate(namespace), dispatchRate);

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
        final String subName = "my-subscriber-name";

        final int messageRate = 5;
        // enable throttling for nonBacklog consumers
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);

        int initValue = pulsar.getConfiguration().getDispatchThrottlingRatePerSubscriptionInMsg();
        // (1) Update message-dispatch-rate limit
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRatePerSubscriptionInMsg",
            Integer.toString(messageRate));
        // sleep incrementally as zk-watch notification is async and may take some time
        for (int i = 0; i < 5; i++) {
            if (pulsar.getConfiguration().getDispatchThrottlingRatePerSubscriptionInMsg() == initValue) {
                Thread.sleep(50 + (i * 10));
            }
        }
        Assert.assertNotEquals(pulsar.getConfiguration().getDispatchThrottlingRatePerSubscriptionInMsg(), initValue);

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        int numMessages = 500;

        final AtomicInteger totalReceived = new AtomicInteger(0);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
            .subscriptionType(subscription).messageListener((c1, msg) -> {
                Assert.assertNotNull(msg, "Message cannot be null");
                String receivedMessage = new String(msg.getData());
                log.debug("Received message [{}] in the listener", receivedMessage);
                totalReceived.incrementAndGet();
            }).subscribe();

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
        pulsar.getConfiguration().setDispatchThrottlingRatePerSubscriptionInMsg(initValue);
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
        final String subName1 = "my-subscriber-name1";
        final String subName2 = "my-subscriber-name2";

        final int clusterMessageRate = 100;
        // enable throttling for nonBacklog consumers
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);

        int initValue = pulsar.getConfiguration().getDispatchThrottlingRatePerSubscriptionInMsg();
        // (1) Update message-dispatch-rate limit
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRatePerSubscriptionInMsg",
            Integer.toString(clusterMessageRate));
        // sleep incrementally as zk-watch notification is async and may take some time
        for (int i = 0; i < 5; i++) {
            if (pulsar.getConfiguration().getDispatchThrottlingRatePerSubscriptionInMsg() == initValue) {
                Thread.sleep(50 + (i * 10));
            }
        }
        Assert.assertNotEquals(pulsar.getConfiguration().getDispatchThrottlingRatePerSubscriptionInMsg(), initValue);

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));

        // create producer and topic
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName1).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName1).get();

        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topicName1).subscriptionName(subName1)
            .subscribe();

        DispatchRateLimiter subRateLimiter = null;
        Dispatcher subDispatcher = topic.getSubscription(subName1).getDispatcher();
        if (subDispatcher instanceof PersistentDispatcherMultipleConsumers) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else if (subDispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else {
            Assert.fail("Should only have PersistentDispatcher in this test");
        }

        // (1) Update dispatch rate on cluster-config update
        Assert.assertEquals(clusterMessageRate, subRateLimiter.getDispatchRateOnMsg());

        // (2) Update namespace throttling limit
        int nsMessageRate = 500;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(nsMessageRate)
                .dispatchThrottlingRateInByte(0)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().setSubscriptionDispatchRate(namespace, dispatchRate);

        subRateLimiter = subDispatcher.getRateLimiter().get();

        for (int i = 0; i < 5; i++) {
            if (subRateLimiter.getDispatchRateOnMsg() != nsMessageRate) {
                Thread.sleep(50 + (i * 10));
            }
        }
        Assert.assertEquals(nsMessageRate, subRateLimiter.getDispatchRateOnMsg());

        // (3) Disable namespace throttling limit will force to take cluster-config
        dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(0)
                .dispatchThrottlingRateInByte(0)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().setSubscriptionDispatchRate(namespace, dispatchRate);
        for (int i = 0; i < 5; i++) {
            if (subRateLimiter.getDispatchRateOnMsg() == nsMessageRate) {
                Thread.sleep(50 + (i * 10));
            }
        }
        Assert.assertEquals(clusterMessageRate, subRateLimiter.getDispatchRateOnMsg());

        // (5) Namespace throttling is disabled so, new topic should take cluster throttling limit
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2).create();
        PersistentTopic topic2 = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName2).get();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic(topicName2).subscriptionName(subName2)
            .subscribe();

        subDispatcher = topic2.getSubscription(subName2).getDispatcher();
        if (subDispatcher instanceof PersistentDispatcherMultipleConsumers) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else if (subDispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
            subRateLimiter = subDispatcher.getRateLimiter().get();
        } else {
            Assert.fail("Should only have PersistentDispatcher in this test");
        }

        Assert.assertEquals(clusterMessageRate, subRateLimiter.getDispatchRateOnMsg());

        producer.close();
        producer2.close();

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "subscriptions", timeOut = 10000)
    public void testClosingRateLimiter(SubscriptionType subscription) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_ns";
        final String topicName = "persistent://" + namespace + "/closingSubRateLimiter" + subscription.name();
        final String subName = "mySubscription" + subscription.name();

        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(1024)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().setSubscriptionDispatchRate(namespace, dispatchRate);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(subscription).subscribe();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        PersistentSubscription sub = topic.getSubscription(subName);

        final int numProducedMessages = 10;

        for (int i = 0; i < numProducedMessages; i++) {
            final String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        for (int i = 0; i < numProducedMessages; i++) {
            Message<byte[]> msg = consumer.receive();
            consumer.acknowledge(msg);
        }

        Dispatcher dispatcher = sub.getDispatcher();
        Assert.assertTrue(dispatcher.getRateLimiter().isPresent());
        DispatchRateLimiter dispatchRateLimiter = dispatcher.getRateLimiter().get();

        producer.close();
        consumer.close();
        sub.disconnect().get();

        // Make sure that the rate limiter is closed
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), -1);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), -1);

        log.info("-- Exiting {} test --", methodName);
    }
}
