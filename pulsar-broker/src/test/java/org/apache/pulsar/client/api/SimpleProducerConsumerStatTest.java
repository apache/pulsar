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

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
@CustomLog
public class SimpleProducerConsumerStatTest extends SharedPulsarBaseTest {

    private String methodName;
    private PulsarClient client;

    @Override
    protected boolean checkClientHandlesClosed() {
        return true;
    }

    @BeforeMethod
    public void setupClient(Method method) throws Exception {
        methodName = method.getName();
        // the client and producer stats of the tests are updated every second
        client = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .statsInterval(1, TimeUnit.SECONDS)
                .build();
    }

    @AfterMethod(alwaysRun = true)
    public void closeClient() throws Exception {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @DataProvider(name = "batch")
    public Object[][] batchMessageDelayMsProvider() {
        return new Object[][] { { 0 }, { 1000 } };
    }

    @DataProvider(name = "batch_with_timeout")
    public Object[][] ackTimeoutSecProvider() {
        return new Object[][] { { 0, 0 }, { 0, 2 }, { 1000, 0 }, { 1000, 2 } };
    }

    @Test(dataProvider = "batch_with_timeout")
    public void testSyncProducerAndConsumer(int batchMessageDelayMs, int ackTimeoutSec) throws Exception {
        log.info().attr("method", methodName).log("Starting test");
        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer()
                .topic("persistent://" + getNamespace() + "/my-topic1").subscriptionName("my-subscriber-name");

        // Cumulative Ack-counter works if ackTimeOutTimer-task is enabled
        boolean isAckTimeoutTaskEnabledForCumulativeAck = ackTimeoutSec > 0;
        if (ackTimeoutSec > 0) {
            consumerBuilder.ackTimeout(ackTimeoutSec, TimeUnit.SECONDS);
        }

        Consumer<byte[]> consumer = consumerBuilder.subscribe();

        ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .topic("persistent://" + getNamespace() + "/my-topic1");
        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true).batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        }

        Producer<byte[]> producer = producerBuilder.create();

        int numMessages = 11;
        for (int i = 0; i < numMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = new HashSet<>();
        for (int i = 0; i < numMessages; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info().attr("message", receivedMessage).log("Received message");
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        waitForStatsRecorded(consumer, producer, numMessages, isAckTimeoutTaskEnabledForCumulativeAck);
        consumer.close();
        producer.close();
        validatingLogInfo(consumer, producer, isAckTimeoutTaskEnabledForCumulativeAck);
        log.info().attr("method", methodName).log("Exiting test");
    }

    @Test(dataProvider = "batch_with_timeout")
    public void testAsyncProducerAndAsyncAck(int batchMessageDelayMs, int ackTimeoutSec) throws Exception {
        log.info().attr("method", methodName).log("Starting test");
        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer()
                .topic("persistent://" + getNamespace() + "/my-topic2").subscriptionName("my-subscriber-name");
        if (ackTimeoutSec > 0) {
            consumerBuilder.ackTimeout(ackTimeoutSec, TimeUnit.SECONDS);
        }

        Consumer<byte[]> consumer = consumerBuilder.subscribe();

        ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .topic("persistent://" + getNamespace() + "/my-topic2")
                .messageRoutingMode(MessageRoutingMode.SinglePartition);
        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true).batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        } else {
            producerBuilder.enableBatching(false);
        }

        Producer<byte[]> producer = producerBuilder.create();
        List<Future<MessageId>> futures = new ArrayList<>();

        int numMessages = 50;
        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            final String message = "my-message-" + i;
            Future<MessageId> future = producer.sendAsync(message.getBytes());
            futures.add(future);
        }

        log.info("Waiting for async publish to complete");
        for (Future<MessageId> future : futures) {
            future.get();
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = new HashSet<>();
        for (int i = 0; i < numMessages; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Asynchronously acknowledge upto and including the last message
        Future<Void> ackFuture = consumer.acknowledgeCumulativeAsync(msg);
        log.info("Waiting for async ack to complete");
        ackFuture.get();
        waitForStatsRecorded(consumer, producer, numMessages, batchMessageDelayMs == 0 && ackTimeoutSec > 0);
        consumer.close();
        producer.close();
        validatingLogInfo(consumer, producer, batchMessageDelayMs == 0 && ackTimeoutSec > 0);
        log.info().attr("method", methodName).log("Exiting test");
    }

    @Test(dataProvider = "batch_with_timeout")
    public void testAsyncProducerAndReceiveAsyncAndAsyncAck(int batchMessageDelayMs, int ackTimeoutSec)
            throws Exception {
        log.info().attr("method", methodName).log("Starting test");
        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer()
                .topic("persistent://" + getNamespace() + "/my-topic2").subscriptionName("my-subscriber-name");
        if (ackTimeoutSec > 0) {
            consumerBuilder.ackTimeout(ackTimeoutSec, TimeUnit.SECONDS);
        }

        Consumer<byte[]> consumer = consumerBuilder.subscribe();

        ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .topic("persistent://" + getNamespace() + "/my-topic2")
                .messageRoutingMode(MessageRoutingMode.SinglePartition);
        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true).batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        } else {
            producerBuilder.enableBatching(false);
        }

        Producer<byte[]> producer = producerBuilder.create();
        List<Future<MessageId>> futures = new ArrayList<>();

        int numMessages = 101;
        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            final String message = "my-message-" + i;
            Future<MessageId> future = producer.sendAsync(message.getBytes());
            futures.add(future);
        }

        log.info("Waiting for async publish to complete");
        for (Future<MessageId> future : futures) {
            future.get();
        }
        Message<byte[]> msg = null;
        CompletableFuture<Message<byte[]>> futureMsg = null;
        Set<String> messageSet = new HashSet<>();
        for (int i = 0; i < numMessages; i++) {
            futureMsg = consumer.receiveAsync();
            Thread.sleep(10);
            msg = futureMsg.get();
            String receivedMessage = new String(msg.getData());
            log.info().attr("message", receivedMessage).log("Received message");
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Asynchronously acknowledge upto and including the last message
        Future<Void> ackFuture = consumer.acknowledgeCumulativeAsync(msg);
        log.info("Waiting for async ack to complete");
        ackFuture.get();
        waitForStatsRecorded(consumer, producer, numMessages, batchMessageDelayMs == 0 && ackTimeoutSec > 0);
        consumer.close();
        producer.close();
        validatingLogInfo(consumer, producer, batchMessageDelayMs == 0 && ackTimeoutSec > 0);
        log.info().attr("method", methodName).log("Exiting test");
    }

    @Test(dataProvider = "batch", timeOut = 100000)
    public void testMessageListener(int batchMessageDelayMs) throws Exception {
        log.info().attr("method", methodName).log("Starting test");

        int numMessages = 100;
        final CountDownLatch latch = new CountDownLatch(numMessages);

        Consumer<byte[]> consumer = client.newConsumer().topic("persistent://" + getNamespace() + "/my-topic3")
                .subscriptionName("my-subscriber-name").ackTimeout(100, TimeUnit.SECONDS)
                .messageListener((consumer1, msg) -> {
                    assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug().attr("message", receivedMessage).log("Received message in the listener");
                    consumer1.acknowledgeAsync(msg);
                    latch.countDown();
                }).subscribe();

        ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .topic("persistent://" + getNamespace() + "/my-topic3");
        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true).batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        }

        Producer<byte[]> producer = producerBuilder.create();
        List<Future<MessageId>> futures = new ArrayList<>();

        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            final String message = "my-message-" + i;
            Future<MessageId> future = producer.sendAsync(message.getBytes());
            futures.add(future);
        }

        log.info("Waiting for async publish to complete");
        for (Future<MessageId> future : futures) {
            future.get();
        }
        log.info("Waiting for message listener to ack all messages");
        assertTrue(latch.await(numMessages, TimeUnit.SECONDS), "Timed out waiting for message listener acks");
        waitForStatsRecorded(consumer, producer, numMessages, true);
        consumer.close();
        producer.close();
        validatingLogInfo(consumer, producer, true);
        log.info().attr("method", methodName).log("Exiting test");
    }

    /**
     * Waits until the stats of the consumer and producer include every message and acknowledgment. The stats timers,
     * which run every second, add them to the totals; closing the consumer and producer stops the timers.
     */
    @SuppressWarnings("deprecation")
    private void waitForStatsRecorded(Consumer<?> consumer, Producer<?> producer, int numMessages,
                                      boolean verifyAckCount) {
        Awaitility.await().untilAsserted(() -> {
            ConsumerStats cStat = consumer.getStats();
            ProducerStats pStat = producer.getStats();
            assertEquals(pStat.getTotalMsgsSent(), numMessages);
            assertEquals(pStat.getTotalAcksReceived(), numMessages);
            assertEquals(cStat.getTotalMsgsReceived(), numMessages);
            if (verifyAckCount) {
                assertEquals(cStat.getTotalAcksSent(), numMessages);
            }
        });
    }
    @SuppressWarnings("deprecation")

    private void validatingLogInfo(Consumer<?> consumer, Producer<?> producer, boolean verifyAckCount) {
        ConsumerStats cStat = consumer.getStats();
        ProducerStats pStat = producer.getStats();
        assertEquals(pStat.getTotalMsgsSent(), cStat.getTotalMsgsReceived());
        assertEquals(pStat.getTotalBytesSent(), cStat.getTotalBytesReceived());
        assertEquals(pStat.getTotalMsgsSent(), pStat.getTotalAcksReceived());
        if (verifyAckCount) {
            assertEquals(cStat.getTotalMsgsReceived(), cStat.getTotalAcksSent());
        }
    }

    /**
     * This test verifies partitioned topic stats for producer and consumer.
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testPartitionTopicStats() throws Exception {
        log.info().attr("method", methodName).log("Starting test");

        String topicName = "persistent://" + getNamespace() + "/testPartitionTopicStats";
        int numPartitions = 10;
        admin.topics().createPartitionedTopic(topicName, numPartitions);

        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer().topic(topicName)
                .subscriptionName("my-subscriber-name");

        Consumer<byte[]> consumer = consumerBuilder.subscribe();

        ProducerBuilder<byte[]> producerBuilder = client.newProducer().enableBatching(false).topic(topicName);

        Producer<byte[]> producer = producerBuilder.create();

        int numMessages = 20;
        for (int i = 0; i < numMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = new HashSet<>();
        for (int i = 0; i < numMessages; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info().attr("message", receivedMessage).log("Received message");
            Assert.assertTrue(messageSet.add(receivedMessage), "Received duplicate message " + receivedMessage);
        }
        Assert.assertEquals(messageSet.size(), numMessages);

        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);

        ConsumerStats cStat = consumer.getStats();
        ProducerStats pStat = producer.getStats();
        retryStrategically((test) -> !pStat.getPartitionStats().isEmpty(), 5, 100);
        retryStrategically((test) -> !cStat.getPartitionStats().isEmpty(), 5, 100);
        Map<String, ProducerStats> prodStatsMap = pStat.getPartitionStats();
        Map<String, ConsumerStats> consStatsMap = cStat.getPartitionStats();
        assertFalse(prodStatsMap.isEmpty());
        assertFalse(consStatsMap.isEmpty());
        for (int i = 0; i < numPartitions; i++) {
            String topic = topicName + "-partition-" + i;
            assertTrue(prodStatsMap.containsKey(topic));
            assertTrue(consStatsMap.containsKey(topic));
        }

        consumer.close();
        producer.close();

        log.info().attr("method", methodName).log("Exiting test");
    }

    @Test
    public void testMsgRateExpired() throws Exception {
        log.info().attr("method", methodName).log("Starting test");

        String topicName = "persistent://" + getNamespace() + "/" + methodName;
        String subName = "my-sub";
        admin.topics().createSubscription(topicName, subName, MessageId.latest);

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .create();

        int numMessages = 100;
        for (int i = 0; i < numMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Thread.sleep(2000);
        admin.topics().expireMessages(topicName, subName, 1);
        getPulsar().getBrokerService().updateRates();

        Awaitility.await().ignoreExceptions().timeout(10, TimeUnit.SECONDS)
                .until(() -> getPulsar().getBrokerService().getTopicStats().get(topicName)
                        .getSubscriptions().get(subName).getTotalMsgExpired() > 0);

        Awaitility.await().ignoreExceptions().timeout(10, TimeUnit.SECONDS).until(() -> {
            getPulsar().getBrokerService().updateRates();
            return getPulsar().getBrokerService().getTopicStats().get(topicName)
                    .getSubscriptions().get(subName).getMsgRateExpired() < 0.001;
        });

        assertEquals(getPulsar().getBrokerService().getTopicStats().get(topicName)
                        .getSubscriptions().get(subName).getMsgRateExpired(), 0.0, 0.001);
        assertEquals(getPulsar().getBrokerService().getTopicStats().get(topicName)
                        .getSubscriptions().get(subName).getTotalMsgExpired(), numMessages);

        log.info().attr("method", methodName).log("Exiting test");
    }
    @SuppressWarnings("deprecation")

    @Test
    public void testRetryLetterAndDeadLetterStats() throws PulsarClientException, InterruptedException {
        final String topicName = "persistent://" + getNamespace() + "/testRetryLetterAndDeadLetterStats";

        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .negativeAckRedeliveryDelay(100, TimeUnit.MILLISECONDS)
                .enableRetry(true)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(3)
                        .retryLetterTopic("persistent://" + getNamespace() + "/retry-topic")
                        .deadLetterTopic("persistent://" + getNamespace() + "/dlq-topic")
                        .build())
                .subscriptionName("sub")
                .subscribe();

        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .create();

        final int messages = 1;
        for (int i = 0; i < messages; i++) {
            producer.send(("message-" + i).getBytes());
        }

        for (int i = 0; i < messages * 4; i++) {
            // nack and reconsumeLater
            Message msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg != null) {
                consumer.reconsumeLater(msg, 100, TimeUnit.MILLISECONDS);
            }
        }

        Awaitility.await().untilAsserted(() -> {
            ConsumerStats stats = consumer.getStats();
            ProducerStats retryStats = stats.getRetryLetterProducerStats();
            ProducerStats deadLetterStats = stats.getDeadLetterProducerStats();
            assertNotNull(retryStats);
            assertNotNull(deadLetterStats);
            assertEquals(retryStats.getTotalMsgsSent(), 3);
            assertEquals(deadLetterStats.getTotalMsgsSent(), 1);
        });
    }
    @SuppressWarnings("deprecation")
    @Test
    public void testDeadLetterStats() throws PulsarClientException, InterruptedException {
        final String topicName = "persistent://" + getNamespace() + "/testDeadLetterStats";

        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .negativeAckRedeliveryDelay(100, TimeUnit.MILLISECONDS)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(1)
                        .deadLetterTopic("persistent://" + getNamespace() + "/dlq-topic")
                        .build())
                .subscriptionName("sub")
                .subscribe();

        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .create();

        final int messages = 1;
        for (int i = 0; i < messages; i++) {
            producer.send(("message-" + i).getBytes());
        }

        for (int i = 0; i < messages * 2; i++) {
            // nack and reconsumeLater
            Message msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg != null) {
                consumer.negativeAcknowledge(msg);
            }
        }

        Awaitility.await().untilAsserted(() -> {
            ConsumerStats stats = consumer.getStats();
            ProducerStats dlqStats = stats.getDeadLetterProducerStats();
            assertNotNull(dlqStats);
            assertEquals(dlqStats.getTotalMsgsSent(), 1);
        });
    }
    @SuppressWarnings("deprecation")

    @Test
    public void testPartitionedRetryLetterAndDeadLetterStats()
            throws PulsarClientException, InterruptedException, PulsarAdminException {
        final String topicName = "persistent://" + getNamespace() + "/testPartitionedRetryLetterAndDeadLetterStats";

        admin.topics().createPartitionedTopic(topicName, 10);
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .negativeAckRedeliveryDelay(100, TimeUnit.MILLISECONDS)
                .enableRetry(true)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(3)
                        .retryLetterTopic("persistent://" + getNamespace() + "/retry-topic")
                        .deadLetterTopic("persistent://" + getNamespace() + "/dlq-topic")
                        .build())
                .subscriptionName("sub")
                .subscribe();

        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();

        final int messages = 30;
        for (int i = 0; i < messages; i++) {
            producer.send(("message-" + i).getBytes());
        }

        for (int i = 0; i < messages * 4; i++) {
            // nack and reconsumeLater
            Message msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg != null) {
                consumer.reconsumeLater(msg, 100, TimeUnit.MILLISECONDS);
            }
        }

        Awaitility.await().untilAsserted(() -> {
            ConsumerStats stats = consumer.getStats();
            ProducerStats retryStats = stats.getRetryLetterProducerStats();
            ProducerStats deadLetterStats = stats.getDeadLetterProducerStats();
            assertNotNull(retryStats);
            assertNotNull(deadLetterStats);
            assertEquals(retryStats.getTotalMsgsSent(), 3 * messages);
            assertEquals(deadLetterStats.getTotalMsgsSent(), messages);
        });
    }

    private <T> void testMessageOrderAndDuplicates(Set<T> messagesReceived, T receivedMessage,
            T expectedMessage) {
        // Make sure that messages are received in order
        Assert.assertEquals(receivedMessage, expectedMessage,
                "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);

        // Make sure that there are no duplicates
        Assert.assertTrue(messagesReceived.add(receivedMessage), "Received duplicate message " + receivedMessage);
    }
}
