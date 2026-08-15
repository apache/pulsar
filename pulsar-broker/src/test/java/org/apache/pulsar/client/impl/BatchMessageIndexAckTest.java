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
package org.apache.pulsar.client.impl;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-impl")
public class BatchMessageIndexAckTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        doReturn(CompletableFuture.completedFuture(new LedgerMetadata() {
            @Override
            public long getLedgerId() {
                return 0;
            }

            @Override
            public int getEnsembleSize() {
                return 0;
            }

            @Override
            public int getWriteQuorumSize() {
                return 0;
            }

            @Override
            public int getAckQuorumSize() {
                return 0;
            }

            @Override
            public long getLastEntryId() {
                return 0;
            }

            @Override
            public long getLength() {
                return 0;
            }

            @Override
            public boolean hasPassword() {
                return false;
            }

            @Override
            public byte[] getPassword() {
                return new byte[0];
            }

            @Override
            public DigestType getDigestType() {
                return null;
            }

            @Override
            public long getCtime() {
                return 0;
            }

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public Map<String, byte[]> getCustomMetadata() {
                return null;
            }

            @Override
            public List<BookieId> getEnsembleAt(long entryId) {
                return null;
            }

            @Override
            public NavigableMap<Long, ? extends List<BookieId>> getAllEnsembles() {
                return null;
            }

            @Override
            public State getState() {
                return null;
            }

            @Override
            public String toSafeString() {
                return null;
            }

            @Override
            public int getMetadataFormatVersion() {
                return 0;
            }

            @Override
            public long getCToken() {
                return 0;
            }
        })).when(pulsarTestContext.getBookKeeperClient()).getLedgerMetadata(anyLong());
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "ackReceiptEnabled")
    public Object[][] ackReceiptEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @DataProvider(name = "sharedDispatcherAndRemainingMessages")
    public Object[][] sharedDispatcherAndRemainingMessages() {
        return new Object[][] {
                {false, 3},
                {false, 1},
                {true, 3},
                {true, 1}
        };
    }

    @Test(timeOut = 30000, dataProvider = "sharedDispatcherAndRemainingMessages")
    public void testPartialBatchRedeliveryKeepsSharedPermitBalance(boolean classicDispatcher,
                                                                   int remainingMessages) throws Exception {
        final int batchSize = 10;
        final String topic = "persistent://my-property/my-ns/partial-redelivery-" + UUID.randomUUID();
        final String subscriptionName = "shared-sub";
        conf.setSubscriptionSharedUseClassicPersistentImplementation(classicDispatcher);

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(batchSize)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();
        @Cleanup
        Consumer<Integer> firstConsumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(1)
                .enableBatchIndexAcknowledgment(true)
                .isAckReceiptEnabled(true)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscribe();

        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            sendFutures.add(producer.sendAsync(i));
        }
        producer.flush();
        FutureUtil.waitForAll(sendFutures).get(10, TimeUnit.SECONDS);

        List<Message<Integer>> firstDelivery = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            Message<Integer> message = firstConsumer.receive(10, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            firstDelivery.add(message);
        }
        int acknowledgedMessages = batchSize - remainingMessages;
        for (int i = 0; i < acknowledgedMessages; i++) {
            firstConsumer.acknowledge(firstDelivery.get(i));
        }
        firstDelivery.forEach(Message::release);
        firstConsumer.close();

        @Cleanup
        Consumer<Integer> replacementConsumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(1)
                .enableBatchIndexAcknowledgment(true)
                .isAckReceiptEnabled(true)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscribe();

        Set<Integer> expectedValues = new HashSet<>();
        for (int i = acknowledgedMessages; i < batchSize; i++) {
            expectedValues.add(i);
        }
        for (int i = 0; i < remainingMessages; i++) {
            Message<Integer> message = replacementConsumer.receive(10, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            try {
                Assert.assertTrue(expectedValues.remove(message.getValue()));
                replacementConsumer.acknowledge(message);
            } finally {
                message.release();
            }
        }
        Assert.assertTrue(expectedValues.isEmpty());
        Assert.assertNull(replacementConsumer.receive(1, TimeUnit.SECONDS));

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic)
                .orElseThrow(() -> new IllegalStateException("Topic was not loaded"));
        PersistentSubscription subscription = topicRef.getSubscription(subscriptionName);
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Assert.assertEquals(subscription.getConsumers().size(), 1);
            org.apache.pulsar.broker.service.Consumer brokerConsumer = subscription.getConsumers().get(0);
            Assert.assertEquals(brokerConsumer.getAvailablePermits(), 1);
            Assert.assertEquals(brokerConsumer.getUnackedMessages(), 0);
        });

        CompletableFuture<MessageId> nextMessage = producer.sendAsync(batchSize);
        producer.flush();
        nextMessage.get(10, TimeUnit.SECONDS);
        Message<Integer> message = replacementConsumer.receive(10, TimeUnit.SECONDS);
        Assert.assertNotNull(message);
        try {
            Assert.assertEquals(message.getValue(), Integer.valueOf(batchSize));
            replacementConsumer.acknowledge(message);
        } finally {
            message.release();
        }
    }

    @Test(dataProvider = "ackReceiptEnabled")
    public void testBatchMessageIndexAckForSharedSubscription(boolean ackReceiptEnabled) throws Exception {
        final String topic = "testBatchMessageIndexAckForSharedSubscription";
        final String subscriptionName = "sub";

        @Cleanup
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .subscriptionName(subscriptionName)
            .receiverQueueSize(100)
            .isAckReceiptEnabled(ackReceiptEnabled)
            .subscriptionType(SubscriptionType.Shared)
            .negativeAckRedeliveryDelay(2, TimeUnit.SECONDS)
            .subscribe();

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
            .topic(topic)
            .batchingMaxPublishDelay(50, TimeUnit.MILLISECONDS)
            .create();

        final int messages = 100;
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.sendAsync(i));
        }
        FutureUtil.waitForAll(futures).get();

        List<MessageId> acked = new ArrayList<>(50);
        for (int i = 0; i < messages; i++) {
            Message<Integer> msg = consumer.receive();
            if (i % 2 == 0) {
                consumer.acknowledge(msg);
                acked.add(msg.getMessageId());
            } else {
                consumer.negativeAcknowledge(consumer.receive());
            }
        }

        List<MessageId> received = new ArrayList<>(50);
        for (int i = 0; i < 50; i++) {
            received.add(consumer.receive().getMessageId());
        }

        Assert.assertEquals(received.size(), 50);
        acked.retainAll(received);
        Assert.assertEquals(acked.size(), 0);

        for (MessageId messageId : received) {
            consumer.acknowledge(messageId);
        }

        Thread.sleep(1000);

        consumer.redeliverUnacknowledgedMessages();

        Message<Integer> moreMessage = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(moreMessage);

        // check the mark delete position was changed
        BatchMessageIdImpl ackedMessageId = (BatchMessageIdImpl) received.get(0);
        PersistentTopicInternalStats stats = admin.topics().getInternalStats(topic, false);
        String markDeletePosition = stats.cursors.get(subscriptionName).markDeletePosition;
        Assert.assertEquals(ackedMessageId.ledgerId + ":" + ackedMessageId.entryId, markDeletePosition);

        futures.clear();
        for (int i = 0; i < 50; i++) {
            futures.add(producer.sendAsync(i));
        }
        FutureUtil.waitForAll(futures).get();

        for (int i = 0; i < 50; i++) {
            received.add(consumer.receive().getMessageId());
        }

        // Ensure the flow permit is work well since the client skip the acked batch index,
        // broker also need to handle the available permits.
        Assert.assertEquals(received.size(), 100);
    }

    @Test(dataProvider = "ackReceiptEnabled")
    public void testBatchMessageIndexAckForExclusiveSubscription(boolean ackReceiptEnabled) throws
            PulsarClientException, ExecutionException, InterruptedException {
        final String topic = "testBatchMessageIndexAckForExclusiveSubscription";

        @Cleanup
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .subscriptionName("sub")
            .receiverQueueSize(100)
            .isAckReceiptEnabled(ackReceiptEnabled)
            .subscribe();

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
            .topic(topic)
            .batchingMaxPublishDelay(50, TimeUnit.MILLISECONDS)
            .create();

        final int messages = 100;
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.sendAsync(i));
        }
        FutureUtil.waitForAll(futures).get();

        for (int i = 0; i < messages; i++) {
            if (i == 49) {
                consumer.acknowledgeCumulative(consumer.receive());
            } else {
                consumer.receive();
            }
        }

        //Wait ack send.
        Thread.sleep(1000);
        consumer.close();
        consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .subscriptionName("sub")
            .receiverQueueSize(100)
            .subscribe();

        List<Message<Integer>> received = new ArrayList<>(50);
        for (int i = 0; i < 50; i++) {
            received.add(consumer.receive());
        }

        Assert.assertEquals(received.size(), 50);

        Message<Integer> moreMessage = consumer.receive(1, TimeUnit.SECONDS);
        Assert.assertNull(moreMessage);

        futures.clear();
        for (int i = 0; i < 50; i++) {
            futures.add(producer.sendAsync(i));
        }
        FutureUtil.waitForAll(futures).get();

        for (int i = 0; i < 50; i++) {
            received.add(consumer.receive());
        }

        // Ensure the flow permit is work well since the client skip the acked batch index,
        // broker also need to handle the available permits.
        Assert.assertEquals(received.size(), 100);
    }

    @Test
    public void testDoNotRecycleAckSetMultipleTimes() throws Exception  {
        final String topic = "persistent://my-property/my-ns/testSafeAckSetRecycle";

        Producer<byte[]> producer = pulsarClient.newProducer()
                .batchingMaxMessages(10)
                .blockIfQueueFull(true).topic(topic)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .acknowledgmentGroupTime(1, TimeUnit.MILLISECONDS)
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        final int messages = 100;
        for (int i = 0; i < messages; i++) {
            producer.sendAsync("Hello Pulsar".getBytes());
        }

        // Should not throw an exception.
        for (int i = 0; i < messages; i++) {
            consumer.acknowledgeCumulative(consumer.receive());
            // make sure the group ack flushed.
            Thread.sleep(2);
        }

        producer.close();
        consumer.close();
    }
}
