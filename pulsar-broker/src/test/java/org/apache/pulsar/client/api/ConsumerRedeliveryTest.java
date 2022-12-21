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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

@Test(groups = "broker-api")
public class ConsumerRedeliveryTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(ConsumerRedeliveryTest.class);

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setManagedLedgerCacheEvictionIntervalMs(10000);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "ackReceiptEnabled")
    public Object[][] ackReceiptEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @DataProvider(name = "batchedMessageAck")
    public Object[][] batchedMessageAck() {
        // When batch index ack is disabled (by default), only after all single messages were sent would the pending
        // ACK be added into the ACK tracker.
        return new Object[][] {
                // numAcked, batchSize, ack type
                { 3, 5, CommandAck.AckType.Individual },
                { 5, 5, CommandAck.AckType.Individual },
                { 3, 5, CommandAck.AckType.Cumulative },
                { 5, 5, CommandAck.AckType.Cumulative }
        };
    }

    /**
     * It verifies that redelivered messages are sorted based on the ledger-ids.
     * <pre>
     * 1. client publishes 100 messages across 50 ledgers
     * 2. broker delivers 100 messages to consumer
     * 3. consumer ack every alternative message and doesn't ack 50 messages
     * 4. broker sorts replay messages based on ledger and redelivers messages ledger by ledger
     * </pre>
     * @throws Exception
     */
    @Test(dataProvider = "ackReceiptEnabled")
    public void testOrderedRedelivery(boolean ackReceiptEnabled) throws Exception {
        String topic = "persistent://my-property/my-ns/redelivery-" + System.currentTimeMillis();

        conf.setManagedLedgerMaxEntriesPerLedger(2);
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("my-producer-name")
                .create();
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topic).subscriptionName("s1")
                .subscriptionType(SubscriptionType.Shared)
                .isAckReceiptEnabled(ackReceiptEnabled);
        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) consumerBuilder.subscribe();

        final int totalMsgs = 100;

        for (int i = 0; i < totalMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }


        int consumedCount = 0;
        Set<MessageId> messageIds = Sets.newHashSet();
        for (int i = 0; i < totalMsgs; i++) {
            Message<byte[]> message = consumer1.receive(5, TimeUnit.SECONDS);
            if (message != null && (consumedCount % 2) == 0) {
                consumer1.acknowledge(message);
            } else {
                messageIds.add(message.getMessageId());
            }
            consumedCount += 1;
        }
        assertEquals(totalMsgs, consumedCount);

        // redeliver all unack messages
        consumer1.redeliverUnacknowledgedMessages(messageIds);

        MessageIdImpl lastMsgId = null;
        for (int i = 0; i < totalMsgs / 2; i++) {
            Message<byte[]> message = consumer1.receive(5, TimeUnit.SECONDS);
            MessageIdImpl msgId = (MessageIdImpl) message.getMessageId();
            if (lastMsgId != null) {
                assertTrue(lastMsgId.getLedgerId() <= msgId.getLedgerId(), "lastMsgId: " + lastMsgId + " -- msgId: " + msgId);
            }
            lastMsgId = msgId;
        }

        // close consumer so, this consumer's unack messages will be redelivered to new consumer
        consumer1.close();

        @Cleanup
        Consumer<byte[]> consumer2 = consumerBuilder.subscribe();
        lastMsgId = null;
        for (int i = 0; i < totalMsgs / 2; i++) {
            Message<byte[]> message = consumer2.receive(5, TimeUnit.SECONDS);
            MessageIdImpl msgId = (MessageIdImpl) message.getMessageId();
            if (lastMsgId != null) {
                assertTrue(lastMsgId.getLedgerId() <= msgId.getLedgerId());
            }
            lastMsgId = msgId;
        }
    }

    @Test(dataProvider = "ackReceiptEnabled")
    public void testUnAckMessageRedeliveryWithReceiveAsync(boolean ackReceiptEnabled) throws PulsarClientException, ExecutionException, InterruptedException {
        String topic = "persistent://my-property/my-ns/async-unack-redelivery";
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("s1")
                .isAckReceiptEnabled(ackReceiptEnabled)
                .enableBatchIndexAcknowledgment(ackReceiptEnabled)
                .ackTimeout(3, TimeUnit.SECONDS)
                .subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(5)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();

        final int messages = 10;
        List<CompletableFuture<Message<String>>> futures = new ArrayList<>(10);
        for (int i = 0; i < messages; i++) {
            futures.add(consumer.receiveAsync());
        }

        for (int i = 0; i < messages; i++) {
            producer.sendAsync("my-message-" + i);
        }

        int messageReceived = 0;
        for (CompletableFuture<Message<String>> future : futures) {
            Message<String> message = future.get();
            assertNotNull(message);
            messageReceived++;
            // Don't ack message, wait for ack timeout.
        }

        assertEquals(10, messageReceived);
        for (int i = 0; i < messages; i++) {
            Message<String> message = consumer.receive();
            assertNotNull(message);
            messageReceived++;
            consumer.acknowledge(message);
        }

        assertEquals(20, messageReceived);

        producer.close();
        consumer.close();
    }

    /**
     * Validates broker should dispatch messages to consumer which still has the permit to consume more messages.
     * 
     * @throws Exception
     */
    @Test
    public void testConsumerWithPermitReceiveBatchMessages() throws Exception {

        log.info("-- Starting {} test --", methodName);

        final int queueSize = 10;
        int batchSize = 100;
        String subName = "my-subscriber-name";
        String topicName = "permitReceiveBatchMessages"+(UUID.randomUUID().toString());
        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .receiverQueueSize(queueSize).subscriptionType(SubscriptionType.Shared).subscriptionName(subName)
                .subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        producerBuilder.enableBatching(true);
        producerBuilder.batchingMaxPublishDelay(2000, TimeUnit.MILLISECONDS);
        producerBuilder.batchingMaxMessages(100);

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < batchSize; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }
        producer.flush();

        for (int i = 0; i < queueSize; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }
        producer.flush();

        retryStrategically((test) -> {
            return consumer1.getTotalIncomingMessages() == batchSize;
        }, 5, 2000);

        assertEquals(consumer1.getTotalIncomingMessages(), batchSize);

        ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .receiverQueueSize(queueSize).subscriptionType(SubscriptionType.Shared).subscriptionName(subName)
                .subscribe();

        retryStrategically((test) -> {
            return consumer2.getTotalIncomingMessages() == queueSize;
        }, 5, 2000);
        assertEquals(consumer2.getTotalIncomingMessages(), queueSize);
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 30000)
    public void testMessageRedeliveryAfterUnloadedWithEarliestPosition() throws Exception {

        final String subName = "my-subscriber-name";
        final String topicName = "testMessageRedeliveryAfterUnloadedWithEarliestPosition" + UUID.randomUUID();
        final int messages = 100;

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();

        List<CompletableFuture<MessageId>> sendResults = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            sendResults.add(producer.sendAsync("Hello - " + i));
        }
        producer.flush();

        FutureUtil.waitForAll(sendResults).get();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(subName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        List<Message<String>> received = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            received.add(consumer.receive());
        }

        assertEquals(received.size(), messages);
        assertNull(consumer.receive(1, TimeUnit.SECONDS));

        admin.topics().unload(topicName);

        // The consumer does not ack any messages, so after unloading the topic,
        // the consumer should get the unacked messages again

        received.clear();
        for (int i = 0; i < messages; i++) {
            received.add(consumer.receive());
        }

        assertEquals(received.size(), messages);
        assertNull(consumer.receive(1, TimeUnit.SECONDS));

        consumer.close();
        producer.close();
    }

    @Test(timeOut = 30000, dataProvider = "batchedMessageAck")
    public void testAckNotSent(int numAcked, int batchSize, CommandAck.AckType ackType) throws Exception {
        String topic = "persistent://my-property/my-ns/test-ack-not-sent-"
                + numAcked + "-" + batchSize + "-" + ackType.getValue();
        @Cleanup Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .enableBatchIndexAcknowledgment(false)
                .acknowledgmentGroupTime(1, TimeUnit.HOURS) // ACK won't be sent
                .subscribe();
        @Cleanup Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(batchSize)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();
        for (int i = 0; i < batchSize; i++) {
            String value = "msg-" + i;
            producer.sendAsync(value).thenAccept(id -> log.info("{} was sent to {}", value, id));
        }
        List<Message<String>> messages = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            messages.add(consumer.receive());
        }
        if (ackType == CommandAck.AckType.Individual) {
            for (int i = 0; i < numAcked; i++) {
                consumer.acknowledge(messages.get(i));
            }
        } else {
            consumer.acknowledgeCumulative(messages.get(numAcked - 1));
        }

        consumer.redeliverUnacknowledgedMessages();

        messages.clear();
        for (int i = 0; i < batchSize; i++) {
            Message<String> msg = consumer.receive(2, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            log.info("Received {} from {}", msg.getValue(), msg.getMessageId());
            messages.add(msg);
        }
        List<String> values = messages.stream().map(Message::getValue).collect(Collectors.toList());
        // All messages are redelivered because only if the whole batch are acknowledged would the message ID be
        // added into the ACK tracker.
        if (numAcked < batchSize) {
            assertEquals(values, IntStream.range(0, batchSize).mapToObj(i -> "msg-" + i).collect(Collectors.toList()));
        } else {
            assertTrue(values.isEmpty());
        }
    }
}
