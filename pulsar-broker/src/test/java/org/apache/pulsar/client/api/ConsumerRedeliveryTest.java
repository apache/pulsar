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

import lombok.Cleanup;

import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

@Test(groups = "broker-api")
public class ConsumerRedeliveryTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(ConsumerRedeliveryTest.class);

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setManagedLedgerCacheEvictionFrequency(0.1);
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
}
