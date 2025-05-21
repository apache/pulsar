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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

@Slf4j
@Test(groups = "broker-impl")
public class ConsumerAckTest extends ProducerConsumerBase {

    private TransactionImpl transaction;
    private PulsarClient clientWithStats;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        this.clientWithStats = newPulsarClient(lookupUrl.toString(), 30);
        transaction = mock(TransactionImpl.class);
        doReturn(1L).when(transaction).getTxnIdLeastBits();
        doReturn(1L).when(transaction).getTxnIdMostBits();
        doReturn(TransactionImpl.State.OPEN).when(transaction).getState();
        CompletableFuture<Void> completableFuture = CompletableFuture.completedFuture(null);
        doNothing().when(transaction).registerAckOp(any());
        doReturn(true).when(transaction).checkIfOpen(any());
        doReturn(completableFuture).when(transaction).registerAckedTopic(any(), any());
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        this.clientWithStats.close();
        super.internalCleanup();
    }

    @Test
    public void testAckResponse() throws PulsarClientException, InterruptedException {
        String topic = "testAckResponse";
        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .enableBatching(false)
                .create();
        @Cleanup
        ConsumerImpl<Integer> consumer = (ConsumerImpl<Integer>) pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscribe();
        producer.send(1);
        producer.send(2);
        try {
            consumer.acknowledgeAsync(new MessageIdImpl(1, 1, 1), transaction).get();
            fail();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof PulsarClientException.NotAllowedException);
        }
        Message<Integer> message = consumer.receive();

        try {
            consumer.acknowledgeAsync(message.getMessageId(), transaction).get();
            fail();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof PulsarClientException.NotAllowedException);
        }
    }
    @Test(timeOut = 30000)
    public void testAckReceipt() throws Exception {
        String topic = "testAckReceipt";
        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .enableBatching(false)
                .create();
        @Cleanup
        ConsumerImpl<Integer> consumer = (ConsumerImpl<Integer>) pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("sub")
                .isAckReceiptEnabled(true)
                .subscribe();
        for (int i = 0; i < 10; i++) {
            producer.send(i);
        }
        Message<Integer> message = consumer.receive();
        MessageId messageId = message.getMessageId();
        consumer.acknowledgeCumulativeAsync(messageId).get();
        consumer.acknowledgeCumulativeAsync(messageId).get();
        consumer.close();
        @Cleanup
        ConsumerImpl<Integer> consumer2 = (ConsumerImpl<Integer>) pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("sub")
                .isAckReceiptEnabled(true)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscribe();
        message = consumer2.receive();
        messageId = message.getMessageId();
        consumer2.acknowledgeCumulativeAsync(messageId).get();
        consumer2.acknowledgeCumulativeAsync(messageId).get();
    }

    @Test
    public void testIndividualAck() throws Exception {
        @Cleanup AckTestData data = prepareDataForAck("test-individual-ack");
        for (MessageId messageId : data.messageIds) {
            data.consumer.acknowledge(messageId);
        }
        assertEquals(data.interceptor.individualAckedMessageIdList, data.messageIds);
        assertEquals(data.consumer.getStats().getNumAcksSent(), data.size());
        assertTrue(data.consumer.getUnAckedMessageTracker().isEmpty());
    }

    @Test
    public void testIndividualAckList() throws Exception {
        @Cleanup AckTestData data = prepareDataForAck("test-individual-ack-list");
        data.consumer.acknowledge(data.messageIds);
        assertEquals(data.interceptor.individualAckedMessageIdList, data.messageIds);
        assertEquals(data.consumer.getStats().getNumAcksSent(), data.size());
        assertTrue(data.consumer.getUnAckedMessageTracker().isEmpty());
    }

    @Test
    public void testCumulativeAck() throws Exception {
        @Cleanup AckTestData data = prepareDataForAck("test-cumulative-ack");
        System.out.println(data.size());
        data.consumer.acknowledgeCumulative(data.messageIds.get(data.size() - 1));
        assertEquals(data.interceptor.cumulativeAckedMessageIdList.get(0),
                data.messageIds.get(data.messageIds.size() - 1));
        assertEquals(data.consumer.getStats().getNumAcksSent(), 2);
        assertTrue(data.consumer.getUnAckedMessageTracker().isEmpty());
    }

    // Send 1 non-batched message, then send N-1 messages that are in the same batch
    private AckTestData prepareDataForAck(String topic) throws PulsarClientException {
        final int numMessages = 10;
        @Cleanup Producer<String> batchProducer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(numMessages - 1)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();
        @Cleanup Producer<String> nonBatchProducer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(false)
                .create();
        AckStatsInterceptor interceptor = new AckStatsInterceptor();
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) clientWithStats.newConsumer(Schema.STRING).topic(topic)
                .subscriptionName("sub").intercept(interceptor).ackTimeout(10, TimeUnit.SECONDS).subscribe();

        nonBatchProducer.send("msg-0");
        for (int i = 1; i < numMessages; i++) {
            batchProducer.sendAsync("msg-" + i);
        }
        List<MessageId> messageIds = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            Message<String> message = consumer.receive(3, TimeUnit.SECONDS);
            assertNotNull(message);
            messageIds.add(message.getMessageId());
        }
        MessageId firstEntryMessageId = messageIds.get(0);
        MessageId secondEntryMessageId = MessageIdAdvUtils.discardBatch(messageIds.get(1));
        // Verify messages 2 to N must be in the same entry
        for (int i = 2; i < messageIds.size(); i++) {
            assertEquals(MessageIdAdvUtils.discardBatch(messageIds.get(i)), secondEntryMessageId);
        }

        assertTrue(interceptor.individualAckedMessageIdList.isEmpty());
        assertTrue(interceptor.cumulativeAckedMessageIdList.isEmpty());
        assertEquals(consumer.getStats().getNumAcksSent(), 0);
        assertNotNull(consumer.getUnAckedMessageTracker().messageIdPartitionMap);
        assertEquals(consumer.getUnAckedMessageTracker().messageIdPartitionMap.keySet(),
                Sets.newHashSet(firstEntryMessageId, secondEntryMessageId));
        return new AckTestData(consumer, interceptor, messageIds);
    }

    // Send 10 messages, the 1st message is a non-batched message, the other messages are in the same batch
    @AllArgsConstructor
    private static class AckTestData implements Closeable {

        private final ConsumerImpl<String> consumer;
        private final AckStatsInterceptor interceptor;
        private final List<MessageId> messageIds;

        public int size() {
            return messageIds.size();
        }

        @Override
        public void close() throws IOException {
            interceptor.close();
            consumer.close();
        }
    }

    private static class AckStatsInterceptor implements ConsumerInterceptor<String> {

        private final List<MessageId> individualAckedMessageIdList = new CopyOnWriteArrayList<>();
        private final List<MessageId> cumulativeAckedMessageIdList = new CopyOnWriteArrayList<>();

        @Override
        public void close() {
            // No ops
        }

        @Override
        public Message<String> beforeConsume(Consumer<String> consumer, Message<String> message) {
            return message;
        }

        @Override
        public void onAcknowledge(Consumer<String> consumer, MessageId messageId, Throwable exception) {
            if (exception != null) {
                log.error("[{}] Failed to acknowledge {}", consumer.getConsumerName(), messageId);
                return;
            }
            individualAckedMessageIdList.add(messageId);
        }

        @Override
        public void onAcknowledgeCumulative(Consumer<String> consumer, MessageId messageId, Throwable exception) {
            if (exception != null) {
                log.error("[{}] Failed to acknowledge {}", consumer.getConsumerName(), messageId);
                return;
            }
            cumulativeAckedMessageIdList.add(messageId);
        }

        @Override
        public void onNegativeAcksSend(Consumer<String> consumer, Set<MessageId> messageIds) {
            // No ops
        }

        @Override
        public void onAckTimeoutSend(Consumer<String> consumer, Set<MessageId> messageIds) {
            // No ops
        }
    }
}
