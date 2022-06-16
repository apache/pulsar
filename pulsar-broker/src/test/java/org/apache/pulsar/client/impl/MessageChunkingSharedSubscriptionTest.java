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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test message chunking with Shared subscriptions.
 */
@Slf4j
public class MessageChunkingSharedSubscriptionTest extends ProducerConsumerBase  {

    private static final int MAX_MESSAGE_SIZE = 50;
    private final List<List<Message<byte[]>>> messagesList = new ArrayList<>();
    private final List<Consumer<byte[]>> consumerList = new ArrayList<>();

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        this.conf.setMaxMessageSize(MAX_MESSAGE_SIZE);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanConsumersAndMessages() throws Exception {
        messagesList.clear();
        for (Consumer<byte[]> consumer : consumerList) {
            consumer.close();
        }
        consumerList.clear();
    }

    @Test
    public void testChunkedMessage() throws Exception {
        final String topic = "test-chunked-message";
        final int numConsumers = 3;
        for (int i = 0; i < numConsumers; i++) {
            addSharedConsumer(topic, 1000);
        }
        // The number of chunks are greater than `dispatcherMaxRoundRobinBatchSize` config
        final int valueSize = MAX_MESSAGE_SIZE * conf.getDispatcherMaxRoundRobinBatchSize() + 1;
        final byte[] value = createMessagePayload(valueSize);
        @Cleanup final Producer<byte[]> producer = createProducer(topic);
        final ChunkMessageIdImpl chunkMsgId = (ChunkMessageIdImpl) producer.newMessage().value(value).send();
        final long numChunks = getNumOfChunks(chunkMsgId);
        log.info("{} chunks were sent to {}", numChunks, chunkMsgId);
        assertTrue(numChunks > conf.getDispatcherMaxRoundRobinBatchSize());

        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() ->
                messagesList.stream().map(List::size).reduce(0, Integer::sum) > 0);

        // Only 1 message could be received
        final List<List<Message<byte[]>>> nonEmptyMessagesList =
                messagesList.stream().filter(l -> !l.isEmpty()).collect(Collectors.toList());
        assertEquals(nonEmptyMessagesList.size(), 1);
        assertEquals(nonEmptyMessagesList.get(0).size(), 1);
        assertEquals(nonEmptyMessagesList.get(0).get(0).getValue(), value);
    }

    @Test(timeOut = 10000)
    public void testPermitsLimit() throws Exception {
        final String topic = "test-permits-limit";
        final int numConsumers = 10;
        final int consumerIndexWithEnoughPermits = 7;
        for (int i = 0; i < numConsumers; i++) {
            if (i != consumerIndexWithEnoughPermits) {
                addSharedConsumer(topic, 1);
            } else {
                addSharedConsumer(topic, 1000);
            }
        }

        @Cleanup final Producer<byte[]> producer = createProducer(topic);
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            final byte[] value = createMessagePayload(5 * MAX_MESSAGE_SIZE + 1);
            values.add(value);
            final ChunkMessageIdImpl chunkMsgId = (ChunkMessageIdImpl) producer.newMessage().value(value).send();
            final long numChunks = getNumOfChunks(chunkMsgId);
            log.info("[{}] {} chunks were sent to {}", i, numChunks, chunkMsgId);
        }

        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() ->
                messagesList.stream().map(List::size).reduce(0, Integer::sum) >= values.size());

        // Only 1 message could be received
        final List<List<Message<byte[]>>> nonEmptyMessagesList =
                messagesList.stream().filter(l -> !l.isEmpty()).collect(Collectors.toList());
        assertEquals(nonEmptyMessagesList.size(), 1);
        assertSame(nonEmptyMessagesList.get(0), messagesList.get(consumerIndexWithEnoughPermits));
        final List<byte[]> receivedValues = nonEmptyMessagesList.get(0).stream()
                .map(Message::getValue).collect(Collectors.toList());
        assertEquals(receivedValues.size(), values.size());
        for (int i = 0; i < values.size(); i++) {
            assertEquals(receivedValues.get(i), values.get(i));
        }
    }

    private Producer<byte[]> createProducer(final String topic) throws PulsarClientException {
        return pulsarClient.newProducer().topic(topic)
                .enableChunking(true)
                .enableBatching(false)
                .create();
    }

    private void addSharedConsumer(final String topic,
                                   final Integer queueSize) throws PulsarClientException {
        final List<Message<byte[]>> messages = Collections.synchronizedList(new ArrayList<>());
        messagesList.add(messages);
        consumerList.add(pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(queueSize)
                .messageListener((MessageListener<byte[]>) (consumer, msg) -> {
                    messages.add(msg);
                    consumer.acknowledgeAsync(msg);
                })
                .subscribe());
    }

    private static long getNumOfChunks(final ChunkMessageIdImpl chunkMessageId) {
        // assuming all chunks were in the same ledger
        return chunkMessageId.getEntryId() - chunkMessageId.getFirstChunkMessageId().getEntryId();
    }

    private static byte[] createMessagePayload(final int size) {
        final byte[] bytes = new byte[size];
        final Random random = new Random();
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) random.nextInt(10);
        }
        return bytes;
    }
}
