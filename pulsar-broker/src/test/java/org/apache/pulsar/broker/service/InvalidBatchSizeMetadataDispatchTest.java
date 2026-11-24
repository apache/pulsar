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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Verifies that entries whose {@code num_messages_in_batch} metadata is out of range don't prevent the dispatcher
 * from delivering the entries that follow them.
 */
@Test(groups = "broker")
public class InvalidBatchSizeMetadataDispatchTest extends SharedPulsarBaseTest {

    private static final int INVALID_ENTRY_PAYLOAD_SIZE = 400;
    // each message in a batch takes at least 4 bytes, so the payload holds at most this many messages
    private static final int MAX_MESSAGES_IN_INVALID_ENTRY = INVALID_ENTRY_PAYLOAD_SIZE / 4;

    @DataProvider
    public Object[][] subscriptionTypesAndBatchSizes() {
        List<Object[]> params = new ArrayList<>();
        for (SubscriptionType subscriptionType : new SubscriptionType[]{SubscriptionType.Shared,
                SubscriptionType.Key_Shared, SubscriptionType.Exclusive}) {
            for (int batchSize : new int[]{-1, 0, Integer.MIN_VALUE, Integer.MAX_VALUE,
                    MAX_MESSAGES_IN_INVALID_ENTRY + 1}) {
                for (boolean subscribeBeforePublishing : new boolean[]{true, false}) {
                    params.add(new Object[]{subscriptionType, batchSize, subscribeBeforePublishing});
                }
            }
        }
        return params.toArray(new Object[0][]);
    }

    @Test(dataProvider = "subscriptionTypesAndBatchSizes", timeOut = 60000)
    public void testDispatchContinuesAfterInvalidNumMessagesInBatch(SubscriptionType subscriptionType,
                                                                    int numMessagesInBatch,
                                                                    boolean subscribeBeforePublishing)
            throws Exception {
        final String topic = newTopicName();
        final String subscriptionName = "sub";
        final String key = "key-1";

        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(false)
                .create();
        admin.topics().createSubscription(topic, subscriptionName, MessageId.earliest);

        Consumer<byte[]> consumer = null;
        try {
            if (subscribeBeforePublishing) {
                consumer = subscribe(topic, subscriptionName, subscriptionType);
            }

            // publish an entry whose num_messages_in_batch metadata is out of range. The payload isn't a valid
            // batch: its first 4 bytes state a message size beyond the end of the payload.
            byte[] payloadBytes = new byte[INVALID_ENTRY_PAYLOAD_SIZE];
            Arrays.fill(payloadBytes, (byte) 0xFF);
            ByteBuf payload = Unpooled.wrappedBuffer(payloadBytes);
            MessageMetadata messageMetadata = new MessageMetadata();
            messageMetadata.setUncompressedSize(INVALID_ENTRY_PAYLOAD_SIZE);
            messageMetadata.setPartitionKey(key);
            MessageImpl<byte[]> invalidMessage = MessageImpl.create(topic, null, messageMetadata, payload,
                    Optional.empty(), null, Schema.BYTES, 0, true, 0);
            invalidMessage.getMessageBuilder().setNumMessagesInBatch(numMessagesInBatch);
            producer.sendAsync(invalidMessage).get(10, TimeUnit.SECONDS);

            // publish valid messages after it
            final int numValidMessages = 10;
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < numValidMessages; i++) {
                String value = "msg-" + i;
                producer.newMessage().key(key).value(value.getBytes(StandardCharsets.UTF_8)).send();
                expected.add(value);
            }

            if (!subscribeBeforePublishing) {
                // the invalid entry is dispatched in the same read as the valid entries
                consumer = subscribe(topic, subscriptionName, subscriptionType);
            }

            List<String> received = new ArrayList<>();
            while (received.size() < numValidMessages) {
                Message<byte[]> msg = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(msg).as("received so far: %s", received).isNotNull();
                consumer.acknowledge(msg);
                received.add(new String(msg.getValue(), StandardCharsets.UTF_8));
            }
            assertThat(received).containsExactlyElementsOf(expected);

            if (numMessagesInBatch < 0 || numMessagesInBatch > MAX_MESSAGES_IN_INVALID_ENTRY) {
                // the entry with the invalid num_messages_in_batch value is skipped and acknowledged by the broker
                Awaitility.await().untilAsserted(() -> assertThat(admin.topics().getStats(topic).getSubscriptions()
                        .get(subscriptionName).getMsgBacklog()).isZero());
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    @DataProvider
    public Object[][] subscriptionTypesAndCompressionTypes() {
        List<Object[]> params = new ArrayList<>();
        for (SubscriptionType subscriptionType : new SubscriptionType[]{SubscriptionType.Shared,
                SubscriptionType.Key_Shared, SubscriptionType.Exclusive}) {
            for (CompressionType compressionType : new CompressionType[]{CompressionType.NONE,
                    CompressionType.LZ4}) {
                params.add(new Object[]{subscriptionType, compressionType});
            }
        }
        return params.toArray(new Object[0][]);
    }

    @Test(dataProvider = "subscriptionTypesAndCompressionTypes", timeOut = 60000)
    public void testLargeBatchOfEmptyMessagesIsDelivered(SubscriptionType subscriptionType,
                                                          CompressionType compressionType) throws Exception {
        final String topic = newTopicName();
        final String subscriptionName = "sub";
        // empty messages make the batch payload as small as possible for its message count
        final int numMessages = 5000;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .compressionType(compressionType)
                .enableBatching(true)
                .batchingMaxMessages(numMessages)
                .batchingMaxBytes(Integer.MAX_VALUE)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();
        @Cleanup
        Consumer<byte[]> consumer = subscribe(topic, subscriptionName, subscriptionType);

        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            sendFutures.add(producer.newMessage().key("key-1").value(new byte[0]).sendAsync());
        }
        producer.flush();
        FutureUtil.waitForAll(sendFutures).get(10, TimeUnit.SECONDS);

        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> msg = consumer.receive(10, TimeUnit.SECONDS);
            assertThat(msg).as("received messages: %d", i).isNotNull();
            assertThat(msg.getMessageId()).isInstanceOfSatisfying(BatchMessageIdImpl.class,
                    messageId -> assertThat(messageId.getBatchSize()).isEqualTo(numMessages));
            consumer.acknowledge(msg);
        }
    }

    private Consumer<byte[]> subscribe(String topic, String subscriptionName, SubscriptionType subscriptionType)
            throws Exception {
        return pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionType(subscriptionType)
                .subscribe();
    }
}
