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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class MessageChunkingSharedTest extends ProducerConsumerBase {

    private static final int MAX_MESSAGE_SIZE = 100;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSingleConsumer() throws Exception {
        final String topic = "my-property/my-ns/test-single-consumer";
        @Cleanup final Producer<String> producer = createProducer(topic);
        @Cleanup final Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(5)
                .subscribe();

        final List<String> values = new ArrayList<>();
        values.add(createChunkedMessage(1)); // non-chunk
        values.add(createChunkedMessage(10)); // number of chunks > receiver queue size
        values.add(createChunkedMessage(4)); // number of chunks < receiver queue size
        for (String value : values) {
            final MessageId messageId = producer.send(value);
            log.info("Sent {} bytes to {}", value.length(), messageId);
        }

        final List<String> receivedValues = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            final Message<String> message = consumer.receive(3, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            receivedValues.add(message.getValue());
            log.info("Received {} bytes from {}", message.getValue().length(), message.getMessageId());
            consumer.acknowledge(message);
        }
        assertEquals(receivedValues, values);
    }

    @Test
    public void testMultiConsumers() throws Exception {
        final String topic = "my-property/my-ns/test-multi-consumers";
        @Cleanup final Producer<String> producer = createProducer(topic);
        final ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(5);

        final List<String> receivedValues1 = Collections.synchronizedList(new ArrayList<>());
        @Cleanup final Consumer<String> consumer1 = consumerBuilder
                .messageListener((MessageListener<String>) (consumer, msg) -> receivedValues1.add(msg.getValue()))
                .subscribe();
        final List<String> receivedValues2 = Collections.synchronizedList(new ArrayList<>());
        @Cleanup final Consumer<String> consumer2 = consumerBuilder
                .messageListener((MessageListener<String>) (consumer, msg) -> receivedValues2.add(msg.getValue()))
                .subscribe();

        final Set<String> values = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            values.add(createChunkedMessage(4));
        }
        for (String value : values) {
            producer.send(value);
        }

        Awaitility.await().atMost(Duration.ofSeconds(3))
                .until(() -> receivedValues1.size() + receivedValues2.size() >= values.size());
        assertEquals(receivedValues1.size() + receivedValues2.size(), values.size());
        assertFalse(receivedValues1.isEmpty());
        assertFalse(receivedValues2.isEmpty());
        for (String value : receivedValues1) {
            assertTrue(values.contains(value));
        }
        for (String value : receivedValues2) {
            assertTrue(values.contains(value));
        }
    }

    @Test
    public void testInterleavedChunks() throws Exception {
        final String topic = "persistent://my-property/my-ns/test-interleaved-chunks";
        final ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared);
        final List<String> receivedUuidList1 = Collections.synchronizedList(new ArrayList<>());
        final Consumer<byte[]> consumer1 = consumerBuilder.messageListener((MessageListener<byte[]>)
                        (consumer, msg) -> receivedUuidList1.add(msg.getProducerName() + "-" + msg.getSequenceId()))
                .consumerName("consumer-1")
                .subscribe();
        final PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topic).get().orElse(null);
        assertNotNull(persistentTopic);
        // send: A-0, A-1-0-3, A-1-1-3, B-0, B-1-0-2
        sendNonChunk(persistentTopic, "A", 0);
        sendChunk(persistentTopic, "A", 1, 0, 3);
        sendChunk(persistentTopic, "A", 1, 1, 3);
        sendNonChunk(persistentTopic, "B", 0);
        sendChunk(persistentTopic, "B", 1, 0, 2);

        Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> receivedUuidList1.size() >= 2);
        assertEquals(receivedUuidList1, Arrays.asList("A-0", "B-0"));

        // complete all 2 chunks of B-1
        sendChunk(persistentTopic, "B", 1, 1, 2);
        Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> receivedUuidList1.size() >= 3);
        assertEquals(receivedUuidList1, Arrays.asList("A-0", "B-0", "B-1"));

        // complete all 3 chunks of A-1
        sendChunk(persistentTopic, "A", 1, 2, 3);
        Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> receivedUuidList1.size() >= 3);
        assertEquals(receivedUuidList1, Arrays.asList("A-0", "B-0", "B-1", "A-1"));

        final List<String> receivedUuidList2 = Collections.synchronizedList(new ArrayList<>());
        @Cleanup final Consumer<byte[]> consumer2 = consumerBuilder.messageListener((MessageListener<byte[]>)
                        (consumer, msg) -> receivedUuidList2.add(msg.getProducerName() + "-" + msg.getSequenceId()))
                .consumerName("consumer-2")
                .subscribe();
        consumer1.close();

        // Since messages were never acknowledged, all messages will be redelivered to consumer-2
        Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> receivedUuidList2.size() >= 4);
        assertEquals(receivedUuidList1, Arrays.asList("A-0", "B-0", "B-1", "A-1"));
    }

    private Producer<String> createProducer(String topic) throws PulsarClientException {
        return pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableChunking(true)
                .enableBatching(false)
                .chunkMaxMessageSize(MAX_MESSAGE_SIZE)
                .create();
    }

    private static String createChunkedMessage(int numChunks) {
        assert numChunks >= 1;
        final byte[] payload = new byte[(numChunks - 1) * MAX_MESSAGE_SIZE + MAX_MESSAGE_SIZE / 10];
        final Random random = new Random();
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) ('a' + random.nextInt(26));
        }
        return Schema.STRING.decode(payload);
    }

    private static void sendNonChunk(final PersistentTopic persistentTopic,
                                     final String producerName,
                                     final long sequenceId) {
        sendChunk(persistentTopic, producerName, sequenceId, null, null);
    }

    private static void sendChunk(final PersistentTopic persistentTopic,
                                  final String producerName,
                                  final long sequenceId,
                                  final Integer chunkId,
                                  final Integer numChunks) {
        final MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName(producerName);
        metadata.setSequenceId(sequenceId);
        metadata.setPublishTime(System.currentTimeMillis());
        if (chunkId != null && numChunks != null) {
            metadata.setUuid(producerName + "-" + sequenceId);
            metadata.setChunkId(chunkId);
            metadata.setNumChunksFromMsg(numChunks);
            metadata.setTotalChunkMsgSize(numChunks);
        }
        final ByteBuf buf = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata,
                PulsarByteBufAllocator.DEFAULT.buffer(1));
        persistentTopic.publishMessage(buf, (e, ledgerId, entryId) -> {
            String name = producerName + "-" + sequenceId;
            if (chunkId != null) {
                name += "-" + chunkId + "-" + numChunks;
            }
            if (e == null) {
                log.info("Sent {} to ({}, {})", name, ledgerId, entryId);
            } else {
                log.error("Failed to send {}: {}", name, e.getMessage());
            }
        });
    }
}
