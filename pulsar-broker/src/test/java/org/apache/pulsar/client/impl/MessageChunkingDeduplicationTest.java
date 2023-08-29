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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class MessageChunkingDeduplicationTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        this.conf.setBrokerDeduplicationEnabled(true);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSendChunkMessageWithSameSequenceID() throws Exception {
        String topicName = "persistent://my-property/my-ns/testSendChunkMessageWithSameSequenceID";
        String producerName = "test-producer";
        @Cleanup
        Consumer<String> consumer = pulsarClient
                .newConsumer(Schema.STRING)
                .subscriptionName("test-sub")
                .topic(topicName)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .producerName(producerName)
                .topic(topicName)
                .enableChunking(true)
                .enableBatching(false)
                .create();
        int messageSize = 6000; // payload size in KB
        String message = String.join("", Collections.nCopies(messageSize * 1000, "a"));
        producer.newMessage().value(message).sequenceId(10).send();
        Message<String> msg = consumer.receive(10, TimeUnit.SECONDS);
        assertNotNull(msg);
        assertTrue(msg.getMessageId() instanceof ChunkMessageIdImpl);
        assertEquals(msg.getValue(), message);
        producer.newMessage().value(message).sequenceId(10).send();
        msg = consumer.receive(3, TimeUnit.SECONDS);
        assertNull(msg);
    }

    @Test
    public void testDeduplicateChunksInSingleChunkMessages() throws Exception {
        String topicName = "persistent://my-property/my-ns/testDeduplicateChunksInSingleChunkMessage";
        String producerName = "test-producer";
        @Cleanup
        Consumer<String> consumer = pulsarClient
                .newConsumer(Schema.STRING)
                .subscriptionName("test-sub")
                .topic(topicName)
                .subscribe();
        final PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topicName).get().orElse(null);
        assertNotNull(persistentTopic);
        sendChunk(persistentTopic, producerName, 1, 0, 2);
        sendChunk(persistentTopic, producerName, 1, 1, 2);
        sendChunk(persistentTopic, producerName, 1, 1, 2);

        Message<String> message = consumer.receive(15, TimeUnit.SECONDS);
        assertEquals(message.getData().length, 2);

        sendChunk(persistentTopic, producerName, 2, 0, 3);
        sendChunk(persistentTopic, producerName, 2, 1, 3);
        sendChunk(persistentTopic, producerName, 2, 1, 3);
        sendChunk(persistentTopic, producerName, 2, 2, 3);
        message = consumer.receive(20, TimeUnit.SECONDS);
        assertEquals(message.getData().length, 3);
    }

    protected static void sendChunk(final PersistentTopic persistentTopic,
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
                Unpooled.wrappedBuffer("a".getBytes()));
        persistentTopic.publishMessage(buf, new Topic.PublishContext() {
            @Override
            public boolean isChunked() {
                return chunkId != null;
            }

            @Override
            public String getProducerName() {
                return producerName;
            }

            public long getSequenceId() {
                return sequenceId;
            }

            @Override
            public void completed(Exception e, long ledgerId, long entryId) {
                String name = producerName + "-" + sequenceId;
                if (chunkId != null) {
                    name += "-" + chunkId + "-" + numChunks;
                }
                if (e == null) {
                    log.info("Sent {} to ({}, {})", name, ledgerId, entryId);
                } else {
                    log.error("Failed to send {}: {}", name, e.getMessage());
                }
            }
        });
    }
}
