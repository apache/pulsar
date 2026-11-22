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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.assertj.core.api.SoftAssertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ChunkedMessageDeduplicationRecoveryTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] reloadBetweenChunks() {
        return new Object[][] {
                // Control case for the live publish path; no reload is performed.
                {false, -1, new String[] {"hello-", "world"}},
                {true, 0, new String[] {"hello-", "world"}},
                {true, 1, new String[] {"hello-", "middle-", "world"}}
        };
    }

    @Test(dataProvider = "reloadBetweenChunks", timeOut = 30000)
    public void testIncompleteChunkedMessageSurvivesDeduplicationRecovery(
            boolean reload, int reloadAfterChunk, String[] chunkValues) throws Exception {
        String topic = newTopicName();
        String producerName = "chunked-producer";
        long sequenceId = 41;
        int numChunks = chunkValues.length;
        String completeMessage = String.join("", chunkValues);
        admin.namespaces().setDeduplicationStatus(getNamespace(), true);
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().createSubscription(topic, "test-sub", MessageId.earliest);
        PersistentTopic persistentTopic = (PersistentTopic) getTopic(topic, false).get(10, TimeUnit.SECONDS)
                .orElseThrow();
        assertThat(persistentTopic.getMessageDeduplication().isEnabled()).isTrue();

        SoftAssertions softly = new SoftAssertions();
        // Publish through the broker's real persistence path, pausing between protocol chunks.
        for (int chunkId = 0; chunkId < numChunks; chunkId++) {
            if (reload && chunkId == reloadAfterChunk + 1) {
                admin.topics().unload(topic);
                persistentTopic = (PersistentTopic) getTopic(topic, false).get(10, TimeUnit.SECONDS).orElseThrow();
                softly.assertThat(persistentTopic.getLastPublishedSequenceId(producerName))
                        .as("Recovery must not advance the watermark past an incomplete chunked message")
                        .isEqualTo(-1);
            }

            Position chunk = publishChunk(persistentTopic, producerName, sequenceId, chunkId, numChunks,
                    chunkValues[chunkId], completeMessage);
            softly.assertThat(chunk.getLedgerId()).isGreaterThanOrEqualTo(0);
            if (chunkId < numChunks - 1) {
                softly.assertThat(persistentTopic.getLastPublishedSequenceId(producerName))
                        .as("A persisted prefix does not complete the logical message")
                        .isEqualTo(-1);
            }
        }

        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic)
                     .enableBatching(false).create();
             Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                     .subscriptionName("test-sub").subscribe()) {
            producer.send("probe");
            List<String> received = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                Message<String> message = consumer.receive(2, TimeUnit.SECONDS);
                if (message == null) {
                    break;
                }
                received.add(message.getValue());
            }
            softly.assertThat(received)
                    .as("The chunked message must remain deliverable when later normal messages are delivered")
                    .containsExactly(completeMessage, "probe");
        }
        softly.assertAll();
    }

    private Position publishChunk(PersistentTopic topic, String producerName, long sequenceId,
                                  int chunkId, int numChunks, String value, String completeMessage) throws Exception {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName(producerName)
                .setSequenceId(sequenceId)
                .setPublishTime(System.currentTimeMillis())
                .setUuid(producerName + "-" + sequenceId)
                .setChunkId(chunkId)
                .setNumChunksFromMsg(numChunks)
                .setTotalChunkMsgSize(completeMessage.getBytes(UTF_8).length);
        ByteBuf payload = Unpooled.copiedBuffer(value, UTF_8);
        ByteBuf data;
        try {
            data = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        } finally {
            payload.release();
        }
        CompletableFuture<Position> persisted = new CompletableFuture<>();
        try {
            topic.publishMessage(data, new Topic.PublishContext() {
                private final Map<String, Object> properties = new HashMap<>();

                @Override
                public String getProducerName() {
                    return producerName;
                }

                @Override
                public long getSequenceId() {
                    return sequenceId;
                }

                @Override
                public boolean isChunked() {
                    return true;
                }

                @Override
                public void setProperty(String propertyName, Object value) {
                    properties.put(propertyName, value);
                }

                @Override
                public Object getProperty(String propertyName) {
                    return properties.get(propertyName);
                }

                @Override
                public void completed(Exception exception, long ledgerId, long entryId) {
                    if (exception != null) {
                        persisted.completeExceptionally(exception);
                    } else {
                        persisted.complete(PositionFactory.create(ledgerId, entryId));
                    }
                }
            });
            return persisted.get(10, TimeUnit.SECONDS);
        } finally {
            data.release();
        }
    }
}
