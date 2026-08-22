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
import static org.testng.Assert.assertNotNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class CorruptedBatchMessagePermitTest extends SharedPulsarBaseTest {

    @Test(timeOut = 30000)
    public void testSharedConsumerReturnsPermitsForCorruptedBatch() throws Exception {
        final int batchSize = 10;
        final String topic = newTopicName();
        admin.namespaces().setDeduplicationStatus(getNamespace(), false);

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("shared-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(batchSize)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(false)
                .create();

        PersistentTopic persistentTopic = (PersistentTopic) getTopicIfExists(topic).get()
                .orElseThrow(() -> new IllegalStateException("Topic was not loaded"));
        publishCorruptedCompressedBatch(persistentTopic, batchSize).get(10, TimeUnit.SECONDS);
        producer.send("message-after-corrupted-batch");

        Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
        assertNotNull(message);
        try {
            assertEquals(message.getValue(), "message-after-corrupted-batch");
        } finally {
            message.release();
        }
    }

    private static CompletableFuture<Void> publishCorruptedCompressedBatch(PersistentTopic topic, int batchSize) {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("corrupted-batch-producer")
                .setSequenceId(0)
                .setPublishTime(System.currentTimeMillis())
                .setNumMessagesInBatch(batchSize)
                .setCompression(CompressionType.LZ4)
                .setUncompressedSize(1024);
        ByteBuf invalidCompressedPayload = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4});
        ByteBuf entry = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, metadata, invalidCompressedPayload);
        invalidCompressedPayload.release();

        CompletableFuture<Void> result = new CompletableFuture<>();
        topic.publishMessage(entry, (error, ledgerId, entryId) -> {
            if (error == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(error);
            }
        });
        return result;
    }
}
