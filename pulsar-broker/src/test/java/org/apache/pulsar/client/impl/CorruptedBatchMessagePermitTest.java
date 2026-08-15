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
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class CorruptedBatchMessagePermitTest extends SharedPulsarBaseTest {

    @Test(timeOut = 30000)
    public void testSharedConsumerReturnsAllPermitsForCorruptedBatch() throws Exception {
        final int batchSize = 10;
        final String subscriptionName = "shared-subscription";
        final String topicName = newTopicName();
        admin.namespaces().setDeduplicationStatus(getNamespace(), false);

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(1)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();

        PersistentTopic topic = (PersistentTopic) getTopicIfExists(topicName).get()
                .orElseThrow(() -> new IllegalStateException("Topic was not loaded"));
        PersistentSubscription subscription = topic.getSubscription(subscriptionName);
        publishCorruptedBatch(topic, batchSize).get(10, TimeUnit.SECONDS);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertEquals(subscription.getConsumers().size(), 1);
            assertEquals(subscription.getConsumers().get(0).getAvailablePermits(), 1);
        });

        producer.send("message-after-corrupted-batch");
        Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
        assertNotNull(message);
        try {
            assertEquals(message.getValue(), "message-after-corrupted-batch");
        } finally {
            message.release();
        }
    }

    private static CompletableFuture<Void> publishCorruptedBatch(PersistentTopic topic, int batchSize) {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("corrupted-batch-producer")
                .setSequenceId(0)
                .setPublishTime(System.currentTimeMillis())
                .setNumMessagesInBatch(batchSize);
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {1});
        ByteBuf entry = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        payload.release();
        int lastByteIndex = entry.writerIndex() - 1;
        entry.setByte(lastByteIndex, entry.getByte(lastByteIndex) ^ 1);

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
