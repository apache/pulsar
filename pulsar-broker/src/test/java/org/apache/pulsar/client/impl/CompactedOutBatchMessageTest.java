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

import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class CompactedOutBatchMessageTest extends SharedPulsarBaseTest {

    @Test
    public void testBatchRemainderIsNotReturnedToReplacementConnection() throws Exception {
        final int batchSize = 2;
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("foobar")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(batchSize);
        ByteBuf compactedBatch = Unpooled.buffer(1000);
        for (int i = 0; i < batchSize; i++) {
            Commands.serializeSingleMessageInBatchWithPayload(
                    new SingleMessageMetadata().setCompactedOut(true), Unpooled.EMPTY_BUFFER, compactedBatch);
        }

        try (ConsumerImpl<byte[]> consumer =
                     (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(newTopicName())
                             .subscriptionName("old-connection-subscription")
                             .receiverQueueSize(20)
                             .subscribe()) {
            int permitsBefore = consumer.getAvailablePermits();
            consumer.receiveIndividualMessagesFromBatch(null, metadata, 0, null, compactedBatch,
                    new MessageIdData().setLedgerId(1234).setEntryId(567), mock(ClientCnx.class),
                    DEFAULT_CONSUMER_EPOCH, false, batchSize);

            assertEquals(consumer.getAvailablePermits(), permitsBefore);
        } finally {
            compactedBatch.release();
        }
    }

    @Test
    public void testStaleEpochDiscardDoesNotReturnPermitToReplacementConnection() throws Exception {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("foobar")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(1);
        ByteBuf batch = Unpooled.buffer(100);
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {1});
        Commands.serializeSingleMessageInBatchWithPayload(new SingleMessageMetadata(), payload, batch);
        payload.release();

        try (ConsumerImpl<byte[]> consumer =
                     (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(newTopicName())
                             .subscriptionName("stale-epoch-subscription")
                             .receiverQueueSize(20)
                             .subscribe()) {
            ConsumerBase.CONSUMER_EPOCH.set(consumer, 2);
            int permitsBefore = consumer.getAvailablePermits();
            consumer.receiveIndividualMessagesFromBatch(null, metadata, 0, null, batch,
                    new MessageIdData().setLedgerId(1234).setEntryId(567), mock(ClientCnx.class), 1, false, 1);
            consumer.internalPinnedExecutor.submit(() -> assertEquals(consumer.numMessagesInQueue(), 0))
                    .get(5, TimeUnit.SECONDS);

            assertEquals(consumer.getAvailablePermits(), permitsBefore);
        } finally {
            batch.release();
        }
    }

    @Test
    public void testCompactedOutMessages() throws Exception {
        final String topic1 = newTopicName();

        BrokerEntryMetadata brokerEntryMetadata = new BrokerEntryMetadata().setBrokerTimestamp(1).setBrokerTimestamp(1);

        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("foobar")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(3);

        // build a buffer with 4 messages, first and last compacted out
        ByteBuf batchBuffer = Unpooled.buffer(1000);
        Commands.serializeSingleMessageInBatchWithPayload(
                new SingleMessageMetadata().setCompactedOut(true).setPartitionKey("key1"),
                Unpooled.EMPTY_BUFFER, batchBuffer);
        Commands.serializeSingleMessageInBatchWithPayload(
                new SingleMessageMetadata().setCompactedOut(true).setPartitionKey("key2"),
                Unpooled.EMPTY_BUFFER, batchBuffer);
        Commands.serializeSingleMessageInBatchWithPayload(
                new SingleMessageMetadata().setCompactedOut(false).setPartitionKey("key3"),
                Unpooled.EMPTY_BUFFER, batchBuffer);
        Commands.serializeSingleMessageInBatchWithPayload(
                new SingleMessageMetadata().setCompactedOut(true).setPartitionKey("key4"),
                Unpooled.EMPTY_BUFFER, batchBuffer);

        try (ConsumerImpl<byte[]> consumer =
             (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic1)
                .subscriptionName("my-subscriber-name").subscribe()) {
            int permitsBefore = consumer.getAvailablePermits();
            // shove it in the sideways
            consumer.receiveIndividualMessagesFromBatch(brokerEntryMetadata, metadata, 0, null,
                    batchBuffer, new MessageIdData().setLedgerId(1234).setEntryId(567),
                    consumer.cnx(), DEFAULT_CONSUMER_EPOCH, false, metadata.getNumMessagesInBatch());
            Message<?> m = consumer.receive();
            assertEquals(((BatchMessageIdImpl) m.getMessageId()).getLedgerId(), 1234);
            assertEquals(((BatchMessageIdImpl) m.getMessageId()).getEntryId(), 567);
            assertEquals(((BatchMessageIdImpl) m.getMessageId()).getBatchIndex(), 2);
            assertEquals(m.getKey(), "key3");

            assertEquals(consumer.numMessagesInQueue(), 0);
            assertEquals(consumer.getAvailablePermits(), permitsBefore + metadata.getNumMessagesInBatch());
            m.release();
        }
    }
}
