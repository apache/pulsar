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

import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.testng.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class CompactedOutBatchMessageTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testCompactedOutMessages() throws Exception {
        final String topic1 = "persistent://my-property/my-ns/my-topic";

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

        try (ConsumerImpl<byte[]> consumer
             = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic1)
                .subscriptionName("my-subscriber-name").subscribe()) {
            // shove it in the sideways
            consumer.receiveIndividualMessagesFromBatch(brokerEntryMetadata, metadata, 0, null,
                    batchBuffer, new MessageIdData().setLedgerId(1234).setEntryId(567), consumer.cnx(), DEFAULT_CONSUMER_EPOCH);
            Message<?> m = consumer.receive();
            assertEquals(((BatchMessageIdImpl)m.getMessageId()).getLedgerId(), 1234);
            assertEquals(((BatchMessageIdImpl)m.getMessageId()).getEntryId(), 567);
            assertEquals(((BatchMessageIdImpl)m.getMessageId()).getBatchIndex(), 2);
            assertEquals(m.getKey(), "key3");

            assertEquals(consumer.numMessagesInQueue(), 0);
        }
    }
}
