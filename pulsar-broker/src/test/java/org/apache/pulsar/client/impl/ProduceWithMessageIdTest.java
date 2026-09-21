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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MockBrokerService;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
@CustomLog
public class ProduceWithMessageIdTest extends SharedPulsarBaseTest {
    MockBrokerService mockBrokerService;

    @BeforeClass(alwaysRun = true)
    public void setupMockBroker() throws Exception {
        mockBrokerService = new MockBrokerService();
        mockBrokerService.start();
    }

    @AfterClass(alwaysRun = true)
    public void cleanupMockBroker() throws Exception {
        if (mockBrokerService != null) {
            mockBrokerService.stop();
            mockBrokerService = null;
        }
    }

    @Test
    public void testSend() throws Exception {
        long ledgerId = 123;
        long entryId = 456;
        mockBrokerService.setHandleSend((ctx, send, headersAndPayload) -> {
            Assert.assertTrue(send.hasMessageId());
            log.info().attr("id", send.getMessageId()).log("receive messageId in ServerCnx, id");
            Assert.assertEquals(send.getMessageId().getLedgerId(), ledgerId);
            Assert.assertEquals(send.getMessageId().getEntryId(), entryId);
            ctx.writeAndFlush(
                    Commands.newSendReceipt(send.getProducerId(), send.getSequenceId(), 0, ledgerId, entryId));
        });

        @Cleanup
        PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl(mockBrokerService.getBrokerAddress())
                .build();

        String topic = newTopicName();
        ProducerImpl<byte[]> producer =
                (ProducerImpl<byte[]>) client.newProducer().topic(topic).enableBatching(false).create();

        MessageMetadata metadata = new MessageMetadata();
        ByteBuffer buffer = ByteBuffer.wrap("data".getBytes(StandardCharsets.UTF_8));
        MessageImpl<byte[]> msg = MessageImpl.create(metadata, buffer, Schema.BYTES, topic);
        //set message id here.
        msg.setMessageId(new MessageIdImpl(ledgerId, entryId, -1));

        AtomicBoolean result = new AtomicBoolean(false);
        producer.sendAsync(msg, new SendCallback() {
            @Override
            public void sendComplete(Throwable e, OpSendMsgStats opSendMsgStats) {
                log.info().exception(e).log("sendComplete");
                result.set(e == null);
            }

            @Override
            public void addCallback(MessageImpl<?> msg, SendCallback scb) {

            }

            @Override
            public SendCallback getNextSendCallback() {
                return null;
            }

            @Override
            public MessageImpl<?> getNextMessage() {
                return null;
            }

            @Override
            public CompletableFuture<MessageId> getFuture() {
                return null;
            }
        });

        // the result is true only if broker received right message id.
        Awaitility.await().untilTrue(result);
    }

    @Test
    public void sendWithCallBack() throws Exception {

        int batchSize = 10;

        String topic = newTopicName();
        ProducerImpl<byte[]> producer =
                (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topic)
                        .enableBatching(true)
                        .batchingMaxMessages(batchSize)
                        .create();

        CountDownLatch cdl = new CountDownLatch(1);
        AtomicReference<OpSendMsgStats> sendMsgStats = new AtomicReference<>();
        SendCallback sendComplete = new SendCallback() {
            @Override
            public void sendComplete(Throwable e, OpSendMsgStats opSendMsgStats) {
                log.info().exception(e).log("sendComplete");
                if (e == null){
                    sendMsgStats.set(opSendMsgStats);
                    cdl.countDown();
                }
            }

            @Override
            public void addCallback(MessageImpl<?> msg, SendCallback scb) {

            }

            @Override
            public SendCallback getNextSendCallback() {
                return null;
            }

            @Override
            public MessageImpl<?> getNextMessage() {
                return null;
            }

            @Override
            public CompletableFuture<MessageId> getFuture() {
                return null;
            }
        };
        int totalReadabled = 0;
        int totalUncompressedSize = 0;
        for (int i = 0; i < batchSize; i++) {
            MessageMetadata metadata = new MessageMetadata();
            ByteBuffer buffer = ByteBuffer.wrap("data".getBytes(StandardCharsets.UTF_8));
            MessageImpl<byte[]> msg = MessageImpl.create(metadata, buffer, Schema.BYTES, topic);
            msg.getDataBuffer().retain();
            totalReadabled += msg.getDataBuffer().readableBytes();
            totalUncompressedSize += msg.getUncompressedSize();
            producer.sendAsync(msg, sendComplete);
        }

        cdl.await();
        OpSendMsgStats opSendMsgStats = sendMsgStats.get();
        // uncompressedSize includes both message payloads and the batch buffer allocation,
        // whose actual capacity depends on the allocator and may differ from the requested size
        Assert.assertTrue(opSendMsgStats.getUncompressedSize() >= totalUncompressedSize);
        Assert.assertEquals(opSendMsgStats.getSequenceId(), 0);
        Assert.assertEquals(opSendMsgStats.getRetryCount(), 1);
        Assert.assertEquals(opSendMsgStats.getBatchSizeByte(), totalReadabled);
        Assert.assertEquals(opSendMsgStats.getNumMessagesInBatch(), batchSize);
        Assert.assertEquals(opSendMsgStats.getHighestSequenceId(), batchSize - 1);
        Assert.assertEquals(opSendMsgStats.getTotalChunks(), 0);
        Assert.assertEquals(opSendMsgStats.getChunkId(), -1);
    }
}
