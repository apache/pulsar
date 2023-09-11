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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.lang.reflect.Field;
import static org.testng.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BatchMessageContainerImplTest {

    @Test
    public void recoveryAfterOom() {
        final AtomicBoolean called = new AtomicBoolean();
        final ProducerImpl<?> producer = mock(ProducerImpl.class);
        final ProducerConfigurationData producerConfigurationData = new ProducerConfigurationData();
        producerConfigurationData.setCompressionType(CompressionType.NONE);
        when(producer.getConfiguration()).thenReturn(producerConfigurationData);
        final ByteBufAllocator mockAllocator = mock(ByteBufAllocator.class);
        doAnswer((ignore) -> {
            called.set(true);
            throw new OutOfMemoryError("test");
        }).when(mockAllocator).buffer(anyInt());
        final BatchMessageContainerImpl batchMessageContainer = new BatchMessageContainerImpl(mockAllocator);
        batchMessageContainer.setProducer(producer);
        MessageMetadata messageMetadata1 = new MessageMetadata();
        messageMetadata1.setSequenceId(1L);
        messageMetadata1.setProducerName("producer1");
        messageMetadata1.setPublishTime(System.currentTimeMillis());
        ByteBuffer payload1 = ByteBuffer.wrap("payload1".getBytes(StandardCharsets.UTF_8));
        final MessageImpl<byte[]> message1 = MessageImpl.create(messageMetadata1, payload1, Schema.BYTES, null);
        batchMessageContainer.add(message1, null);
        assertTrue(called.get());
        MessageMetadata messageMetadata2 = new MessageMetadata();
        messageMetadata2.setSequenceId(1L);
        messageMetadata2.setProducerName("producer1");
        messageMetadata2.setPublishTime(System.currentTimeMillis());
        ByteBuffer payload2 = ByteBuffer.wrap("payload2".getBytes(StandardCharsets.UTF_8));
        final MessageImpl<byte[]> message2 = MessageImpl.create(messageMetadata2, payload2, Schema.BYTES, null);
        // after oom, our add can self-healing, won't throw exception
        batchMessageContainer.add(message2, null);
    }
    @Test
    public void testMessagesSize() throws Exception {
        ProducerImpl producer = mock(ProducerImpl.class);

        final ProducerConfigurationData producerConfigurationData = new ProducerConfigurationData();
        producerConfigurationData.setCompressionType(CompressionType.NONE);
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(pulsarClient.getCnxPool()).thenReturn(connectionPool);
        MemoryLimitController memoryLimitController = mock(MemoryLimitController.class);
        when(pulsarClient.getMemoryLimitController()).thenReturn(memoryLimitController);
        try {
            Field clientFiled = HandlerState.class.getDeclaredField("client");
            clientFiled.setAccessible(true);
            clientFiled.set(producer, pulsarClient);
        } catch (Exception e){
            Assert.fail(e.getMessage());
        }

        ByteBuffer payload = ByteBuffer.wrap("payload".getBytes(StandardCharsets.UTF_8));

        when(producer.getConfiguration()).thenReturn(producerConfigurationData);
        when(producer.encryptMessage(any(), any())).thenReturn(ByteBufAllocator.DEFAULT.buffer().writeBytes(payload));

        final int initNum = 32;
        BatchMessageContainerImpl batchMessageContainer = new BatchMessageContainerImpl();
        batchMessageContainer.setProducer(producer);
        assertEquals(batchMessageContainer.getMaxMessagesNum(), initNum);

        addMessagesAndCreateOpSendMsg(batchMessageContainer, 10);
        assertEquals(batchMessageContainer.getMaxMessagesNum(), initNum);

        addMessagesAndCreateOpSendMsg(batchMessageContainer, 200);
        assertEquals(batchMessageContainer.getMaxMessagesNum(), 200);

        addMessagesAndCreateOpSendMsg(batchMessageContainer, 10);
        assertEquals(batchMessageContainer.getMaxMessagesNum(), 200);
    }

    private void addMessagesAndCreateOpSendMsg(BatchMessageContainerImpl batchMessageContainer, int num)
            throws Exception{
        ArrayList<MessageImpl<?>> messages = new ArrayList<>();
        for (int i = 0; i < num; ++i) {
            MessageMetadata messageMetadata = new MessageMetadata();
            messageMetadata.setSequenceId(i);
            messageMetadata.setProducerName("producer");
            messageMetadata.setPublishTime(System.currentTimeMillis());
            ByteBuffer payload = ByteBuffer.wrap("payload".getBytes(StandardCharsets.UTF_8));
            MessageImpl<?> message = MessageImpl.create(messageMetadata, payload, Schema.BYTES, null);
            messages.add(message);
            batchMessageContainer.add(message, null);
        }

        batchMessageContainer.createOpSendMsg();
        batchMessageContainer.clear();
        messages.forEach(ReferenceCountUtil::safeRelease);
    }
}
