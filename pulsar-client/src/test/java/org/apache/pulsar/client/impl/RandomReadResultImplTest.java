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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageInvisibleReason;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.RandomReaderConfigurationData;
import org.apache.pulsar.common.api.proto.CommandRandomReadMessage;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.Test;

public class RandomReadResultImplTest {

    @Test
    public void testInvisibleResultThrowsMessageInvisibleException() {
        MessageIdImpl messageId = new MessageIdImpl(3L, 4L, 0);
        RandomReadResultImpl<byte[]> result = RandomReadResultImpl.invisible(
                messageId, MessageInvisibleReason.ABORTED_TRANSACTION);

        assertFalse(result.isVisible());
        PulsarClientException.MessageInvisibleException exception =
                expectThrows(PulsarClientException.MessageInvisibleException.class, result::getMessages);
        assertEquals(exception.getMessageId(), messageId);
        assertEquals(exception.getReason(), MessageInvisibleReason.ABORTED_TRANSACTION);
        assertTrue(result.getInvisibleException().isPresent());
        assertEquals(result.getInvisibleException().get().getReason(), MessageInvisibleReason.ABORTED_TRANSACTION);
    }

    @Test
    public void testVisibleResultReturnsMessages() throws Exception {
        MessageIdImpl messageId = new MessageIdImpl(3L, 4L, 0);
        RandomReadResultImpl<byte[]> result = RandomReadResultImpl.visible(messageId, List.of());

        assertTrue(result.isVisible());
        assertEquals(result.getMessageId(), messageId);
        assertTrue(result.getMessages().isEmpty());
        assertTrue(result.getInvisibleException().isEmpty());
    }

    @Test
    public void testDecodeVisibleSingleEntryWithShortPayload() throws Exception {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("p")
                .setSequenceId(1L)
                .setPublishTime(System.currentTimeMillis());
        ByteBuf data = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, metadata, Unpooled.wrappedBuffer(new byte[] {9}));
        try {
            CommandRandomReadMessage command = Commands.newRandomReadMessageCommand(7L, 11L, 3L, 4L, 0)
                    .getRandomReadMessage();
            RandomReaderConfigurationData<byte[]> conf = new RandomReaderConfigurationData<>();

            List<Message<byte[]>> messages = decodeEntry(command, data, Schema.BYTES, conf);

            assertEquals(messages.size(), 1);
            assertEquals(messages.get(0).getMessageId(), new MessageIdImpl(3L, 4L, 0));
            assertEquals(messages.get(0).getData(), new byte[] {9});
        } finally {
            data.release();
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Message<byte[]>> decodeEntry(CommandRandomReadMessage command, ByteBuf data,
                                                     Schema<byte[]> schema,
                                                     RandomReaderConfigurationData<byte[]> conf) throws Exception {
        Method method = RandomReaderImpl.PendingRandomRead.class.getDeclaredMethod("decodeEntry",
                CommandRandomReadMessage.class, ByteBuf.class, Schema.class, RandomReaderConfigurationData.class,
                String.class, int.class);
        method.setAccessible(true);
        return (List<Message<byte[]>>) method.invoke(null, command, data, schema, conf,
                "persistent://public/default/t1", 0);
    }
}
