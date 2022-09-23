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
package org.apache.pulsar.common.compression;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.scurrilous.circe.checksum.Crc32cIntChecksum;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.Base64;
import io.netty.util.ReferenceCountUtil;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CommandsTest {

    @Test
    public void testChecksumSendCommand() throws Exception {

        // test checksum in send command
        String producerName = "prod-name";
        int sequenceId = 0;
        ByteBuf data = Unpooled.buffer(1024);
        MessageMetadata messageMetadata = new MessageMetadata()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName(producerName)
                .setSequenceId(sequenceId);
        int expectedChecksum = computeChecksum(messageMetadata, data);
        ByteBufPair clientCommand = Commands.newSend(1, 0, 1, ChecksumType.Crc32c, messageMetadata, data);
        clientCommand.retain();
        ByteBuf receivedBuf = ByteBufPair.coalesce(clientCommand);
        System.err.println(ByteBufUtil.prettyHexDump(receivedBuf));
        receivedBuf.skipBytes(4); //skip [total-size]
        int cmdSize = (int) receivedBuf.readUnsignedInt();
        receivedBuf.readerIndex(8 + cmdSize);
        int startMessagePos = receivedBuf.readerIndex();

        /*** 1. verify checksum and metadataParsing ***/
        boolean hasChecksum = Commands.hasChecksum(receivedBuf);
        int checksum = Commands.readChecksum(receivedBuf);


        // verify checksum is present
        assertTrue(hasChecksum);
        // verify checksum value
        assertEquals(expectedChecksum, checksum);
        MessageMetadata metadata = Commands.parseMessageMetadata(receivedBuf);
        // verify metadata parsing
        assertEquals(metadata.getProducerName(), producerName);

        /** 2. parseMessageMetadata should skip checksum if present **/
        receivedBuf.readerIndex(startMessagePos);
        metadata = Commands.parseMessageMetadata(receivedBuf);
        // verify metadata parsing
        assertEquals(metadata.getProducerName(), producerName);

    }

    private int computeChecksum(MessageMetadata msgMetadata, ByteBuf compressedPayload) throws IOException {
        int metadataSize = msgMetadata.getSerializedSize();
        int metadataFrameSize = 4 + metadataSize;
        ByteBuf metaPayloadFrame = PulsarByteBufAllocator.DEFAULT.buffer(metadataFrameSize, metadataFrameSize);
        metaPayloadFrame.writeInt(metadataSize);
        msgMetadata.writeTo(metaPayloadFrame);
        ByteBuf payload = compressedPayload.copy();
        ByteBufPair metaPayloadBuf = ByteBufPair.get(metaPayloadFrame, payload);
        int computedChecksum = Crc32cIntChecksum.computeChecksum(ByteBufPair.coalesce(metaPayloadBuf));
        metaPayloadBuf.release();
        return computedChecksum;
    }

    @Test
    public void testPeekStickyKey() {
        String message = "msg-1";
        String partitionedKey = "key1";
        MessageMetadata messageMetadata2 = new MessageMetadata()
                .setSequenceId(1)
                .setProducerName("testProducer")
                .setPartitionKey(partitionedKey)
                .setPartitionKeyB64Encoded(false)
                .setPublishTime(System.currentTimeMillis());
        ByteBuf byteBuf = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata2,
                Unpooled.copiedBuffer(message.getBytes(UTF_8)));
        byte[] bytes = Commands.peekStickyKey(byteBuf, "topic-1", "sub-1");
        String key = new String(bytes);
        Assert.assertEquals(partitionedKey, key);
        ReferenceCountUtil.safeRelease(byteBuf);
        // test 64 encoded
        String partitionedKey2 = Base64.getEncoder().encodeToString("key2".getBytes(UTF_8));
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(1)
                .setProducerName("testProducer")
                .setPartitionKey(partitionedKey2)
                .setPartitionKeyB64Encoded(true)
                .setPublishTime(System.currentTimeMillis());
        ByteBuf byteBuf2 = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata,
                Unpooled.copiedBuffer(message.getBytes(UTF_8)));
        byte[] bytes2 = Commands.peekStickyKey(byteBuf2, "topic-2", "sub-2");
        String key2 = Base64.getEncoder().encodeToString(bytes2);;
        Assert.assertEquals(partitionedKey2, key2);
        ReferenceCountUtil.safeRelease(byteBuf2);
    }
}
