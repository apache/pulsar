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
package org.apache.pulsar.common.compression;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import com.scurrilous.circe.checksum.Crc32cIntChecksum;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.util.Base64;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
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
        receivedBuf.release();
    }

    private int computeChecksum(MessageMetadata msgMetadata, ByteBuf compressedPayload) throws IOException {
        int metadataSize = msgMetadata.getSerializedSize();
        int metadataFrameSize = 4 + metadataSize;
        ByteBuf metaPayloadFrame = PulsarByteBufAllocator.DEFAULT.buffer(metadataFrameSize, metadataFrameSize);
        metaPayloadFrame.writeInt(metadataSize);
        msgMetadata.writeTo(metaPayloadFrame);
        ByteBuf payload = compressedPayload.copy();
        ByteBuf byteBuf = ByteBufPair.coalesce(ByteBufPair.get(metaPayloadFrame, payload));
        int computedChecksum = Crc32cIntChecksum.computeChecksum(byteBuf);
        byteBuf.release();
        return computedChecksum;
    }

    @DataProvider
    public Object[][] checksumTypes() {
        return new Object[][] {{ChecksumType.None}, {ChecksumType.Crc32c}};
    }

    @Test(dataProvider = "checksumTypes")
    public void testSerializeMetadataAndPayloadPreservesSourceIndices(ChecksumType checksumType) {
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {9, 8, 7, 6, 5, 4});
        payload.readerIndex(1);
        payload.writerIndex(5);
        int readerIndex = payload.readerIndex();
        int writerIndex = payload.writerIndex();
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(1)
                .setPublishTime(System.currentTimeMillis());

        ByteBuf serialized = serializeMetadataAndPayload(checksumType, metadata, payload);

        assertEquals(payload.readerIndex(), readerIndex, "serialization must not consume the source payload");
        assertEquals(payload.writerIndex(), writerIndex, "serialization must not change the source capacity");
        assertEquals(serialized.readableBytes(), metadata.getSerializedSize() + 4 + (writerIndex - readerIndex)
                + (ChecksumType.Crc32c.equals(checksumType) ? 6 : 0));
        assertEquals(serialized.getByte(serialized.writerIndex() - (writerIndex - readerIndex) + 1), (byte) 7);
        ReferenceCountUtil.safeRelease(serialized);
        payload.release();
    }

    @Test(dataProvider = "checksumTypes")
    public void testNewSendEncoderPreservesSourceIndices(ChecksumType checksumType) {
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {9, 8, 7, 6, 5, 4});
        payload.readerIndex(1);
        payload.writerIndex(5);
        int readerIndex = payload.readerIndex();
        int writerIndex = payload.writerIndex();
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(1)
                .setPublishTime(System.currentTimeMillis());

        ByteBufPair serialized = Commands.newSend(1, 1, 1, checksumType, metadata, payload);
        payload.retain();
        EmbeddedChannel channel = new EmbeddedChannel(ByteBufPair.getEncoder(false));
        assertTrue(channel.writeOutbound(serialized));
        channel.finishAndReleaseAll();

        assertEquals(payload.readerIndex(), readerIndex,
                "serialization and encoding must not consume the source payload");
        assertEquals(payload.writerIndex(), writerIndex,
                "serialization and encoding must not change the source capacity");
        assertEquals(payload.refCnt(), 1, "encoding must release only the pair's payload reference");
        payload.release();
    }

    @Test(dataProvider = "checksumTypes")
    public void testSerializeMetadataAndPayloadEncoderPreservesSourceIndices(ChecksumType checksumType) {
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {9, 8, 7, 6, 5, 4});
        payload.readerIndex(1);
        payload.writerIndex(5);
        int readerIndex = payload.readerIndex();
        int writerIndex = payload.writerIndex();
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(1)
                .setPublishTime(System.currentTimeMillis());

        ByteBuf metadataAndPayload = serializeMetadataAndPayload(checksumType, metadata, payload);
        EmbeddedChannel channel = new EmbeddedChannel(ByteBufPair.getEncoder(false));
        assertTrue(channel.writeOutbound(ByteBufPair.get(Unpooled.buffer().writeZero(1), metadataAndPayload)));
        channel.finishAndReleaseAll();
        assertEquals(payload.readerIndex(), readerIndex,
                "encoding must not consume the source payload");
        assertEquals(payload.writerIndex(), writerIndex,
                "encoding must not change the source capacity");
        payload.release();
    }

    @Test
    public void testSerializeSingleMessageInBatchPreservesSourceIndices() {
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {9, 8, 7, 6, 5, 4});
        payload.readerIndex(1);
        payload.writerIndex(5);
        int readerIndex = payload.readerIndex();
        int writerIndex = payload.writerIndex();
        SingleMessageMetadata metadata = new SingleMessageMetadata().setSequenceId(1);
        ByteBuf batch = Unpooled.buffer();

        ByteBuf serialized = Commands.serializeSingleMessageInBatchWithPayload(metadata, payload, batch);

        assertSame(serialized, batch);
        assertEquals(payload.readerIndex(), readerIndex, "serialization must not consume the source payload");
        assertEquals(payload.writerIndex(), writerIndex, "serialization must not change the source capacity");
        assertEquals(batch.readInt(), metadata.getSerializedSize());
        assertEquals(batch.getByte(batch.writerIndex() - (writerIndex - readerIndex) + 1), (byte) 7);
        ReferenceCountUtil.safeRelease(batch);
        payload.release();
    }

    @Test
    public void testPeekStickyKey() {
        String message = "msg-1";
        String partitionedKey = "key1";
        String producerName = "testProducer";
        int sequenceId = 1;
        MessageMetadata messageMetadata2 = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName(producerName)
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
                .setSequenceId(sequenceId)
                .setProducerName(producerName)
                .setPartitionKey(partitionedKey2)
                .setPartitionKeyB64Encoded(true)
                .setPublishTime(System.currentTimeMillis());
        ByteBuf byteBuf2 = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata,
                Unpooled.copiedBuffer(message.getBytes(UTF_8)));
        byte[] bytes2 = Commands.peekStickyKey(byteBuf2, "topic-2", "sub-2");
        String key2 = Base64.getEncoder().encodeToString(bytes2);
        Assert.assertEquals(partitionedKey2, key2);
        ReferenceCountUtil.safeRelease(byteBuf2);
        // test fallback key if no key given in message metadata
        String fallbackPartitionedKey = producerName + "-" + sequenceId;
        MessageMetadata messageMetadataWithoutKey = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName(producerName)
                .setPublishTime(System.currentTimeMillis());
        ByteBuf byteBuf3 = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadataWithoutKey,
                Unpooled.copiedBuffer(message.getBytes(UTF_8)));
        byte[] bytes3 = Commands.peekStickyKey(byteBuf3, "topic-3", "sub-3");
        String key3 = new String(bytes3);
        Assert.assertEquals(fallbackPartitionedKey, key3);
        ReferenceCountUtil.safeRelease(byteBuf3);
    }
}
