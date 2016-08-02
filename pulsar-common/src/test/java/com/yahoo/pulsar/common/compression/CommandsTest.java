/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.common.compression;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import com.yahoo.pulsar.checksum.utils.Crc32cChecksum;
import com.yahoo.pulsar.common.api.Commands;
import com.yahoo.pulsar.common.api.Commands.ChecksumType;
import com.yahoo.pulsar.common.api.DoubleByteBuf;
import com.yahoo.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import com.yahoo.pulsar.common.util.protobuf.ByteBufCodedOutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

public class CommandsTest {

    @Test
    public void testChecksumSendCommand() throws Exception {

        // test checksum in send command
        String producerName = "prod-name";
        int sequenceId = 0;
        ByteBuf data = Unpooled.buffer(1024);
        MessageMetadata messageMetadata = MessageMetadata.newBuilder().setPublishTime(System.currentTimeMillis())
                .setProducerName(producerName).setSequenceId(sequenceId).build();
        int expectedChecksum = computeChecksum(messageMetadata, data);
        ByteBuf clientCommand = Commands.newSend(1, 0, 1, ChecksumType.Crc32c, messageMetadata, data);
        clientCommand.retain();
        ByteBuffer inputBytes = clientCommand.nioBuffer();
        ByteBuf receivedBuf = Unpooled.wrappedBuffer(inputBytes);
        receivedBuf.skipBytes(4); //skip [total-size]
        int cmdSize = (int) receivedBuf.readUnsignedInt();
        receivedBuf.readerIndex(8 + cmdSize);
        int startMessagePos = receivedBuf.readerIndex();
        
        /*** 1. verify checksum and metadataParsing ***/
        boolean hasChecksum = Commands.hasChecksum(receivedBuf);
        int checksum = Commands.readChecksum(receivedBuf).intValue();
        
        
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
        ByteBuf metaPayloadFrame = PooledByteBufAllocator.DEFAULT.buffer(metadataFrameSize, metadataFrameSize);
        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(metaPayloadFrame);
        metaPayloadFrame.writeInt(metadataSize);
        msgMetadata.writeTo(outStream);
        ByteBuf payload = compressedPayload.copy();
        ByteBuf metaPayloadBuf = DoubleByteBuf.get(metaPayloadFrame, payload);
        int computedChecksum = Crc32cChecksum.computeChecksum(metaPayloadBuf); 
        outStream.recycle();
        metaPayloadBuf.release();
        return computedChecksum;
    }

    
}
