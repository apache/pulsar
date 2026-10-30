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
package org.apache.pulsar.common.protocol;

import static org.testng.Assert.assertEquals;
import io.netty.buffer.AdaptiveByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.testng.annotations.Test;

public class CommandsMessageSerializationTest {

    @Test
    public void copiesASliceOfADirectBufferIntoAGrowingDirectBuffer() {
        // as the broker copies a slice of a cached entry into the buffer of a consumer's coalesced messages
        ByteBufAllocator allocator = new AdaptiveByteBufAllocator();
        ByteBuf entry = allocator.directBuffer(64);
        entry.writeBytes("entry header,metadata and payload".getBytes(StandardCharsets.UTF_8));
        ByteBuf metadataAndPayload = entry.slice(13, entry.readableBytes() - 13);
        ByteBuf out = allocator.directBuffer(4);
        try {
            BaseCommand command = Commands.newMessageCommand(1, 2, 3, 4, 5, null, 8);
            Commands.serializeCommandMessageWithSize(command, metadataAndPayload, out);
            int firstSize = out.readableBytes();
            Commands.serializeCommandMessageWithSize(command, metadataAndPayload, out);
            assertEquals(out.readableBytes(), 2 * firstSize);
            for (int message = 0; message < 2; message++) {
                int frameStart = message * firstSize;
                assertEquals(out.getInt(frameStart), firstSize - 4);
                int cmdSize = out.getInt(frameStart + 4);
                int payloadStart = frameStart + 8 + cmdSize;
                assertEquals(out.toString(payloadStart, firstSize - 8 - cmdSize, StandardCharsets.UTF_8),
                        "metadata and payload");
            }
            assertEquals(metadataAndPayload.readerIndex(), 0);
        } finally {
            out.release();
            entry.release();
        }
    }

    @Test
    public void copiedMessageHasTheWireFormatOfTheReferencedOne() {
        ByteBuf metadataAndPayload = Unpooled.buffer();
        metadataAndPayload.writeBytes("skipped".getBytes(StandardCharsets.UTF_8));
        metadataAndPayload.writeBytes("metadata and payload".getBytes(StandardCharsets.UTF_8));
        metadataAndPayload.readerIndex(7);
        BaseCommand command = Commands.newMessageCommand(1, 2, 3, 4, 5, new long[] {6, 7}, 8);
        ByteBuf out = Unpooled.buffer();
        out.writeBytes("previous message".getBytes(StandardCharsets.UTF_8));
        int start = out.writerIndex();

        Commands.serializeCommandMessageWithSize(command, metadataAndPayload, out);

        ByteBufPair pair = Commands.serializeCommandMessageWithSize(
                Commands.newMessageCommand(1, 2, 3, 4, 5, new long[] {6, 7}, 8), metadataAndPayload.retain());
        try {
            ByteBuf expected = Unpooled.wrappedBuffer(pair.getFirst().retainedDuplicate(),
                    pair.getSecond().retainedDuplicate());
            assertEquals(ByteBufUtil.hexDump(out, start, out.writerIndex() - start), ByteBufUtil.hexDump(expected));
            expected.release();
        } finally {
            pair.release();
        }
        // the source isn't consumed or released
        assertEquals(metadataAndPayload.readerIndex(), 7);
        assertEquals(metadataAndPayload.refCnt(), 1);
        metadataAndPayload.release();
        out.release();
    }
}
