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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import static org.testng.Assert.assertEquals;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.OffloadSegmentInfoImpl;
import org.testng.annotations.Test;
import org.testng.Assert;

public class BufferedOffloadStreamTest {
    final Random random = new Random();

    private void testWithPadding(int paddingLen) throws Exception {
        int blockSize = StreamingDataBlockHeaderImpl.getDataStartOffset();
        List<Entry> entryBuffer = new LinkedList<>();
        final UUID uuid = UUID.randomUUID();
        OffloadSegmentInfoImpl segmentInfo = new OffloadSegmentInfoImpl(uuid, 0, 0, "",
                new HashMap<>());
        final int entryCount = 10;
        List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            final byte[] bytes = new byte[random.nextInt(10)];
            final EntryImpl entry = EntryImpl.create(0, i, bytes);
            entries.add(entry);
            entry.retain();
            entryBuffer.add(entry);
            blockSize += BufferedOffloadStream.ENTRY_HEADER_SIZE + entry.getLength();
        }
        segmentInfo.closeSegment(0, 9);
        blockSize += paddingLen;

        final BufferedOffloadStream inputStream = new BufferedOffloadStream(blockSize, entryBuffer,
                segmentInfo.beginLedgerId,
                segmentInfo.beginEntryId);
        Assert.assertEquals(inputStream.getLedgerId(), 0);
        Assert.assertEquals(inputStream.getBeginEntryId(), 0);
        Assert.assertEquals(inputStream.getBlockSize(), blockSize);

        byte[] headerB = new byte[DataBlockHeaderImpl.getDataStartOffset()];
        ByteStreams.readFully(inputStream, headerB);
        StreamingDataBlockHeaderImpl headerRead = StreamingDataBlockHeaderImpl
                .fromStream(new ByteArrayInputStream(headerB));
        assertEquals(headerRead.getBlockLength(), blockSize);
        assertEquals(headerRead.getFirstEntryId(), 0);

        int left = blockSize - DataBlockHeaderImpl.getDataStartOffset();
        for (int i = 0; i < entryCount; i++) {
            byte[] lengthBuf = new byte[4];
            byte[] entryIdBuf = new byte[8];
            byte[] content = new byte[entries.get(i).getLength()];

            left -= lengthBuf.length + entryIdBuf.length + content.length;
            inputStream.read(lengthBuf);
            inputStream.read(entryIdBuf);
            inputStream.read(content);
            assertEquals(entries.get(i).getLength(), Ints.fromByteArray(lengthBuf));
            assertEquals(i, Longs.fromByteArray(entryIdBuf));
            assertEquals(entries.get(i).getData(), content);
        }
        Assert.assertEquals(left, paddingLen);
        byte[] padding = new byte[left];
        inputStream.read(padding);

        ByteBuf paddingBuf = Unpooled.wrappedBuffer(padding);
        for (int i = 0; i < paddingBuf.capacity() / 4; i++) {
            assertEquals(Integer.toHexString(paddingBuf.readInt()),
                    Integer.toHexString(0xFEDCDEAD));
        }

        // 4. reach end.
        assertEquals(inputStream.read(), -1);
        assertEquals(inputStream.read(), -1);
        inputStream.close();
    }

    @Test
    public void testHavePadding() throws Exception {
        testWithPadding(10);
    }

    @Test
    public void testNoPadding() throws Exception {
        testWithPadding(0);
    }

    @Test(enabled = false, description = "Disable because let offloader to ensure there is no another ledger id")
    public void shouldEndWhenSegmentChanged() throws IOException {
        int blockSize = StreamingDataBlockHeaderImpl.getDataStartOffset();
        int paddingLen = 10;
        List<Entry> entryBuffer = new LinkedList<>();
        final UUID uuid = UUID.randomUUID();
        OffloadSegmentInfoImpl segmentInfo = new OffloadSegmentInfoImpl(uuid, 0, 0, "",
                new HashMap<>());
        AtomicLong bufferLength = new AtomicLong();
        final int entryCount = 10;
        List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            final byte[] bytes = new byte[random.nextInt(10)];
            final EntryImpl entry = EntryImpl.create(0, i, bytes);
            entries.add(entry);
            entry.retain();
            entryBuffer.add(entry);
            blockSize += BufferedOffloadStream.ENTRY_HEADER_SIZE + entry.getLength();
        }
        //create new ledger
        {
            final byte[] bytes = new byte[random.nextInt(10)];
            final EntryImpl entry = EntryImpl.create(1, 0, bytes);
            entries.add(entry);
            entry.retain();
            entryBuffer.add(entry);
        }
        segmentInfo.closeSegment(1, 0);
        blockSize += paddingLen;

        final BufferedOffloadStream inputStream = new BufferedOffloadStream(blockSize, entryBuffer,
                segmentInfo.beginLedgerId,
                segmentInfo.beginEntryId);
        Assert.assertEquals(inputStream.getLedgerId(), 0);
        Assert.assertEquals(inputStream.getBeginEntryId(), 0);
        Assert.assertEquals(inputStream.getBlockSize(), blockSize);

        byte headerB[] = new byte[DataBlockHeaderImpl.getDataStartOffset()];
        ByteStreams.readFully(inputStream, headerB);
        StreamingDataBlockHeaderImpl headerRead = StreamingDataBlockHeaderImpl
                .fromStream(new ByteArrayInputStream(headerB));
        assertEquals(headerRead.getBlockLength(), blockSize);
        assertEquals(headerRead.getFirstEntryId(), 0);

        int left = blockSize - DataBlockHeaderImpl.getDataStartOffset();
        for (int i = 0; i < entryCount; i++) {
            byte[] lengthBuf = new byte[4];
            byte[] entryIdBuf = new byte[8];
            byte[] content = new byte[entries.get(i).getLength()];

            left -= lengthBuf.length + entryIdBuf.length + content.length;
            inputStream.read(lengthBuf);
            inputStream.read(entryIdBuf);
            inputStream.read(content);
            assertEquals(entries.get(i).getLength(), Ints.fromByteArray(lengthBuf));
            assertEquals(i, Longs.fromByteArray(entryIdBuf));
            assertEquals(entries.get(i).getData(), content);
        }
        Assert.assertEquals(left, paddingLen);
        byte[] padding = new byte[left];
        inputStream.read(padding);

        ByteBuf paddingBuf = Unpooled.wrappedBuffer(padding);
        for (int i = 0; i < paddingBuf.capacity() / 4; i++) {
            assertEquals(Integer.toHexString(paddingBuf.readInt()),
                    Integer.toHexString(0xFEDCDEAD));
        }

        // 4. reach end.
        assertEquals(inputStream.read(), -1);
        assertEquals(inputStream.read(), -1);
        inputStream.close();

        Assert.assertEquals(entryBuffer.size(), 1);
    }
}
