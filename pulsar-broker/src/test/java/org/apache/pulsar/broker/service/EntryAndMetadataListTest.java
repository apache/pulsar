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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.Test;

public class EntryAndMetadataListTest {

    @Test
    public void testNoChunk() {
        final List<Entry> entries = attachMetadataList(
                createMetadata("A", 0),
                createMetadata("B", 0),
                createMetadata("A", 1)
        );
        @Cleanup
        final EntryAndMetadataList entryAndMetadataList = new EntryAndMetadataList(entries, "sub");
        assertSame(entryAndMetadataList.getEntries(), entries);
        assertEquals(entryAndMetadataList.getWatermark(), entries.size());
        assertTrue(entryAndMetadataList.getChunkIndexRanges().isEmpty());
    }

    @Test
    public void testSortChunks() {
        @Cleanup
        final EntryAndMetadataList entryAndMetadataList = new EntryAndMetadataList(attachMetadataList(
                createMetadata("A", 0),
                createMetadata("B", 0, 0, 3),
                createMetadata("C", 0, 0, 4),
                createMetadata("A", 1),
                createMetadata("C", 0, 1, 4),
                createMetadata("B", 0, 1, 3),
                createMetadata("D", 0),
                createMetadata("B", 0, 2, 3),
                createMetadata("A", 2)
        ), "sub");
        final List<String> metadataDescriptionList = entryAndMetadataList.getMetadataList().stream()
                .map(m -> {
                    if (m == null) {
                        return "null";
                    } else if (m.hasUuid()) {
                        return m.getProducerName() + "-" + m.getSequenceId() + "-" + m.getChunkId();
                    } else {
                        return m.getProducerName() + "-" + m.getSequenceId();
                    }
                }).collect(Collectors.toList());
        // C-0 has 4 chunks while the latest chunk's id is 1, so C-0-0 and C-0-1 will be put at the end
        assertEquals(metadataDescriptionList,
                Arrays.asList("A-0", "A-1", "D-0", "B-0-0", "B-0-1", "B-0-2", "A-2", "C-0-0", "C-0-1"));
        assertEquals(entryAndMetadataList.getChunkIndexRanges(), Collections.singletonList(IntRange.get(3, 6)));
        assertEquals(entryAndMetadataList.getWatermark(), 7);
    }

    @Test
    public void testShrinkRangeEnd() {
        @Cleanup
        final EntryAndMetadataList entryAndMetadataList = new EntryAndMetadataList(attachMetadataList(
                // B-0 has 3 chunks but only 2 chunks are received
                createMetadata("B", 0, 0, 3),
                createMetadata("B", 0, 1, 3),
                createMetadata("A", 0, 0, 3),
                createMetadata("A", 0, 1, 3),
                createMetadata("A", 0, 2, 3),
                createMetadata("A", 1),
                createMetadata("A", 2, 0, 2),
                createMetadata("A", 2, 1, 2),
                createMetadata("A", 3)
        ), "sub");
        // The order: A-0-0, A-0-1, A-0-2, A-1, A-2-0, A-2-1, A-3, B-0-1, B-0-2
        assertEquals(entryAndMetadataList.getWatermark(), 7);
        // end is shrunk because it cannot include the whole A-0 (first overlapped chunked message)
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(0, 1), 0);
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(0, 2), 0);
        // end is not shrunk
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(0, 3), 3);
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(0, 4), 4);
        // end is shrunk because it cannot include the whole A-2 (last overlapped chunked message)
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(0, 5), 4);
        // end is not shrunk
        for (int end = 6; end <= 10; end++) {
            assertEquals(entryAndMetadataList.shrinkEndIfNecessary(0, end), end);
        }
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(0, 6), 6);
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(0, 7), 7);
        // end is shrunk because it cannot include the whole A-2 (the only overlapped chunked message)
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(3, 5), 4);
        // end is not shrunk
        for (int end = 6; end <= 10; end++) {
            assertEquals(entryAndMetadataList.shrinkEndIfNecessary(3, end), end);
        }
        // invalid start
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(1, 4), -1);
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(2, 4), -1);
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(5, 100), -1);
        // No overlapped chunked messages
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(3, 4), 4);
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(8, 100), 100);
        assertEquals(entryAndMetadataList.shrinkEndIfNecessary(9, 110), 110);
    }

    private static List<Entry> attachMetadataList(MessageMetadata... metadataList) {
        final List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < metadataList.length; i++) {
            entries.add(createEntry(i, metadataList[i]));
        }
        return entries;
    }

    private static Entry createEntry(long entryId, MessageMetadata metadata) {
        final ByteBuf payload = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, metadata, PulsarByteBufAllocator.DEFAULT.buffer());
        return EntryImpl.create(0, entryId, payload);
    }

    private static MessageMetadata createMetadata(String producerName, long sequenceId) {
        final MessageMetadata metadata = createMetadata(producerName, sequenceId, 0, 0);
        metadata.clearUuid();
        metadata.clearChunkId();
        metadata.clearNumChunksFromMsg();
        return metadata;
    }

    private static MessageMetadata createMetadata(String producerName, long sequenceId, int chunkId, int numChunks) {
        final MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName(producerName);
        metadata.setSequenceId(sequenceId);
        metadata.setPublishTime(0L);
        metadata.setUuid(producerName + "-" + sequenceId);
        metadata.setChunkId(chunkId);
        metadata.setNumChunksFromMsg(numChunks);
        return metadata;
    }
}
