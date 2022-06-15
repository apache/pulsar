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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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
        final EntryAndMetadataList entryAndMetadataList = new EntryAndMetadataList(entries, "sub");
        assertFalse(entryAndMetadataList.isHasChunks());

        entryAndMetadataList.sortChunks();
        assertSame(entryAndMetadataList.getEntries(), entries);
        assertEquals(entryAndMetadataList.getWatermark(), entries.size());
    }

    @Test
    public void testSortChunks() {
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
        assertTrue(entryAndMetadataList.isHasChunks());
        entryAndMetadataList.sortChunks();
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
        assertEquals(metadataDescriptionList,
                Arrays.asList("A-0", "A-1", "D-0", "B-0-0", "B-0-1", "B-0-2", "A-2", "C-0-0", "C-0-1"));
        assertEquals(entryAndMetadataList.getWatermark(), 7);
    }

    @Test
    public void testContainIncompleteChunks() {
        final EntryAndMetadataList entryAndMetadataList = new EntryAndMetadataList(attachMetadataList(
                createMetadata("A", 0),
                createMetadata("B", 0),
                // entry id range of chunk C-0: [2, 5)
                createMetadata("C", 0, 0, 3),
                createMetadata("C", 0, 1, 3),
                createMetadata("C", 0, 2, 3),
                // entry id range of chunk C-1: [5, 7)
                createMetadata("C", 1, 0, 2),
                createMetadata("C", 1, 1, 2),
                createMetadata("B", 1),
                // entry id range of chunk D-0: [8, 11)
                createMetadata("D", 0, 0, 3),
                createMetadata("D", 0, 1, 3),
                createMetadata("D", 0, 2, 3)
        ), "sub");
        final int size = entryAndMetadataList.getEntries().size();
        for (int end = 1; end <= size; end++) {
            if ((end > 2 && end < 5) || (end == 6) || (end > 8 && end < 11)) {
                assertTrue(entryAndMetadataList.containIncompleteChunks(0, end));
            } else {
                assertFalse(entryAndMetadataList.containIncompleteChunks(0, end));
            }
        }
        final Set<Integer> indexOfNotFirstChunk = new HashSet<>(Arrays.asList(3, 4, 6, 9, 10));
        for (int start = 0; start < size; start++) {
            if (indexOfNotFirstChunk.contains(start)) {
                assertTrue(entryAndMetadataList.containIncompleteChunks(start, size));
            } else {
                assertFalse(entryAndMetadataList.containIncompleteChunks(start, size));
            }
        }
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
