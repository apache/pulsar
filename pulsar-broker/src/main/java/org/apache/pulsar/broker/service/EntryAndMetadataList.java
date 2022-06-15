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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

@Getter
public class EntryAndMetadataList implements Closeable {

    private final List<IntRange> chunkIndexRanges = new ArrayList<>();
    private List<Entry> entries;
    private List<MessageMetadata> metadataList;
    private int watermark;

    /**
     * It will first parse the metadata into the metadata list and then sort the entries and metadata list.
     *
     * The sort operation is performed to make all chunks of the same message consistent in distribution.
     *
     * All incomplete chunks, i.e. chunks whose message has N chunks but less than N chunks are received, will be put at
     * the end. And the `watermark` field indicates the index of the first incomplete chunk. This design makes it easy
     * to skip all incomplete chunks when dispatching entries to consumer.
     *
     * For example, assuming the original entries are:
     *   M0, M1-C0, M2, M3-C0, M1-C1, M4
     * where M1 and M3 are chunked messages that both have 2 chunks.
     * We should sort them to:
     *   M0, M3, M1-C0, M1-C1, M4, M3-C0
     * and the watermark will be 5 that is the index of `M3-C0` because `M3-C1` is not received.
     */
    public EntryAndMetadataList(final List<Entry> entries, final String subscription) {
        this.entries = entries;
        this.metadataList = new ArrayList<>();
        this.watermark = entries.size();

        boolean hasChunks = false;
        for (Entry entry : entries) {
            final MessageMetadata metadata =
                    Commands.peekAndCopyMessageMetadata(entry.getDataBuffer(), subscription, -1);
            if (!hasChunks && metadata != null && metadata.hasUuid()) {
                hasChunks = true;
            }
            metadataList.add(metadata);
        }
        if (hasChunks) {
            sortChunks();
        }
    }

    @Override
    public void close() {
        chunkIndexRanges.forEach(IntRange::recycle);
    }

    public int size() {
        return entries.size();
    }

    /**
     * Shrink the given range [start, end) to make it include all complete chunks.
     *
     * @param start the left-closed index
     * @param end the right-open index
     * @return the shrunk end index if the given range contains incomplete chunks, otherwise `end`
     */
    public int shrinkEndIfNecessary(final int start, final int end) {
        if (start >= watermark) {
            return end;
        }
        final IntRange firstRange = chunkIndexRanges.stream()
                .filter(range -> range.overlap(start, end))
                .findFirst()
                .orElse(null);
        if (firstRange == null) {
            return end;
        }
        if (firstRange.getStart() < start) {
            // It should not happen because we assume entries.get(start) cannot be a chunk whose chunk id is non-zero
            return -1;
        }
        if (firstRange.getEnd() <= end) {
            // The first range is included in [start, end), let's check the last range
            final IntRange lastRange = findLastOverlapChunk(start, end);
            assert lastRange != null;
            if (lastRange.getEnd() > end) {
                // The last range is not included in [start, end), shrink the end
                return lastRange.getStart();
            }
        } else {
            // The first range is not included in [start, end), shrink the end
            return firstRange.getStart();
        }
        return end;
    }

    private IntRange findLastOverlapChunk(int start, int end) {
        for (int i = chunkIndexRanges.size() - 1; i >= 0; i--) {
            final IntRange range = chunkIndexRanges.get(i);
            if (range.overlap(start, end)) {
                return range;
            }
        }
        return null;
    }

    private void sortChunks() {
        final Map<String, List<Integer>> uuidToIndexes = new HashMap<>();
        final List<Entry> sortedEntries = new ArrayList<>();
        final List<MessageMetadata> sortedMetadataList = new ArrayList<>();
        for (int i = 0; i < size(); i++) {
            final MessageMetadata metadata = metadataList.get(i);
            if (metadata == null) {
                continue;
            }
            if (metadata.hasUuid()) {
                final String uuid = metadata.getUuid();
                final List<Integer> indexes = uuidToIndexes.computeIfAbsent(uuid, __ -> new ArrayList<>());
                indexes.add(i);
                if (metadata.getChunkId() + 1 == metadata.getNumChunksFromMsg()) {
                    // A complete chunk is met
                    int start = sortedEntries.size();
                    int end = start + metadata.getNumChunksFromMsg();
                    chunkIndexRanges.add(IntRange.get(start, end));
                    indexes.forEach(index -> {
                        sortedEntries.add(entries.get(index));
                        sortedMetadataList.add(metadataList.get(index));
                    });
                    uuidToIndexes.remove(uuid);
                }
            } else {
                sortedEntries.add(entries.get(i));
                sortedMetadataList.add(metadata);
            }
        }
        watermark = sortedEntries.size();
        // Process all incomplete chunks
        uuidToIndexes.values().forEach(indexes -> {
            indexes.forEach(index -> {
                sortedEntries.add(entries.get(index));
                sortedMetadataList.add(metadataList.get(index));
            });
        });
        entries = sortedEntries;
        metadataList = sortedMetadataList;
    }
}
