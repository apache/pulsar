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

import io.netty.util.Recycler;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
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

    /**
     * Process the entries in the given range.
     *
     * @param start the left-closed index of the range
     * @param end the right-open index of the range
     * @param processor the processor that accepts an Entry and the sticky key's hash value
     */
    public void processEntries(final int start, final int end,
                               final BiConsumer<Entry, Long> processor) {
        for (int i = start; i < end; i++) {
            final byte[] stickyKey = peekStickyKey(i);
            if (stickyKey == null) { // the metadata is null
                continue;
            }
            final long stickyKeyHash = StickyKeyConsumerSelector.makeStickyKeyHash(stickyKey);
            processor.accept(entries.get(i), stickyKeyHash);
        }
    }

    /**
     * @see Commands#peekStickyKey
     */
    private byte[] peekStickyKey(final int index) {
        final MessageMetadata metadata = metadataList.get(index);
        if (metadata == null) {
            return null;
        }
        if (metadata.hasOrderingKey()) {
            return metadata.getOrderingKey();
        } else if (metadata.hasPartitionKey()) {
            return metadata.getPartitionKey().getBytes(StandardCharsets.UTF_8);
        } else {
            return "NONE_KEY".getBytes(StandardCharsets.UTF_8);
        }
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
        final Map<String, ChunkedMessageContext> uuidToContext = new HashMap<>();
        final List<Entry> sortedEntries = new ArrayList<>();
        final List<MessageMetadata> sortedMetadataList = new ArrayList<>();
        for (int i = 0; i < size(); i++) {
            final MessageMetadata metadata = metadataList.get(i);
            if (metadata == null) {
                continue;
            }
            if (metadata.hasUuid()) {
                final String uuid = metadata.getUuid();
                final ChunkedMessageContext context =
                        uuidToContext.computeIfAbsent(uuid, __ -> ChunkedMessageContext.get());
                context.add(i, metadata);
                if (context.complete && context.ordered) {
                    int start = sortedEntries.size();
                    int end = sortedEntries.size() + context.indexes.size();
                    chunkIndexRanges.add(IntRange.get(start, end));
                    context.indexes.forEach(index -> {
                        sortedEntries.add(entries.get(index));
                        sortedMetadataList.add(metadataList.get(index));
                    });
                    context.recycle();
                    uuidToContext.remove(uuid);
                }
            } else {
                sortedEntries.add(entries.get(i));
                sortedMetadataList.add(metadata);
            }
        }
        watermark = sortedEntries.size();
        // Process all incomplete chunks
        uuidToContext.values().forEach(context -> {
            context.indexes.forEach(index -> {
                sortedEntries.add(entries.get(index));
                sortedMetadataList.add(metadataList.get(index));
            });
            context.recycle();
        });
        entries = sortedEntries;
        metadataList = sortedMetadataList;
    }

    @RequiredArgsConstructor
    private static class ChunkedMessageContext {
        static final Recycler<ChunkedMessageContext> RECYCLER = new Recycler<ChunkedMessageContext>() {
            @Override
            protected ChunkedMessageContext newObject(Handle<ChunkedMessageContext> handle) {
                return new ChunkedMessageContext(handle);
            }
        };

        final Recycler.Handle<ChunkedMessageContext> handle;

        List<Integer> indexes;
        int lastChunkId;
        boolean ordered;
        boolean complete;

        void add(final int index, final MessageMetadata metadata) {
            indexes.add(index);
            final int chunkId = metadata.getChunkId();
            if (ordered) {
                ordered = (lastChunkId + 1 == chunkId);
            }
            lastChunkId = chunkId;
            complete = (chunkId + 1 == metadata.getNumChunksFromMsg());
        }

        static ChunkedMessageContext get() {
            final ChunkedMessageContext context = RECYCLER.get();
            context.indexes = new ArrayList<>();
            context.lastChunkId = -1; // the first chunk id (0) == lastChunkId (-1) + 1
            context.ordered = true;
            context.complete = false;
            return context;
        }

        void recycle() {
            indexes = null;
            lastChunkId = -1;
            ordered = true;
            complete = false;
            handle.recycle(this);
        }
    }
}
