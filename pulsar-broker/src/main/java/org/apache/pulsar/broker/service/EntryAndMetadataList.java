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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

@Getter
public class EntryAndMetadataList {

    private List<Entry> entries;
    private List<MessageMetadata> metadataList;
    private int watermark;
    private boolean hasChunks;

    public EntryAndMetadataList(final List<Entry> entries, final String subscription) {
        this.entries = entries;
        this.metadataList = new ArrayList<>();
        this.watermark = entries.size();
        this.hasChunks = false;
        entries.forEach(entry -> {
            final MessageMetadata metadata =
                    Commands.peekAndCopyMessageMetadata(entry.getDataBuffer(), subscription, -1);
            if (!hasChunks && metadata != null && metadata.hasUuid()) {
                hasChunks = true;
            }
            metadataList.add(metadata);
        });
    }

    public int size() {
        return entries.size();
    }

    /**
     * it will sort the entries to make all chunks of the same message consistent in distribution.
     *
     * For example, if the original entries are:
     *   M0, M1-C0, M2, M3, M1-C1, M4
     * where M1 is a chunked message that has 2 chunks (C0 and C1).
     * We should sort them to:
     *   M0, M2, M3, M1-C0, M1-C1, M4
     */
    public void sortChunks() {
        if (!hasChunks) {
            return;
        }
        final Map<String, List<Integer>> uuidToIndexes = new HashMap<>();
        final List<Entry> sortedEntries = new ArrayList<>();
        final List<MessageMetadata> sortedMetadataList = new ArrayList<>();
        for (int i = 0; i < entries.size(); i++) {
            final MessageMetadata metadata = metadataList.get(i);
            if (metadata == null) {
                continue;
            }
            if (metadata.hasUuid()) {
                final String uuid = metadata.getUuid();
                final List<Integer> indexes = uuidToIndexes.computeIfAbsent(uuid, __ -> new ArrayList<>());
                indexes.add(i);
                if (metadata.getChunkId() + 1 == metadata.getNumChunksFromMsg()) {
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

    // NOTE: it must be called after sortChunks() to ensure chunks of the same message are distributed consistently.
    public boolean containIncompleteChunks(int start, int end) {
        String currentUuid = null;
        Optional<Boolean> optIsLastChunk = Optional.empty();
        for (int i = start; i < end; i++) {
            final MessageMetadata metadata = metadataList.get(i);
            if (metadata == null) {
                continue;
            }
            if (metadata.hasUuid()) {
                final String uuid = metadata.getUuid();
                if (!uuid.equals(currentUuid)) {
                    if (metadata.getChunkId() != 0) {
                        return true;
                    }
                    if (optIsLastChunk.isPresent() && !optIsLastChunk.get()) {
                        return true;
                    }
                    currentUuid = uuid;
                }
                optIsLastChunk = Optional.of(metadata.getChunkId() == metadata.getNumChunksFromMsg() - 1);
            } else if (currentUuid != null) {
                if (!optIsLastChunk.get()) {
                    return true;
                }
            }
        }
        if (optIsLastChunk.isEmpty()) {
            // There are no chunks
            return false;
        } else {
            // Verify the last entry
            return !optIsLastChunk.get();
        }
    }
}
