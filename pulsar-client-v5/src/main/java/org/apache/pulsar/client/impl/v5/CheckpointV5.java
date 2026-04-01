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
package org.apache.pulsar.client.impl.v5;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.v5.Checkpoint;

/**
 * V5 Checkpoint implementation that stores a position vector across all segments.
 * Each entry maps a segment ID to a v4 MessageId position within that segment.
 */
final class CheckpointV5 implements Checkpoint {

    // Sentinel type markers for serialization
    private static final byte TYPE_REGULAR = 0;
    private static final byte TYPE_EARLIEST = 1;
    private static final byte TYPE_LATEST = 2;
    private static final byte TYPE_TIMESTAMP = 3;

    static final Checkpoint EARLIEST = new SentinelCheckpoint(TYPE_EARLIEST, Instant.EPOCH);
    static final Checkpoint LATEST = new SentinelCheckpoint(TYPE_LATEST, Instant.EPOCH);

    private final Map<Long, org.apache.pulsar.client.api.MessageId> segmentPositions;
    private final Instant creationTime;

    CheckpointV5(Map<Long, org.apache.pulsar.client.api.MessageId> segmentPositions, Instant creationTime) {
        this.segmentPositions = Map.copyOf(segmentPositions);
        this.creationTime = creationTime;
    }

    /**
     * Get the position map. Package-private for internal use.
     */
    Map<Long, org.apache.pulsar.client.api.MessageId> segmentPositions() {
        return segmentPositions;
    }

    @Override
    public Instant creationTime() {
        return creationTime;
    }

    @Override
    public byte[] toByteArray() {
        // Format: [1 byte type] [8 bytes creationTimeMillis] [4 bytes numEntries]
        //         [for each entry: [8 bytes segmentId] [4 bytes msgIdLen] [msgIdBytes]]
        int totalSize = 1 + 8 + 4;
        Map<Long, byte[]> serializedIds = new HashMap<>();
        for (var entry : segmentPositions.entrySet()) {
            byte[] idBytes = entry.getValue().toByteArray();
            serializedIds.put(entry.getKey(), idBytes);
            totalSize += 8 + 4 + idBytes.length;
        }

        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.put(TYPE_REGULAR);
        buf.putLong(creationTime.toEpochMilli());
        buf.putInt(segmentPositions.size());
        for (var entry : serializedIds.entrySet()) {
            buf.putLong(entry.getKey());
            buf.putInt(entry.getValue().length);
            buf.put(entry.getValue());
        }
        return buf.array();
    }

    static Checkpoint fromByteArray(byte[] data) throws IOException {
        if (data == null || data.length < 1) {
            throw new IOException("Invalid checkpoint data: empty");
        }

        ByteBuffer buf = ByteBuffer.wrap(data);
        byte type = buf.get();

        return switch (type) {
            case TYPE_EARLIEST -> EARLIEST;
            case TYPE_LATEST -> LATEST;
            case TYPE_TIMESTAMP -> {
                long millis = buf.getLong();
                yield new TimestampCheckpoint(Instant.ofEpochMilli(millis));
            }
            case TYPE_REGULAR -> {
                long creationMillis = buf.getLong();
                int numEntries = buf.getInt();
                Map<Long, org.apache.pulsar.client.api.MessageId> positions = new HashMap<>();
                for (int i = 0; i < numEntries; i++) {
                    long segmentId = buf.getLong();
                    int msgIdLen = buf.getInt();
                    byte[] msgIdBytes = new byte[msgIdLen];
                    buf.get(msgIdBytes);
                    positions.put(segmentId,
                            org.apache.pulsar.client.api.MessageId.fromByteArray(msgIdBytes));
                }
                yield new CheckpointV5(positions, Instant.ofEpochMilli(creationMillis));
            }
            default -> throw new IOException("Unknown checkpoint type: " + type);
        };
    }

    static Checkpoint atTimestamp(Instant timestamp) {
        return new TimestampCheckpoint(timestamp);
    }

    /**
     * Sentinel checkpoint for earliest/latest positions.
     */
    private record SentinelCheckpoint(byte type, Instant creation) implements Checkpoint {
        @Override
        public byte[] toByteArray() {
            ByteBuffer buf = ByteBuffer.allocate(1);
            buf.put(type);
            return buf.array();
        }

        @Override
        public Instant creationTime() {
            return creation;
        }
    }

    /**
     * Checkpoint that positions at a specific timestamp.
     */
    private record TimestampCheckpoint(Instant timestamp) implements Checkpoint {
        @Override
        public byte[] toByteArray() {
            ByteBuffer buf = ByteBuffer.allocate(1 + 8);
            buf.put(TYPE_TIMESTAMP);
            buf.putLong(timestamp.toEpochMilli());
            return buf.array();
        }

        @Override
        public Instant creationTime() {
            return timestamp;
        }
    }
}
