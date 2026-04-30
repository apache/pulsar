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
import java.util.Map;
import java.util.Objects;
import org.apache.pulsar.client.api.v5.MessageId;

/**
 * V5 MessageId implementation that wraps a v4 MessageId and includes a position
 * vector across all segments.
 *
 * <p>The position vector captures the latest delivered message ID per segment at the
 * moment this message was dequeued. This enables correct cumulative acknowledgment:
 * when the application acknowledges this message, all segments are advanced to the
 * positions recorded in the vector, not just the segment this message came from.
 *
 * <p>For non-cumulative consumers (QueueConsumer) and readers (CheckpointConsumer),
 * only the single segment ID and v4 message ID are needed; the position vector is
 * empty.
 */
public final class MessageIdV5 implements MessageId {

    static final long NO_SEGMENT = -1;

    static final MessageIdV5 EARLIEST = new MessageIdV5(
            org.apache.pulsar.client.api.MessageId.earliest, NO_SEGMENT, Map.of());
    static final MessageIdV5 LATEST = new MessageIdV5(
            org.apache.pulsar.client.api.MessageId.latest, NO_SEGMENT, Map.of());

    private final org.apache.pulsar.client.api.MessageId v4MessageId;
    private final long segmentId;

    /**
     * Position vector: snapshot of the latest delivered message ID per segment,
     * taken at the moment this message was delivered to the application.
     * Used by StreamConsumer for cumulative ack across all segments.
     */
    private final Map<Long, org.apache.pulsar.client.api.MessageId> positionVector;

    /**
     * Create a MessageIdV5 with a position vector for cumulative ack support.
     */
    public MessageIdV5(org.apache.pulsar.client.api.MessageId v4MessageId,
                       long segmentId,
                       Map<Long, org.apache.pulsar.client.api.MessageId> positionVector) {
        this.v4MessageId = Objects.requireNonNull(v4MessageId);
        this.segmentId = segmentId;
        this.positionVector = Map.copyOf(positionVector);
    }

    /**
     * Create a MessageIdV5 without a position vector (for individual ack / reader use).
     */
    public MessageIdV5(org.apache.pulsar.client.api.MessageId v4MessageId, long segmentId) {
        this(v4MessageId, segmentId, Map.of());
    }

    /**
     * Get the underlying v4 MessageId. Package-private for internal use.
     */
    org.apache.pulsar.client.api.MessageId v4MessageId() {
        return v4MessageId;
    }

    /**
     * Get the segment ID this message belongs to. Package-private for internal routing.
     */
    long segmentId() {
        return segmentId;
    }

    /**
     * Get the position vector — the latest delivered message ID per segment at the
     * time this message was delivered. Used by StreamConsumer for cumulative ack.
     */
    Map<Long, org.apache.pulsar.client.api.MessageId> positionVector() {
        return positionVector;
    }

    @Override
    public byte[] toByteArray() {
        byte[] v4Bytes = v4MessageId.toByteArray();
        // Format: [8 bytes segmentId] [4 bytes v4Length] [v4Bytes]
        //         [4 bytes numPositions] [for each: [8 bytes segId] [4 bytes idLen] [idBytes]]
        int totalSize = 8 + 4 + v4Bytes.length + 4;
        var serializedPositions = new java.util.HashMap<Long, byte[]>();
        for (var entry : positionVector.entrySet()) {
            byte[] idBytes = entry.getValue().toByteArray();
            serializedPositions.put(entry.getKey(), idBytes);
            totalSize += 8 + 4 + idBytes.length;
        }

        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.putLong(segmentId);
        buf.putInt(v4Bytes.length);
        buf.put(v4Bytes);
        buf.putInt(positionVector.size());
        for (var entry : serializedPositions.entrySet()) {
            buf.putLong(entry.getKey());
            buf.putInt(entry.getValue().length);
            buf.put(entry.getValue());
        }
        return buf.array();
    }

    static MessageIdV5 fromByteArray(byte[] data) throws IOException {
        if (data == null || data.length < 12) {
            throw new IOException("Invalid MessageIdV5 data: too short");
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        long segmentId = buf.getLong();
        int v4Length = buf.getInt();
        if (v4Length < 0 || v4Length > buf.remaining()) {
            throw new IOException("Invalid MessageIdV5 data: bad v4 length");
        }
        byte[] v4Bytes = new byte[v4Length];
        buf.get(v4Bytes);
        org.apache.pulsar.client.api.MessageId v4Id =
                org.apache.pulsar.client.api.MessageId.fromByteArray(v4Bytes);

        // Read position vector if present
        Map<Long, org.apache.pulsar.client.api.MessageId> positions = Map.of();
        if (buf.hasRemaining()) {
            int numPositions = buf.getInt();
            var posMap = new java.util.HashMap<Long, org.apache.pulsar.client.api.MessageId>();
            for (int i = 0; i < numPositions; i++) {
                long posSegId = buf.getLong();
                int idLen = buf.getInt();
                byte[] idBytes = new byte[idLen];
                buf.get(idBytes);
                posMap.put(posSegId,
                        org.apache.pulsar.client.api.MessageId.fromByteArray(idBytes));
            }
            positions = posMap;
        }

        return new MessageIdV5(v4Id, segmentId, positions);
    }

    @Override
    public int compareTo(MessageId other) {
        if (!(other instanceof MessageIdV5 o)) {
            throw new IllegalArgumentException("Cannot compare with " + other.getClass());
        }
        int cmp = Long.compare(this.segmentId, o.segmentId);
        if (cmp != 0) {
            return cmp;
        }
        return this.v4MessageId.compareTo(o.v4MessageId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MessageIdV5 o)) {
            return false;
        }
        return segmentId == o.segmentId && v4MessageId.equals(o.v4MessageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(v4MessageId, segmentId);
    }

    @Override
    public String toString() {
        return "MessageIdV5{segment=" + segmentId + ", id=" + v4MessageId
                + ", positions=" + positionVector.size() + "}";
    }
}
