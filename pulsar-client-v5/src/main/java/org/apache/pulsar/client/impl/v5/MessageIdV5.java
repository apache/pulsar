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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.pulsar.client.api.MessageId;

// The v5 MessageId interface and the underlying v4 MessageId class share the same simple
// name in different packages — a real conflict that Java imports can't resolve. We import
// the v4 type as `MessageId` (used heavily) and refer to v5 via FQN in the few places it
// appears (the `implements` clause and `compareTo`).

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
 *
 * <p>Multi-topic consumer wrappers additionally tag the id with the parent scalable
 * topic (for ack routing) and a cross-topic position vector (for cumulative ack
 * across topics). Both fields are optional and round-trip through
 * {@link #toByteArray} / {@link #fromByteArray}.
 */
public final class MessageIdV5 implements org.apache.pulsar.client.api.v5.MessageId {

    static final long NO_SEGMENT = -1;

    static final MessageIdV5 EARLIEST = new MessageIdV5(MessageId.earliest, NO_SEGMENT, Map.of());
    static final MessageIdV5 LATEST = new MessageIdV5(MessageId.latest, NO_SEGMENT, Map.of());

    private final MessageId v4MessageId;
    private final long segmentId;

    /**
     * Position vector: snapshot of the latest delivered message ID per segment,
     * taken at the moment this message was delivered to the application.
     * Used by StreamConsumer for cumulative ack across all segments.
     */
    private final Map<Long, MessageId> positionVector;

    /**
     * Parent scalable topic this message was delivered from. Set only by multi-topic
     * consumer wrappers so they can route a subsequent ack back to the right
     * per-topic consumer; {@code null} for single-topic consumers (where the consumer
     * already knows its topic).
     */
    private final String parentTopic;

    /**
     * Cross-topic position vector for multi-topic StreamConsumer cumulative ack.
     * Maps parent topic name → (segment id → latest-delivered msgId at the moment
     * this message entered the multiplexed queue). {@code null} for single-topic
     * messages — use {@link #positionVector()} instead.
     */
    private final Map<String, Map<Long, MessageId>> multiTopicVector;

    /**
     * Create a MessageIdV5 with a position vector for single-topic cumulative ack.
     */
    public MessageIdV5(MessageId v4MessageId,
                       long segmentId,
                       Map<Long, MessageId> positionVector) {
        this(v4MessageId, segmentId, positionVector, null, null);
    }

    /**
     * Constructor for multi-topic queue consumer messages: parent topic tag for ack
     * routing; no cross-topic position vector (queue consumers don't do cumulative
     * ack).
     */
    public MessageIdV5(MessageId v4MessageId,
                       long segmentId,
                       Map<Long, MessageId> positionVector,
                       String parentTopic) {
        this(v4MessageId, segmentId, positionVector, parentTopic, null);
    }

    /**
     * Create a MessageIdV5 without a position vector (for individual ack / reader use).
     */
    public MessageIdV5(MessageId v4MessageId, long segmentId) {
        this(v4MessageId, segmentId, Map.of(), null, null);
    }

    /**
     * Full constructor for multi-topic StreamConsumer messages: parent topic + the
     * cross-topic position vector captured at enqueue time. Used by the multi-topic
     * stream consumer's pump.
     */
    public MessageIdV5(MessageId v4MessageId,
                       long segmentId,
                       Map<Long, MessageId> positionVector,
                       String parentTopic,
                       Map<String, Map<Long, MessageId>> multiTopicVector) {
        this.v4MessageId = Objects.requireNonNull(v4MessageId);
        this.segmentId = segmentId;
        this.positionVector = Map.copyOf(positionVector);
        this.parentTopic = parentTopic;
        this.multiTopicVector = multiTopicVector;
    }

    /**
     * Get the underlying v4 MessageId. Package-private for internal use.
     */
    MessageId v4MessageId() {
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
    Map<Long, MessageId> positionVector() {
        return positionVector;
    }

    /**
     * Parent scalable topic when this message was delivered through a multi-topic
     * consumer; {@code null} for single-topic consumers. Package-private — used by
     * multi-topic ack routing only.
     */
    String parentTopic() {
        return parentTopic;
    }

    Map<String, Map<Long, MessageId>> multiTopicVector() {
        return multiTopicVector;
    }

    /**
     * Wire format. All sections are length-prefixed so the reader can detect
     * absent trailing sections (older serialised forms wrote only sections 1-3).
     *
     * <pre>
     * 1. segmentId               : 8 bytes
     * 2. v4MessageId             : 4-byte length + bytes
     * 3. positionVector          : 4-byte count + repeating { 8-byte segId,
     *                                                          4-byte len, idBytes }
     * 4. parentTopic             : 4-byte length (-1 = null) + UTF-8 bytes
     * 5. multiTopicVector        : 4-byte count (-1 = null) + repeating {
     *                                4-byte topic name length, UTF-8 bytes,
     *                                4-byte segCount, repeating { 8-byte segId,
     *                                                              4-byte len, idBytes } }
     * </pre>
     *
     * <p>Sections 4 and 5 are present in every new id; older serialisations from a
     * pre-multi-topic build are still readable — the reader treats the missing
     * sections as null.
     */
    @Override
    public byte[] toByteArray() {
        byte[] v4Bytes = v4MessageId.toByteArray();

        // Pre-serialise position-vector entries so we can size the buffer.
        Map<Long, byte[]> serializedPositions = serializeSegmentVector(positionVector);
        int positionBytes = 4; // count
        for (var entry : serializedPositions.entrySet()) {
            positionBytes += 8 + 4 + entry.getValue().length;
        }

        // Section 4: parent topic.
        byte[] parentTopicBytes = parentTopic == null
                ? null : parentTopic.getBytes(StandardCharsets.UTF_8);

        // Section 5: pre-serialise the multi-topic vector tree.
        Map<byte[], Map<Long, byte[]>> serializedMulti = null;
        int multiBytes = 4; // count or -1
        if (multiTopicVector != null) {
            serializedMulti = new HashMap<>(multiTopicVector.size());
            for (var entry : multiTopicVector.entrySet()) {
                byte[] topicBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                Map<Long, byte[]> inner = serializeSegmentVector(entry.getValue());
                serializedMulti.put(topicBytes, inner);
                multiBytes += 4 + topicBytes.length + 4;
                for (var seg : inner.entrySet()) {
                    multiBytes += 8 + 4 + seg.getValue().length;
                }
            }
        }

        int totalSize = 8 + 4 + v4Bytes.length + positionBytes
                + 4 + (parentTopicBytes == null ? 0 : parentTopicBytes.length)
                + multiBytes;

        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.putLong(segmentId);
        buf.putInt(v4Bytes.length);
        buf.put(v4Bytes);

        buf.putInt(serializedPositions.size());
        for (var entry : serializedPositions.entrySet()) {
            buf.putLong(entry.getKey());
            buf.putInt(entry.getValue().length);
            buf.put(entry.getValue());
        }

        if (parentTopicBytes == null) {
            buf.putInt(-1);
        } else {
            buf.putInt(parentTopicBytes.length);
            buf.put(parentTopicBytes);
        }

        if (serializedMulti == null) {
            buf.putInt(-1);
        } else {
            buf.putInt(serializedMulti.size());
            for (var entry : serializedMulti.entrySet()) {
                buf.putInt(entry.getKey().length);
                buf.put(entry.getKey());
                buf.putInt(entry.getValue().size());
                for (var seg : entry.getValue().entrySet()) {
                    buf.putLong(seg.getKey());
                    buf.putInt(seg.getValue().length);
                    buf.put(seg.getValue());
                }
            }
        }
        return buf.array();
    }

    private static Map<Long, byte[]> serializeSegmentVector(Map<Long, MessageId> vector) {
        Map<Long, byte[]> out = new HashMap<>(vector.size());
        for (var entry : vector.entrySet()) {
            out.put(entry.getKey(), entry.getValue().toByteArray());
        }
        return out;
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
        MessageId v4Id = MessageId.fromByteArray(v4Bytes);

        // Section 3: position vector (single-topic / per-segment).
        Map<Long, MessageId> positions = Map.of();
        if (buf.hasRemaining()) {
            positions = readSegmentVector(buf);
        }

        // Section 4: parent topic. Length -1 sentinel means "absent".
        String parentTopic = null;
        if (buf.hasRemaining()) {
            int parentLen = buf.getInt();
            if (parentLen >= 0) {
                if (parentLen > buf.remaining()) {
                    throw new IOException("Invalid MessageIdV5 data: bad parent-topic length");
                }
                byte[] parentBytes = new byte[parentLen];
                buf.get(parentBytes);
                parentTopic = new String(parentBytes, StandardCharsets.UTF_8);
            }
        }

        // Section 5: cross-topic vector. Count -1 sentinel means "absent".
        Map<String, Map<Long, MessageId>> multiTopic = null;
        if (buf.hasRemaining()) {
            int topicCount = buf.getInt();
            if (topicCount >= 0) {
                multiTopic = new HashMap<>(topicCount);
                for (int i = 0; i < topicCount; i++) {
                    int topicLen = buf.getInt();
                    byte[] topicBytes = new byte[topicLen];
                    buf.get(topicBytes);
                    String topic = new String(topicBytes, StandardCharsets.UTF_8);
                    Map<Long, MessageId> inner = readSegmentVector(buf);
                    multiTopic.put(topic, inner);
                }
            }
        }

        return new MessageIdV5(v4Id, segmentId, positions, parentTopic, multiTopic);
    }

    private static Map<Long, MessageId> readSegmentVector(ByteBuffer buf) throws IOException {
        int count = buf.getInt();
        Map<Long, MessageId> out = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            long segId = buf.getLong();
            int idLen = buf.getInt();
            if (idLen < 0 || idLen > buf.remaining()) {
                throw new IOException("Invalid MessageIdV5 data: bad inner id length");
            }
            byte[] idBytes = new byte[idLen];
            buf.get(idBytes);
            out.put(segId, MessageId.fromByteArray(idBytes));
        }
        return out;
    }

    @Override
    public int compareTo(org.apache.pulsar.client.api.v5.MessageId other) {
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
