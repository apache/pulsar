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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MessageIdV5Test {

    private static MessageId v4(long ledger, long entry, int partition) {
        return new MessageIdImpl(ledger, entry, partition);
    }

    // --- construction ---

    @Test
    public void testConstructorRejectsNullV4MessageId() {
        assertThrows(NullPointerException.class,
                () -> new MessageIdV5(null, 0L, Map.of()));
    }

    @Test
    public void testConstructorWithoutPositionVectorUsesEmptyMap() {
        MessageIdV5 id = new MessageIdV5(v4(1, 2, 0), 7L);
        assertEquals(id.segmentId(), 7L);
        assertEquals(id.positionVector().size(), 0);
    }

    @Test
    public void testPositionVectorIsImmutable() {
        Map<Long, MessageId> mutable = new HashMap<>();
        mutable.put(0L, v4(1, 2, 0));
        MessageIdV5 id = new MessageIdV5(v4(1, 2, 0), 0L, mutable);
        // Mutating the source map after construction must not affect the stored vector.
        mutable.put(1L, v4(3, 4, 0));
        assertEquals(id.positionVector().size(), 1);
        // The returned map is unmodifiable.
        assertThrows(UnsupportedOperationException.class,
                () -> id.positionVector().put(99L, v4(0, 0, 0)));
    }

    // --- equals / hashCode ---

    @Test
    public void testEqualsIsByV4IdAndSegmentId() {
        MessageIdV5 a = new MessageIdV5(v4(1, 2, 0), 7L);
        MessageIdV5 b = new MessageIdV5(v4(1, 2, 0), 7L);
        MessageIdV5 c = new MessageIdV5(v4(1, 2, 0), 8L);
        MessageIdV5 d = new MessageIdV5(v4(1, 3, 0), 7L);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
        assertFalse(a.equals(d));
        assertFalse(a.equals("not-a-message-id"));
        assertFalse(a.equals(null));
    }

    @Test
    public void testEqualsIgnoresPositionVector() {
        // Position vector is diagnostic state for cumulative ack, not part of identity.
        Map<Long, MessageId> posA = Map.of(0L, v4(1, 2, 0));
        Map<Long, MessageId> posB = Map.of(0L, v4(1, 2, 0), 1L, v4(5, 6, 0));

        MessageIdV5 a = new MessageIdV5(v4(1, 2, 0), 7L, posA);
        MessageIdV5 b = new MessageIdV5(v4(1, 2, 0), 7L, posB);
        assertEquals(a, b);
    }

    // --- compareTo ---

    @Test
    public void testCompareToBySegmentIdFirst() {
        MessageIdV5 a = new MessageIdV5(v4(10, 10, 0), 1L);
        MessageIdV5 b = new MessageIdV5(v4(0, 0, 0), 2L);
        assertTrue(a.compareTo(b) < 0, "lower segmentId compares less regardless of v4 id");
        assertTrue(b.compareTo(a) > 0);
    }

    @Test
    public void testCompareToByV4WithinSameSegment() {
        MessageIdV5 a = new MessageIdV5(v4(1, 2, 0), 0L);
        MessageIdV5 b = new MessageIdV5(v4(1, 3, 0), 0L);
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
        assertEquals(new MessageIdV5(v4(1, 2, 0), 0L).compareTo(a), 0);
    }

    @Test
    public void testCompareToRejectsForeignType() {
        MessageIdV5 id = new MessageIdV5(v4(1, 2, 0), 0L);
        // A v5 MessageId that isn't a MessageIdV5 must be rejected by compareTo.
        org.apache.pulsar.client.api.v5.MessageId foreign =
                new org.apache.pulsar.client.api.v5.MessageId() {
                    @Override
                    public byte[] toByteArray() {
                        return new byte[0];
                    }

                    @Override
                    public int compareTo(org.apache.pulsar.client.api.v5.MessageId o) {
                        return 0;
                    }
                };
        assertThrows(IllegalArgumentException.class, () -> id.compareTo(foreign));
    }

    // --- byte-array roundtrip ---

    @Test
    public void testRoundtripWithoutPositionVector() throws Exception {
        MessageIdV5 original = new MessageIdV5(v4(42, 99, 3), 7L);

        byte[] bytes = original.toByteArray();
        MessageIdV5 decoded = MessageIdV5.fromByteArray(bytes);

        assertEquals(decoded.segmentId(), 7L);
        assertEquals(decoded.v4MessageId(), v4(42, 99, 3));
        assertEquals(decoded.positionVector().size(), 0);
        assertEquals(decoded, original);
    }

    @Test
    public void testRoundtripWithPositionVector() throws Exception {
        Map<Long, MessageId> positions = Map.of(
                0L, v4(1, 2, 0),
                1L, v4(3, 4, 0),
                2L, v4(5, 6, 0));
        MessageIdV5 original = new MessageIdV5(v4(10, 20, 0), 1L, positions);

        byte[] bytes = original.toByteArray();
        MessageIdV5 decoded = MessageIdV5.fromByteArray(bytes);

        assertEquals(decoded.segmentId(), 1L);
        assertEquals(decoded.v4MessageId(), v4(10, 20, 0));
        assertEquals(decoded.positionVector().size(), 3);
        assertEquals(decoded.positionVector().get(0L), v4(1, 2, 0));
        assertEquals(decoded.positionVector().get(1L), v4(3, 4, 0));
        assertEquals(decoded.positionVector().get(2L), v4(5, 6, 0));
    }

    @Test
    public void testFromByteArrayRejectsNull() {
        assertThrows(java.io.IOException.class, () -> MessageIdV5.fromByteArray(null));
    }

    @Test
    public void testRoundtripWithParentTopic() throws Exception {
        // Multi-topic queue consumer ids carry a parent-topic tag for ack routing
        // but no cross-topic vector. Both must survive serialisation.
        MessageIdV5 original = new MessageIdV5(v4(11, 22, 0), 3L,
                Map.of(0L, v4(1, 2, 0)),
                "topic://tenant/ns/my-topic");

        MessageIdV5 decoded = MessageIdV5.fromByteArray(original.toByteArray());

        assertEquals(decoded.segmentId(), 3L);
        assertEquals(decoded.parentTopic(), "topic://tenant/ns/my-topic");
        assertEquals(decoded.positionVector().size(), 1);
        // No multi-topic vector was set; round-trip must preserve null.
        assertEquals(decoded.multiTopicVector(), null);
    }

    @Test
    public void testRoundtripWithMultiTopicVector() throws Exception {
        // Multi-topic stream consumer ids carry a cross-topic position vector and
        // the parent topic. Whole tree must survive byte-level round trip so an
        // application that serialises the id and sends it through can still call
        // acknowledgeCumulative against the right per-topic / per-segment positions.
        Map<String, Map<Long, MessageId>> multi = Map.of(
                "topic://tenant/ns/a", Map.of(0L, v4(1, 1, 0), 1L, v4(2, 2, 0)),
                "topic://tenant/ns/b", Map.of(0L, v4(3, 3, 0)));
        MessageIdV5 original = new MessageIdV5(v4(99, 100, 0), 5L,
                Map.of(0L, v4(1, 1, 0), 1L, v4(2, 2, 0)),
                "topic://tenant/ns/a",
                multi);

        MessageIdV5 decoded = MessageIdV5.fromByteArray(original.toByteArray());

        assertEquals(decoded.segmentId(), 5L);
        assertEquals(decoded.parentTopic(), "topic://tenant/ns/a");
        assertEquals(decoded.positionVector().size(), 2);
        assertNotNull(decoded.multiTopicVector());
        assertEquals(decoded.multiTopicVector().keySet(),
                java.util.Set.of("topic://tenant/ns/a", "topic://tenant/ns/b"));
        assertEquals(decoded.multiTopicVector().get("topic://tenant/ns/a"),
                Map.of(0L, v4(1, 1, 0), 1L, v4(2, 2, 0)));
        assertEquals(decoded.multiTopicVector().get("topic://tenant/ns/b"),
                Map.of(0L, v4(3, 3, 0)));
    }

    @Test
    public void testRoundtripPreservesNullMultiTopicVector() throws Exception {
        // Single-topic ids leave both new fields null; we must not accidentally
        // hydrate them on decode.
        MessageIdV5 original = new MessageIdV5(v4(7, 8, 0), 1L,
                Map.of(0L, v4(1, 2, 0)));

        MessageIdV5 decoded = MessageIdV5.fromByteArray(original.toByteArray());

        assertEquals(decoded.parentTopic(), null);
        assertEquals(decoded.multiTopicVector(), null);
    }

    @Test
    public void testFromByteArrayRejectsTooShort() {
        assertThrows(java.io.IOException.class, () -> MessageIdV5.fromByteArray(new byte[3]));
    }

    @Test
    public void testFromByteArrayRejectsGarbage() {
        // Crafted header that advertises an implausibly large v4 payload length.
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(12);
        buf.putLong(0L);
        buf.putInt(Integer.MAX_VALUE);
        assertThrows(java.io.IOException.class,
                () -> MessageIdV5.fromByteArray(buf.array()));
    }

    @DataProvider
    public Object[][] malformedSections() {
        return new Object[][]{
                {"negative position count", idFollowedBy(-1)},
                {"negative cross-topic count", idFollowedBy(0, -1, -2)},
                {"negative cross-topic name length", idFollowedBy(0, -1, 1, -1)},
                {"cross-topic name longer than the data", idFollowedBy(0, -1, 1, 100)},
                {"negative cross-topic position count", idFollowedBy(0, -1, 1, 0, -1)},
                {"truncated parent-topic length", withoutLastBytes(idFollowedBy(0, -1), 2)}
        };
    }

    @Test(dataProvider = "malformedSections")
    public void testFromByteArrayRejectsMalformedSections(String description, byte[] data) {
        assertThrows(IOException.class, () -> MessageIdV5.fromByteArray(data));
    }

    @Test
    public void testFromByteArrayFailsOnlyWithIOExceptionForAnyPrefix() {
        byte[] bytes = new MessageIdV5(v4(10, 20, 0), 1L, Map.of(0L, v4(1, 2, 0)), "topic://t",
                Map.of("topic://t", Map.of(0L, v4(1, 1, 0)))).toByteArray();
        // A prefix that ends between sections is a valid older encoding; any other one is malformed
        for (int length = 0; length < bytes.length; length++) {
            byte[] prefix = Arrays.copyOf(bytes, length);
            try {
                MessageIdV5.fromByteArray(prefix);
            } catch (IOException expected) {
                // reported as malformed, as the API documents
            }
        }
    }

    private static byte[] withoutLastBytes(byte[] data, int count) {
        return Arrays.copyOf(data, data.length - count);
    }

    /** A serialised id of segment 5 whose sections after the v4 id are the given ints. */
    private static byte[] idFollowedBy(int... ints) {
        byte[] v4Bytes = v4(1, 2, 0).toByteArray();
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + v4Bytes.length + Integer.BYTES * ints.length);
        buf.putLong(5L).putInt(v4Bytes.length).put(v4Bytes);
        for (int value : ints) {
            buf.putInt(value);
        }
        return buf.array();
    }

    // --- sentinels ---

    @Test
    public void testEarliestAndLatestSentinels() {
        assertNotNull(MessageIdV5.EARLIEST);
        assertNotNull(MessageIdV5.LATEST);
        assertEquals(MessageIdV5.EARLIEST.segmentId(), MessageIdV5.NO_SEGMENT);
        assertEquals(MessageIdV5.LATEST.segmentId(), MessageIdV5.NO_SEGMENT);
    }

    // --- toString is non-fragile ---

    @Test
    public void testToStringContainsSegmentAndPositionInfo() {
        MessageIdV5 id = new MessageIdV5(v4(1, 2, 0), 7L, Map.of(0L, v4(1, 1, 0)));
        String s = id.toString();
        assertTrue(s.contains("7"), s);
        assertTrue(s.contains("positions="), s);
    }
}
