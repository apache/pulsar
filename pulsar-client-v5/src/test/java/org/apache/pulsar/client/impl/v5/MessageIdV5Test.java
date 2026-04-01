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
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
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
