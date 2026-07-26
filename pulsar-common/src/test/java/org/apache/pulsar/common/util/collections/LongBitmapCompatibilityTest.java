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
package org.apache.pulsar.common.util.collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.testng.annotations.Test;

/**
 * Verifies the serialization compatibility characteristics of {@link LongBitmap}.
 *
 * <p>Context: the previous implementation used two different RoaringBitmap variants:
 * <ul>
 *   <li>{@code InMemoryDelayedDeliveryTracker} used {@link Roaring64Bitmap} (in-memory only)
 *   <li>{@code BucketDelayedDeliveryTracker} used {@link RoaringBitmap} (32-bit, persisted)
 * </ul>
 *
 * <p>The new {@link LongBitmap} abstraction always uses the 32-bit {@link RoaringBitmap}
 * internally. These tests document and verify:
 * <ol>
 *   <li>32-bit buffer compatibility: {@link LongBitmap} buffers are byte-identical to standard
 *       {@link RoaringBitmap} buffers (required for {@code BucketDelayedDeliveryTracker}
 *       backward compatibility — old persisted snapshots must round-trip).
 *   <li>64-bit buffer incompatibility: {@link Roaring64Bitmap} buffers cannot be deserialized
 *       by {@link LongBitmap} (format includes high-32-bit bucket prefixes). Migration from
 *       {@code Roaring64Bitmap} is safe only for in-memory state; persisted state would require
 *       a one-time entry-by-entry rebuild.
 *   <li>Behavioral equivalence within {@code uint32} range: {@link LongBitmap} and
 *       {@link Roaring64Bitmap} produce identical add/remove/contains results for all
 *       values in {@code [0, 2^32)}.
 * </ol>
 */
public class LongBitmapCompatibilityTest {

    /**
     * LongBitmap's serialized bytes must be identical to a 32-bit RoaringBitmap's
     * for the same uint32 values. This guarantees persisted snapshots written by
     * the old {@code BucketDelayedDeliveryTracker} (using RoaringBitmap) remain
     * readable, and vice versa.
     */
    @Test
    public void testLongBitmapBufferEqualsStandardRoaringBitmap() throws Exception {
        LongBitmap longBitmap = LongBitmaps.create();
        longBitmap.add(0);
        longBitmap.add(100);
        longBitmap.add(1L << 20);
        longBitmap.add(0xFFFFFFFFL); // uint32 max

        RoaringBitmap roaring = new RoaringBitmap();
        roaring.add(0);
        roaring.add(100);
        roaring.add(1 << 20);
        roaring.add(0xFFFFFFFF); // -1 as signed int

        byte[] longBitmapBytes = serializeLongBitmap(longBitmap);
        byte[] roaringBytes = serializeRoaring32(roaring);

        assertEquals(longBitmapBytes, roaringBytes,
                "LongBitmap buffer must be byte-identical to standard 32-bit RoaringBitmap buffer");
    }

    /**
     * Round-trip: 32-bit RoaringBitmap buffer -> LongBitmap. Confirms that
     * persisted data written by the old code path can be read by LongBitmap.
     */
    @Test
    public void testDeserializeFromRoaring32Buffer() throws Exception {
        RoaringBitmap roaring = new RoaringBitmap();
        for (int i = 0; i < 1000; i += 7) {
            roaring.add(i);
        }
        roaring.add(0xFFFFFFFF);

        byte[] roaringBytes = serializeRoaring32(roaring);
        ByteBuf buf = Unpooled.wrappedBuffer(roaringBytes);
        try {
            LongBitmap longBitmap = LongBitmaps.deserialize(buf);
            assertEquals(longBitmap.cardinality(), roaring.getLongCardinality());
            assertTrue(longBitmap.contains(0xFFFFFFFFL));
            for (int i = 0; i < 1000; i += 7) {
                assertTrue(longBitmap.contains(i), "missing " + i);
            }
            for (int i = 1; i < 1000; i += 7) {
                assertFalse(longBitmap.contains(i));
            }
        } finally {
            buf.release();
        }
    }

    /**
     * LongBitmap buffer cannot be deserialized as a Roaring64Bitmap.
     * The 32-bit RoaringBitmap portable format (cookie 12346, ~22 bytes for small sets)
     * is shorter than Roaring64Bitmap's bucketed header, so {@code Roaring64Bitmap.deserialize}
     * throws {@link java.nio.BufferUnderflowException}.
     *
     * <p>Migration implication: persisted 32-bit data cannot be read by old code paths
     * that still expect Roaring64Bitmap, and vice versa.
     */
    @Test
    public void testLongBitmapBufferNotReadableAsRoaring64() throws Exception {
        LongBitmap longBitmap = LongBitmaps.create();
        longBitmap.add(1);
        longBitmap.add(100);
        longBitmap.add(1000);

        byte[] longBitmapBytes = serializeLongBitmap(longBitmap);

        Roaring64Bitmap roaring64 = new Roaring64Bitmap();
        // 32-bit format is shorter than 64-bit header expects — buffer underflow or related I/O error.
        assertThrows(Exception.class,
                () -> roaring64.deserialize(ByteBuffer.wrap(longBitmapBytes)));
    }

    /**
     * Roaring64Bitmap buffer cannot be deserialized as a LongBitmap.
     * Roaring64Bitmap's serialized format starts with a bucket count, not the
     * 32-bit cookie (12346), so {@code MutableRoaringBitmap.deserialize} throws.
     */
    @Test
    public void testRoaring64BufferNotReadableAsLongBitmap() throws Exception {
        Roaring64Bitmap roaring64 = new Roaring64Bitmap();
        roaring64.addLong(1);
        roaring64.addLong(100);
        roaring64.addLong(1000);

        byte[] roaring64Bytes = serializeRoaring64(roaring64);

        ByteBuf buf = Unpooled.wrappedBuffer(roaring64Bytes);
        try {
            // MutableRoaringBitmap wraps IOException as RuntimeException.
            assertThrows(Exception.class, () -> LongBitmaps.deserialize(buf));
        } finally {
            buf.release();
        }
    }

    /**
     * Behavioral equivalence within uint32 range: LongBitmap and Roaring64Bitmap
     * produce identical results for all operations on values in [0, 2^32).
     * This is what makes the InMemoryDelayedDeliveryTracker migration safe —
     * BookKeeper entry IDs are currently always < 2^32.
     */
    @Test
    public void testBehavioralEquivalenceWithinUint32() {
        LongBitmap longBitmap = LongBitmaps.create();
        Roaring64Bitmap roaring64 = new Roaring64Bitmap();

        long[] values = {0, 1, 100, 65535, 65536, 1L << 20, 1L << 30, 0xFFFFFFFFL};
        for (long v : values) {
            longBitmap.add(v);
            roaring64.addLong(v);
        }

        assertEquals(longBitmap.cardinality(), roaring64.getLongCardinality());
        for (long v : values) {
            assertTrue(longBitmap.contains(v));
            assertTrue(roaring64.contains(v));
        }

        long[] toRemove = {0, 100, 65536, 1L << 30};
        for (long v : toRemove) {
            longBitmap.remove(v);
            roaring64.removeLong(v);
        }

        assertEquals(longBitmap.cardinality(), roaring64.getLongCardinality());
        for (long v : toRemove) {
            assertFalse(longBitmap.contains(v));
            assertFalse(roaring64.contains(v));
        }
    }

    /**
     * LongBitmap accepts the uint32 boundary (2^32 - 1) but rejects 2^32 and above.
     * Migration from Roaring64Bitmap must ensure no values >= 2^32 are present;
     * otherwise the migration would silently drop or reject those entries.
     */
    @Test
    public void testUint32Boundary() {
        LongBitmap longBitmap = LongBitmaps.create();
        longBitmap.add(0xFFFFFFFFL);
        assertTrue(longBitmap.contains(0xFFFFFFFFL));
        assertEquals(longBitmap.cardinality(), 1);

        assertThrows(IllegalArgumentException.class, () -> longBitmap.add(0x100000000L));
        assertThrows(IllegalArgumentException.class, () -> longBitmap.add(-1));
    }

    private static byte[] serializeLongBitmap(LongBitmap bitmap) {
        return bitmap.serialize();
    }

    private static byte[] serializeRoaring32(RoaringBitmap bitmap) throws Exception {
        bitmap.runOptimize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        bitmap.serialize(dos);
        dos.close();
        return baos.toByteArray();
    }

    private static byte[] serializeRoaring64(Roaring64Bitmap bitmap) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        bitmap.serialize(dos);
        dos.close();
        return baos.toByteArray();
    }
}
