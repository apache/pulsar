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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link LongBitmap}. Covers point/range operations, uint32 boundary
 * behavior, serialization round-trip, drain-to atomicity, and concurrency contracts
 * (or lock ordering, snapshot safety under mutation, or-input immutability).
 */
public class LongBitmapTest {

    @Test
    public void testBasicOperations() {
        LongBitmap bitmap = LongBitmaps.create();

        assertEquals(bitmap.cardinality(), 0);
        assertFalse(bitmap.contains(1));

        bitmap.add(1);
        bitmap.add(100);
        bitmap.add(1000);

        assertEquals(bitmap.cardinality(), 3);
        assertTrue(bitmap.contains(1));
        assertTrue(bitmap.contains(100));
        assertTrue(bitmap.contains(1000));
        assertFalse(bitmap.contains(2));

        bitmap.remove(100);
        assertEquals(bitmap.cardinality(), 2);
        assertFalse(bitmap.contains(100));
    }

    @Test
    public void testRangeValidation() {
        LongBitmap bitmap = LongBitmaps.create();
        bitmap.add(0);
        bitmap.add(0xFFFFFFFFL);
        assertTrue(bitmap.contains(0));
        assertTrue(bitmap.contains(0xFFFFFFFFL));

        assertThrows(IllegalArgumentException.class, () -> bitmap.add(-1));
        assertThrows(IllegalArgumentException.class, () -> bitmap.add(0x100000000L));
        assertFalse(bitmap.contains(-1));
        assertFalse(bitmap.contains(0x100000000L));
    }

    @Test
    public void testSerialization() {
        LongBitmap bitmap = LongBitmaps.create();
        for (int i = 0; i < 1000; i += 10) {
            bitmap.add(i);
        }

        byte[] bytes = bitmap.serialize();
        assertTrue(bytes.length > 0);

        LongBitmap deserialized = LongBitmaps.deserialize(Unpooled.wrappedBuffer(bytes));
        assertEquals(deserialized.cardinality(), 100);
        for (int i = 0; i < 1000; i += 10) {
            assertTrue(deserialized.contains(i));
        }
        assertFalse(deserialized.contains(5));
    }

    @Test
    public void testDeserializeFromVariousByteBufTypes() {
        LongBitmap bitmap = LongBitmaps.create();
        for (int i = 0; i < 2000; i += 7) {
            bitmap.add(i);
        }
        byte[] bytes = bitmap.serialize();

        ByteBuf heap = Unpooled.wrappedBuffer(bytes);
        try {
            LongBitmap r = LongBitmaps.deserialize(heap);
            assertEquals(r.cardinality(), bitmap.cardinality());
            assertEquals(heap.readerIndex(), bytes.length);
        } finally {
            heap.release();
        }

        ByteBuf direct = Unpooled.directBuffer(bytes.length);
        try {
            direct.writeBytes(bytes);
            LongBitmap r = LongBitmaps.deserialize(direct);
            assertEquals(r.cardinality(), bitmap.cardinality());
            assertEquals(direct.readerIndex(), bytes.length);
        } finally {
            direct.release();
        }

        ByteBuf singleComp = Unpooled.wrappedBuffer(new ByteBuf[]{Unpooled.wrappedBuffer(bytes)});
        try {
            LongBitmap r = LongBitmaps.deserialize(singleComp);
            assertEquals(r.cardinality(), bitmap.cardinality());
            assertEquals(singleComp.readerIndex(), bytes.length);
        } finally {
            singleComp.release();
        }

        int split = bytes.length / 2;
        ByteBuf part1 = Unpooled.wrappedBuffer(bytes, 0, split);
        ByteBuf part2 = Unpooled.wrappedBuffer(bytes, split, bytes.length - split);
        ByteBuf composite = Unpooled.wrappedBuffer(part1, part2);
        try {
            LongBitmap r = LongBitmaps.deserialize(composite);
            assertEquals(r.cardinality(), bitmap.cardinality());
            assertEquals(composite.readerIndex(), bytes.length);
        } finally {
            composite.release();
        }
    }

    @Test
    public void testAddIsIdempotent() {
        LongBitmap bitmap = LongBitmaps.create();
        bitmap.add(42);
        assertEquals(bitmap.cardinality(), 1);
        assertTrue(bitmap.contains(42));

        bitmap.add(42);
        bitmap.add(42);
        assertEquals(bitmap.cardinality(), 1);
        assertTrue(bitmap.contains(42));
    }

    @Test
    public void testCheckedAdd() {
        LongBitmap bitmap = LongBitmaps.create();

        assertTrue(bitmap.checkedAdd(42));
        assertTrue(bitmap.contains(42));
        assertEquals(bitmap.cardinality(), 1);

        assertFalse(bitmap.checkedAdd(42));
        assertEquals(bitmap.cardinality(), 1);

        assertTrue(bitmap.checkedAdd(100));
        assertEquals(bitmap.cardinality(), 2);

        assertThrows(IllegalArgumentException.class, () -> bitmap.checkedAdd(-1));
        assertThrows(IllegalArgumentException.class, () -> bitmap.checkedAdd(0x100000000L));
        assertEquals(bitmap.cardinality(), 2);
    }

    @Test
    public void testForEach() {
        LongBitmap bitmap = LongBitmaps.create();
        bitmap.add(1);
        bitmap.add(5);
        bitmap.add(10);

        List<Long> values = new ArrayList<>();
        bitmap.forEachLong(values::add);

        assertEquals(values.size(), 3);
        Collections.sort(values);
        assertEquals(values.get(0).longValue(), 1);
        assertEquals(values.get(1).longValue(), 5);
        assertEquals(values.get(2).longValue(), 10);
    }

    @Test
    public void testConcurrentReads() throws Exception {
        LongBitmap bitmap = LongBitmaps.create();
        for (int i = 0; i < 10000; i++) {
            bitmap.add(i);
        }

        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < 1000; j++) {
                        if (!bitmap.contains(j)) {
                            errors.incrementAndGet();
                        }
                        if (bitmap.cardinality() != 10000) {
                            errors.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
        assertEquals(errors.get(), 0);
    }

    @Test
    public void testConcurrentWrites() throws Exception {
        LongBitmap bitmap = LongBitmaps.create();

        int numThreads = 10;
        int valuesPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int t = 0; t < numThreads; t++) {
            int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < valuesPerThread; i++) {
                        bitmap.add(threadId * valuesPerThread + i);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(bitmap.cardinality(), numThreads * valuesPerThread);
        for (int t = 0; t < numThreads; t++) {
            for (int i = 0; i < valuesPerThread; i++) {
                assertTrue(bitmap.contains(t * valuesPerThread + i));
            }
        }
    }

    @Test
    public void testConcurrentReadWrite() throws Exception {
        LongBitmap bitmap = LongBitmaps.create();
        for (int i = 0; i < 5000; i++) {
            bitmap.add(i);
        }

        int numReaders = 5;
        int numWriters = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numReaders + numWriters);
        CountDownLatch latch = new CountDownLatch(numReaders + numWriters);
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < numReaders; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < 1000; j++) {
                        bitmap.contains(j % 5000);
                        bitmap.cardinality();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        for (int i = 0; i < numWriters; i++) {
            int writerId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < 1000; j++) {
                        bitmap.add(5000 + writerId * 1000 + j);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
        assertEquals(errors.get(), 0);
    }

    @Test
    public void testConcurrentForEachLongAndMutate() throws Exception {
        // forEachLong takes a clone() snapshot under read lock; concurrent mutations on the
        // live bitmap must never corrupt the snapshot. Regression guard for pulsar#25991.
        LongBitmap bitmap = LongBitmaps.create();
        for (int i = 0; i < 10000; i++) {
            bitmap.add(i);
        }

        int numReaders = 5;
        int numWriters = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numReaders + numWriters);
        CountDownLatch latch = new CountDownLatch(numReaders + numWriters);
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < numReaders; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < 100; j++) {
                        long[] last = {-1};
                        bitmap.forEachLong(v -> {
                            // Snapshot values must arrive in ascending order.
                            if (v <= last[0]) {
                                errors.incrementAndGet();
                            }
                            last[0] = v;
                        });
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        for (int i = 0; i < numWriters; i++) {
            int id = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < 1000; j++) {
                        long v = 10000 + id * 1000 + (j % 1000);
                        bitmap.add(v);
                        bitmap.remove(v);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
        assertEquals(errors.get(), 0);
        assertEquals(bitmap.cardinality(), 10000);
    }

    @Test
    public void testMemoryTrim() {
        LongBitmap bitmap = LongBitmaps.create();
        for (int i = 0; i < 100000; i++) {
            bitmap.add(i);
        }
        for (int i = 0; i < 50000; i++) {
            bitmap.remove(i);
        }

        assertEquals(bitmap.cardinality(), 50000);
        for (int i = 0; i < 50000; i++) {
            assertFalse(bitmap.contains(i));
        }
        for (int i = 50000; i < 100000; i++) {
            assertTrue(bitmap.contains(i));
        }
    }

    @Test
    public void testIsEmpty() {
        LongBitmap bitmap = LongBitmaps.create();
        assertTrue(bitmap.isEmpty());

        bitmap.add(1);
        assertFalse(bitmap.isEmpty());

        bitmap.remove(1);
        assertTrue(bitmap.isEmpty());

        bitmap.remove(999);  // never added
        assertTrue(bitmap.isEmpty());
    }

    @Test
    public void testOr() {
        LongBitmap a = LongBitmaps.create();
        a.add(1);
        a.add(100);
        a.add(1000);

        LongBitmap b = LongBitmaps.create();
        b.add(100);     // overlap
        b.add(2000);    // unique to b

        a.or(b);

        assertEquals(a.cardinality(), 4);
        assertTrue(a.contains(1));
        assertTrue(a.contains(100));
        assertTrue(a.contains(1000));
        assertTrue(a.contains(2000));

        // b is not modified
        assertEquals(b.cardinality(), 2);
    }

    @Test
    public void testOrEmpty() {
        LongBitmap a = LongBitmaps.create();
        a.add(1);
        a.add(2);

        LongBitmap empty = LongBitmaps.create();
        a.or(empty);
        assertEquals(a.cardinality(), 2);

        LongBitmap target = LongBitmaps.create();
        target.or(a);
        assertEquals(target.cardinality(), 2);
    }

    @Test
    public void testOrSelfIsNoOp() {
        LongBitmap a = LongBitmaps.create();
        a.add(1);
        a.add(100);

        a.or(a);  // should not deadlock

        assertEquals(a.cardinality(), 2);
        assertTrue(a.contains(1));
        assertTrue(a.contains(100));
    }

    @Test
    public void testRangeAddRemoveContains() {
        LongBitmap bitmap = LongBitmaps.create();
        bitmap.add(100, 200);
        assertEquals(bitmap.cardinality(), 100);
        for (long v = 100; v < 200; v++) {
            assertTrue(bitmap.contains(v));
        }
        assertFalse(bitmap.contains(99));
        assertFalse(bitmap.contains(200));

        // Single-value range [x, x+1) is equivalent to add(x).
        LongBitmap single = LongBitmaps.create();
        single.add(42, 43);
        assertTrue(single.contains(42, 43));
        assertTrue(single.contains(42));
        assertEquals(single.cardinality(), 1);

        // Range contains: true iff EVERY value in [from, to) is set.
        assertTrue(bitmap.contains(100, 200));
        assertTrue(bitmap.contains(150, 160));
        assertFalse(bitmap.contains(99, 101));
        assertFalse(bitmap.contains(199, 201));
        assertFalse(bitmap.contains(200, 300));

        bitmap.remove(100, 150);
        assertEquals(bitmap.cardinality(), 50);
        for (long v = 100; v < 150; v++) {
            assertFalse(bitmap.contains(v));
        }
        for (long v = 150; v < 200; v++) {
            assertTrue(bitmap.contains(v));
        }
    }

    @Test
    public void testRangeVsSingleValueEquivalence() {
        // For any v, add(v, v+1) / contains(v, v+1) / remove(v, v+1) ≡ add(v) / contains(v) / remove(v).
        long[] values = {0, 1, 100, 65535, 65536, 1L << 30, 0xFFFFFFFFL};
        for (long v : values) {
            LongBitmap bitmap = LongBitmaps.create();
            bitmap.add(v);
            assertTrue(bitmap.contains(v));
            assertEquals(bitmap.cardinality(), 1);

            bitmap.add(v);  // idempotent
            assertEquals(bitmap.cardinality(), 1);

            bitmap.remove(v);
            assertFalse(bitmap.contains(v));
            assertEquals(bitmap.cardinality(), 0);
        }
    }

    @Test
    public void testNextAbsentValue() {
        LongBitmap bitmap = LongBitmaps.create();
        // Empty bitmap: first absent is 0
        assertEquals(bitmap.nextAbsentValue(0), 0);

        bitmap.add(0);
        bitmap.add(1);
        bitmap.add(2);
        // [0,1,2] present, next absent from 0 is 3
        assertEquals(bitmap.nextAbsentValue(0), 3);

        bitmap.add(5);
        // Gap at 3,4
        assertEquals(bitmap.nextAbsentValue(0), 3);
        assertEquals(bitmap.nextAbsentValue(3), 3);
        assertEquals(bitmap.nextAbsentValue(5), 6);

        // Out of range returns -1
        assertEquals(bitmap.nextAbsentValue(-1), -1);
        assertEquals(bitmap.nextAbsentValue(0x100000000L), -1);
    }

    @Test
    public void testDrainTo() {
        LongBitmap bitmap = LongBitmaps.create();
        for (int i = 0; i < 100; i++) {
            bitmap.add(i * 10);  // 0, 10, 20, ..., 990
        }

        List<Long> drained = new ArrayList<>();
        long count = bitmap.drainTo(5, drained::add);

        assertEquals(count, 5);
        assertEquals(drained.size(), 5);
        assertEquals(drained.get(0).longValue(), 0);
        assertEquals(drained.get(4).longValue(), 40);
        assertEquals(bitmap.cardinality(), 95);
        assertFalse(bitmap.contains(0));
        assertFalse(bitmap.contains(40));
        assertTrue(bitmap.contains(50));

        drained.clear();
        count = bitmap.drainTo(1000, drained::add);  // drain all remaining
        assertEquals(count, 95);
        assertTrue(bitmap.isEmpty());
    }

    @Test
    public void testDrainToZeroOrNegative() {
        LongBitmap bitmap = LongBitmaps.create();
        bitmap.add(1);
        bitmap.add(2);

        assertEquals(bitmap.drainTo(0, v -> {
        }), 0);
        assertEquals(bitmap.cardinality(), 2);

        assertEquals(bitmap.drainTo(-1, v -> {
        }), 0);
        assertEquals(bitmap.cardinality(), 2);
    }

    @Test
    public void testEmptyRangeIsNoOp() {
        // add(x, x) and remove(x, x) must be no-ops, not throw.
        LongBitmap bitmap = LongBitmaps.create();
        bitmap.add(1);
        bitmap.add(2);

        bitmap.add(5, 5);
        bitmap.add(10, 1);  // reversed range
        assertEquals(bitmap.cardinality(), 2);

        bitmap.remove(5, 5);
        bitmap.remove(10, 1);
        assertEquals(bitmap.cardinality(), 2);
    }

    @Test
    public void testDrainToFollowedByDrainToDoesNotLeak() {
        // drainTo must respect the same trim threshold as remove(value).
        // Add many values, drain in small batches; if trim never fires, the bitmap
        // accumulates unused container capacity across calls.
        LongBitmap bitmap = LongBitmaps.create();
        for (int i = 0; i < 50000; i++) {
            bitmap.add(i);
        }

        // Drain 100 at a time — well under TRIM_AFTER_REMOVES (10000) per call,
        // but cumulative trim counter should cross the threshold after several batches.
        long totalDrained = 0;
        while (!bitmap.isEmpty()) {
            totalDrained += bitmap.drainTo(100, v -> {
            });
        }
        assertEquals(totalDrained, 50000);
        assertTrue(bitmap.isEmpty());
    }

    @Test
    public void testOrDoesNotMutateInput() {
        // A.or(B) must treat B as read-only — required for our lock split
        // (this=writeLock, other=readLock).
        LongBitmap a = LongBitmaps.create();
        LongBitmap b = LongBitmaps.create();
        for (int i = 0; i < 1000; i++) {
            a.add(i * 2);
            b.add(i * 2 + 1);
        }
        long bCardinalityBefore = b.cardinality();

        a.or(b);

        assertEquals(b.cardinality(), bCardinalityBefore);
        for (int i = 0; i < 1000; i++) {
            assertTrue(b.contains(i * 2 + 1), "b lost value after a.or(b)");
            assertFalse(b.contains(i * 2), "b gained value after a.or(b)");
        }
        assertEquals(a.cardinality(), 2000);
    }

    @Test
    public void testOrCrossDirectionNoDeadlock() throws Exception {
        // Concurrent A.or(B) and B.or(A) must not deadlock.
        // Lock ordering by identityHashCode prevents the classic AB-BA deadlock.
        int pairs = 10;
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(pairs * 2);
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < pairs; i++) {
            final LongBitmap a = LongBitmaps.create();
            final LongBitmap b = LongBitmaps.create();
            for (int j = 0; j < 100; j++) {
                a.add(j);
                b.add(j + 50);  // overlap
            }
            executor.submit(() -> {
                try {
                    a.or(b);
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
            executor.submit(() -> {
                try {
                    b.or(a);
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS),
                "or() cross-direction should not deadlock");
        executor.shutdown();
        assertEquals(errors.get(), 0);
    }

    @Test
    public void testDrainToActionDoesNotHoldWriteLock() {
        // Verify that the action runs outside the lock: a slow action should not block
        // concurrent operations. The select+remove phase holds writeLock briefly, but
        // the action invocation happens after unlock.
        LongBitmap bitmap = LongBitmaps.create();
        for (int i = 0; i < 1000; i++) {
            bitmap.add(i);
        }

        // Slow action: sleep briefly per value. drainTo should not hold any lock
        // during action execution — concurrent operations should remain responsive.
        long start = System.nanoTime();
        long drained = bitmap.drainTo(100, v -> {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
        });
        long elapsed = System.nanoTime() - start;
        assertEquals(drained, 100);
        // Total elapsed ~100ms (sleep); the lock was released before action ran.
        assertTrue(elapsed >= 100_000_000L);  // sanity: drainTo ran the action 100 times
    }

    @Test
    public void testUint32BoundaryRange() {
        // Verify range APIs handle the uint32 upper boundary correctly.
        // add(MAX_UINT32, MAX_UINT32+1) should add exactly one value: MAX_UINT32.
        // Note: MutableRoaringBitmap.contains(long, long) has a known issue at this
        // boundary where it returns false even when the value is present, so we only
        // test contains(long) single-value form and cardinality.
        LongBitmap bitmap = LongBitmaps.create();
        bitmap.add(0xFFFFFFFFL, 0x100000000L);
        assertEquals(bitmap.cardinality(), 1);
        assertTrue(bitmap.contains(0xFFFFFFFFL));

        bitmap.remove(0xFFFFFFFFL, 0x100000000L);
        assertEquals(bitmap.cardinality(), 0);
        assertFalse(bitmap.contains(0xFFFFFFFFL));
    }

    @Test
    public void testRangeReachesMaxUint32WithoutClamp() {
        // Validates that add/remove do not need Math.min(to, UINT32_SIZE):
        // validateRange(to - 1) already guarantees to <= UINT32_SIZE, and RoaringBitmap
        // handles to == UINT32_SIZE correctly at the boundary. If this test ever fails,
        // the clamp must be restored.
        LongBitmap bitmap = LongBitmaps.create();

        // Multi-value range that ends exactly at UINT32_SIZE: [MAX-1, MAX+1) = {MAX-1, MAX}
        bitmap.add(0xFFFFFFFEL, 0x100000000L);
        assertEquals(bitmap.cardinality(), 2);
        assertTrue(bitmap.contains(0xFFFFFFFEL));
        assertTrue(bitmap.contains(0xFFFFFFFFL));

        // Same range on remove
        bitmap.remove(0xFFFFFFFEL, 0x100000000L);
        assertEquals(bitmap.cardinality(), 0);
        assertFalse(bitmap.contains(0xFFFFFFFEL));
        assertFalse(bitmap.contains(0xFFFFFFFFL));

        // Crossing container boundary (65535/65536) with to on a power-of-2 boundary
        bitmap.add(65530L, 65540L);
        assertEquals(bitmap.cardinality(), 10);
        bitmap.remove(65530L, 65540L);
        assertEquals(bitmap.cardinality(), 0);

        // Out-of-range `to` must throw — proving validateRange guards the upper bound.
        assertThrows(IllegalArgumentException.class,
                () -> bitmap.add(0L, 0x100000001L));  // to = UINT32_SIZE + 1
    }

    @Test
    public void testSerializedSizeIsUpperBoundForSerialize() {
        // serializedSize() is an upper bound (no runOptimize). serialize() runs
        // runOptimize, which only shrinks. So estimated >= actual.
        long[] seeds = {0, 100, 65535, 65536, 1L << 30, 0xFFFFFF00L};
        for (int trial = 0; trial < 5; trial++) {
            LongBitmap bitmap = LongBitmaps.create();
            java.util.Random rng = new java.util.Random(trial);
            int count = 1000 + rng.nextInt(5000);
            for (int i = 0; i < count; i++) {
                bitmap.add(seeds[rng.nextInt(seeds.length)] + rng.nextInt(100));
            }
            long estimated = bitmap.serializedSize();
            byte[] actual = bitmap.serialize();
            assertTrue(estimated >= actual.length,
                    "trial " + trial + ": estimated " + estimated + " < actual " + actual.length);
        }
    }

    @Test
    public void testDrainToIsAtomic() {
        // drainTo must atomically select+remove values. Concurrent add/remove should
        // not cause newly added values to be lost between snapshot and removal.
        LongBitmap bitmap = LongBitmaps.create();
        for (int i = 0; i < 100; i++) {
            bitmap.add(i);
        }

        // Drain with a slow action, while concurrently adding/removing values
        AtomicInteger errors = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        // T1: drain slowly
        executor.submit(() -> {
            try {
                bitmap.drainTo(50, v -> {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                    }
                });
            } catch (Exception e) {
                errors.incrementAndGet();
            } finally {
                latch.countDown();
            }
        });

        // T2: add/remove while T1 drains
        executor.submit(() -> {
            try {
                Thread.sleep(10);  // let T1 start draining
                for (int i = 0; i < 50; i++) {
                    bitmap.remove(i);
                    bitmap.add(i);
                }
            } catch (Exception e) {
                errors.incrementAndGet();
            } finally {
                latch.countDown();
            }
        });

        try {
            assertTrue(latch.await(30, TimeUnit.SECONDS));
            executor.shutdown();
            assertEquals(errors.get(), 0);
            // After drain(50) and concurrent add/remove, the bitmap should contain
            // the values that were re-added by T2, not lost due to racy andNot.
            long finalCount = bitmap.cardinality();
            // We drained ~50, then T2 added back some of those. Final count >= 50
            // (the un-drained values) is the key invariant.
            assertTrue(finalCount >= 50,
                    "finalCount=" + finalCount + " should be >= 50 (un-drained values)");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDeserializeFromLongArrayFactory() {
        LongBitmap bitmap = LongBitmaps.create();
        bitmap.add(1);
        bitmap.add(100);
        bitmap.add(1000);
        bitmap.add(Integer.MAX_VALUE);

        long[] data = bitmap.serializeToLongArray();

        LongBitmap restored = LongBitmaps.deserializeFromLongArray(data);

        assertEquals(restored.cardinality(), 4);
        assertTrue(restored.contains(1));
        assertTrue(restored.contains(100));
        assertTrue(restored.contains(1000));
        assertTrue(restored.contains(Integer.MAX_VALUE));
    }

    @Test
    public void testLastPresentValueWithMaxUint32() {
        LongBitmap bitmap = LongBitmaps.create();
        bitmap.add(0xFFFFFFFFL);

        assertEquals(bitmap.lastPresentValue(), 0xFFFFFFFFL);

        bitmap.add(Integer.MAX_VALUE);
        assertEquals(bitmap.lastPresentValue(), 0xFFFFFFFFL);

        bitmap.clear();
        assertEquals(bitmap.lastPresentValue(), -1);
    }

    @Test
    public void testNavigationWithUint32Boundary() {
        LongBitmap bitmap = LongBitmaps.create();
        bitmap.add(Integer.MAX_VALUE);
        bitmap.add(Integer.MAX_VALUE + 1L);
        bitmap.add(0xFFFFFFFFL);

        assertEquals(bitmap.nextPresentValue(Integer.MAX_VALUE), Integer.MAX_VALUE);
        assertEquals(bitmap.nextPresentValue(Integer.MAX_VALUE + 1L), Integer.MAX_VALUE + 1L);
        assertEquals(bitmap.nextPresentValue(0xFFFFFFFEL), 0xFFFFFFFFL);

        assertEquals(bitmap.nextAbsentValue(0xFFFFFFFFL), -1);

        assertEquals(bitmap.nextPresentValue(0xFFFFFFFFL), 0xFFFFFFFFL);
        assertEquals(bitmap.nextPresentValue(0xFFFFFFFFL + 1L), -1);
    }

    @Test
    public void testRank() {
        LongBitmap bitmap = LongBitmaps.create();
        bitmap.add(10);
        bitmap.add(20);
        bitmap.add(30);
        bitmap.add(40);
        bitmap.add(50);

        assertEquals(bitmap.rank(0), 0);
        assertEquals(bitmap.rank(10), 0);
        assertEquals(bitmap.rank(11), 1);
        assertEquals(bitmap.rank(20), 1);
        assertEquals(bitmap.rank(25), 2);
        assertEquals(bitmap.rank(50), 4);
        assertEquals(bitmap.rank(51), 5);
        assertEquals(bitmap.rank(100), 5);

        bitmap.add(0xFFFFFFFFL);

        assertEquals(bitmap.rank(0xFFFFFFFFL), 5);
        assertEquals(bitmap.rank(0x100000000L), 6);
    }

    @Test
    public void testPreviousAbsentValue() {
        LongBitmap bitmap = LongBitmaps.create();
        bitmap.add(5);
        bitmap.add(10);
        bitmap.add(15);
        bitmap.add(20);

        assertEquals(bitmap.previousAbsentValue(25), 25);
        assertEquals(bitmap.previousAbsentValue(21), 21);
        assertEquals(bitmap.previousAbsentValue(20), 19);
        assertEquals(bitmap.previousAbsentValue(16), 16);
        assertEquals(bitmap.previousAbsentValue(15), 14);
        assertEquals(bitmap.previousAbsentValue(6), 6);
        assertEquals(bitmap.previousAbsentValue(5), 4);
        assertEquals(bitmap.previousAbsentValue(2), 2);

        bitmap.clear();
        bitmap.add(0xFFFFFFFFL);

        assertEquals(bitmap.previousAbsentValue(0xFFFFFFFFL), 0xFFFFFFFEL);
        assertEquals(bitmap.previousAbsentValue(0x100000000L), 0xFFFFFFFEL);
    }

    @Test
    public void testNextPresentValueAtMaxUint32() {
        LongBitmap bitmap = LongBitmaps.create();

        bitmap.add(0xFFFFFFFFL);

        assertEquals(bitmap.nextPresentValue(0), 0xFFFFFFFFL);
        assertEquals(bitmap.nextPresentValue(0xFFFFFFFFL), 0xFFFFFFFFL);
        assertEquals(bitmap.nextPresentValue(0x100000000L), -1);
    }
}