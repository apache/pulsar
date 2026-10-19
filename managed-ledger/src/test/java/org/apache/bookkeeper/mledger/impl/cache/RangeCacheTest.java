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
package org.apache.bookkeeper.mledger.impl.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReferenceCountedEntry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.util.Reflections;
import org.assertj.core.groups.Tuple;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class RangeCacheTest {

    @Test
    public void simple() {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);

        putToCache(cache, 0, "0");
        putToCache(cache, 1, "1");

        assertEquals(cache.getSize(), 2);
        assertEquals(cache.getNumberOfEntries(), 2);

        ReferenceCountedEntry s = cache.get(createPosition(0));
        assertEquals(s.getData(), "0".getBytes());
        assertEquals(s.refCnt(), 2);
        s.release();

        ReferenceCountedEntry s1 = cache.get(createPosition(0));
        ReferenceCountedEntry s2 = cache.get(createPosition(0));
        assertEquals(s1, s2);
        assertEquals(s1.refCnt(), 3);
        s1.release();
        s2.release();

        assertNull(cache.get(createPosition(2)));

        putToCache(cache, 2, "2");
        putToCache(cache, 8, "8");
        putToCache(cache, 11, "11");

        assertEquals(cache.getSize(), 6);
        assertEquals(cache.getNumberOfEntries(), 5);

        cache.removeRange(createPosition(1), createPosition(5),  true);
        assertEquals(cache.getSize(), 4);
        assertEquals(cache.getNumberOfEntries(), 3);

        cache.removeRange(createPosition(2), createPosition(8),  false);
        assertEquals(cache.getSize(), 4);
        assertEquals(cache.getNumberOfEntries(), 3);

        cache.removeRange(createPosition(0), createPosition(100),  false);
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);

        cache.removeRange(createPosition(0), createPosition(100),  false);
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    private static RangeCacheRemovalQueue createRemovalQueue() {
        return new RangeCacheRemovalQueue(5, false);
    }

    private void putToCache(RangeCache cache, int i, String str) {
        Position position = createPosition(i);
        ReferenceCountedEntry cachedEntry = createCachedEntry(position, str);
        cache.put(position, cachedEntry);
    }

    private static ReferenceCountedEntry createCachedEntry(int i, String str) {
        return createCachedEntry(createPosition(i), str);
    }

    private static ReferenceCountedEntry createCachedEntry(Position position, String str) {
        return EntryImpl.create(position, Unpooled.wrappedBuffer(str.getBytes()), 0);
    }

    private static Position createPosition(int i) {
        return PositionFactory.create(0, i);
    }

    @DataProvider
    public static Object[][] retainBeforeEviction() {
        return new Object[][]{ { true }, { false } };
    }


    @Test(dataProvider = "retainBeforeEviction")
    public void customTimeExtraction(boolean retain) {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);

        putToCache(cache, 1, "1");
        putToCache(cache, 22, "22");
        putToCache(cache, 333, "333");
        long timestamp = System.nanoTime();
        putToCache(cache, 4444, "4444");

        assertEquals(cache.getSize(), 10);
        assertEquals(cache.getNumberOfEntries(), 4);
        final var retainedEntries = cache.getRange(createPosition(1), createPosition(4444));
        for (final var entry : retainedEntries) {
            assertEquals(entry.refCnt(), 2);
            if (!retain) {
                entry.release();
            }
        }

        Pair<Integer, Long> evictedSize = removalQueue.evictLEntriesBeforeTimestamp(timestamp);
        assertEquals(evictedSize.getRight().longValue(), 6);
        assertEquals(evictedSize.getLeft().longValue(), 3);
        assertEquals(cache.getSize(), 4);
        assertEquals(cache.getNumberOfEntries(), 1);

        if (retain) {
            final var valueToRefCnt =
                    retainedEntries.stream().collect(Collectors.toMap(cachedEntry -> new String(cachedEntry.getData()),
                            cachedEntry -> cachedEntry.refCnt()));
            assertEquals(valueToRefCnt, Map.of("1", 1, "22", 1, "333", 1, "4444", 2));
            retainedEntries.forEach(Entry::release);
        } else {
            final var valueToRefCnt = retainedEntries.stream().filter(v -> v.refCnt() > 0).collect(Collectors.toMap(
                    cachedEntry -> new String(cachedEntry.getData()), ReferenceCountedEntry::refCnt));
            assertEquals(valueToRefCnt, Map.of("4444", 1));
        }
    }

    @Test
    public void doubleInsert() {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);

        ReferenceCountedEntry s0 = createCachedEntry(0, "zero");
        assertEquals(s0.refCnt(), 1);
        assertTrue(cache.put(s0.getPosition(), s0));
        assertEquals(s0.refCnt(), 1);

        ReferenceCountedEntry one = createCachedEntry(1, "one");
        assertTrue(cache.put(one.getPosition(), one));
        assertEquals(createPosition(1), one.getPosition());

        assertEquals(cache.getSize(), 7);
        assertEquals(cache.getNumberOfEntries(), 2);
        ReferenceCountedEntry s = cache.get(createPosition(1));
        assertEquals(s.getData(), "one".getBytes());
        assertEquals(s.refCnt(), 2);

        ReferenceCountedEntry s1 = createCachedEntry(1, "uno");
        assertEquals(s1.refCnt(), 1);
        assertFalse(cache.put(s1.getPosition(), s1));
        assertEquals(s1.refCnt(), 1);
        s1.release();

        // Should not have been overridden in cache
        assertEquals(cache.getSize(), 7);
        assertEquals(cache.getNumberOfEntries(), 2);
        assertEquals(cache.get(createPosition(1)).getData(), "one".getBytes());
    }

    @Test
    public void getRange() {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);

        putToCache(cache, 0, "0");
        putToCache(cache, 1, "1");
        putToCache(cache, 3, "3");
        putToCache(cache, 5, "5");

        assertThat(cache.getRange(createPosition(1), createPosition(8)))
                .map(entry -> Tuple.tuple(entry.getPosition(), new String(entry.getData())))
                .containsExactly(
                        Tuple.tuple(createPosition(1), "1"),
                        Tuple.tuple(createPosition(3), "3"),
                        Tuple.tuple(createPosition(5), "5")
                );

        putToCache(cache, 8, "8");

        assertThat(cache.getRange(createPosition(1), createPosition(8)))
                .map(entry -> Tuple.tuple(entry.getPosition(), new String(entry.getData())))
                .containsExactly(
                        Tuple.tuple(createPosition(1), "1"),
                        Tuple.tuple(createPosition(3), "3"),
                        Tuple.tuple(createPosition(5), "5"),
                        Tuple.tuple(createPosition(8), "8")
                );

        cache.clear();
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    @Test
    public void visitRangeReleasesReferencesAndPreservesOrder() {
        RangeCache cache = new RangeCache(createRemovalQueue());
        ReferenceCountedEntry first = createCachedEntry(1, "one");
        ReferenceCountedEntry last = createCachedEntry(3, "three");
        assertTrue(cache.put(first.getPosition(), first));
        assertTrue(cache.put(last.getPosition(), last));
        try {
            List<Position> visited = new ArrayList<>();
            cache.forEachInRange(createPosition(1), createPosition(3), entry -> {
                assertEquals(entry.refCnt(), 2);
                visited.add(entry.getPosition());
            });
            assertThat(visited).containsExactly(createPosition(1), createPosition(3));
            assertEquals(first.refCnt(), 1);
            assertEquals(last.refCnt(), 1);
            cache.forEachInRange(createPosition(4), createPosition(5), entry -> fail("Unexpected cache hit"));

            RuntimeException failure = new RuntimeException("visitor failed");
            assertThatThrownBy(() -> cache.forEachInRange(createPosition(1), createPosition(3), entry -> {
                throw failure;
            })).isSameAs(failure);
            assertEquals(first.refCnt(), 1);
            assertEquals(last.refCnt(), 1);
        } finally {
            cache.clear();
        }
        assertEquals(first.refCnt(), 0);
        assertEquals(last.refCnt(), 0);
    }

    @Test
    public void visitRangeKeepsEntryAliveDuringEviction() {
        RangeCache cache = new RangeCache(createRemovalQueue());
        ReferenceCountedEntry entry = createCachedEntry(1, "one");
        assertTrue(cache.put(entry.getPosition(), entry));
        try {
            cache.forEachInRange(createPosition(1), createPosition(1), value -> {
                cache.clear();
                assertEquals(value.refCnt(), 1);
                assertEquals(new String(value.getData()), "one");
            });
            assertEquals(entry.refCnt(), 0);
        } finally {
            cache.clear();
        }
    }

    @Test
    public void eviction() {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);

        putToCache(cache, 0, "zero");
        putToCache(cache, 1, "one");
        putToCache(cache, 2, "two");
        putToCache(cache, 3, "three");

        // This should remove the LRU entries: 0, 1 whose combined size is 7
        assertEquals(removalQueue.evictLeastAccessedEntries(5), Pair.of(2, (long) 7));

        assertEquals(cache.getNumberOfEntries(), 2);
        assertEquals(cache.getSize(), 8);
        assertNull(cache.get(createPosition(0)));
        assertNull(cache.get(createPosition(1)));
        assertEquals(cache.get(createPosition(2)).getData(), "two".getBytes());
        assertEquals(cache.get(createPosition(3)).getData(), "three".getBytes());

        assertEquals(removalQueue.evictLeastAccessedEntries(100), Pair.of(2, (long) 8));
        assertEquals(cache.getNumberOfEntries(), 0);
        assertEquals(cache.getSize(), 0);
        assertNull(cache.get(createPosition(0)));
        assertNull(cache.get(createPosition(1)));
        assertNull(cache.get(createPosition(2)));
        assertNull(cache.get(createPosition(3)));

        try {
            removalQueue.evictLeastAccessedEntries(0);
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            removalQueue.evictLeastAccessedEntries(-1);
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void evictions() {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);

        int expectedSize = 0;
        for (int i = 0; i < 100; i++) {
            String string = Integer.toString(i);
            expectedSize += string.length();
            putToCache(cache, i, string);
        }

        assertEquals(cache.getSize(), expectedSize);
        Pair<Integer, Long> res = removalQueue.evictLeastAccessedEntries(1);
        assertEquals((int) res.getLeft(), 1);
        assertEquals((long) res.getRight(), 1);
        expectedSize -= 1;
        assertEquals(cache.getSize(), expectedSize);

        res = removalQueue.evictLeastAccessedEntries(10);
        assertEquals((int) res.getLeft(), 10);
        assertEquals((long) res.getRight(), 11);
        expectedSize -= 11;
        assertEquals(cache.getSize(), expectedSize);

        res = removalQueue.evictLeastAccessedEntries(expectedSize);
        assertEquals((int) res.getLeft(), 89);
        assertEquals((long) res.getRight(), expectedSize);
        assertEquals(cache.getSize(), 0);

        expectedSize = 0;
        for (int i = 0; i < 100; i++) {
            String string = Integer.toString(i);
            expectedSize += string.length();
            putToCache(cache, i, string);
        }

        assertEquals(cache.getSize(), expectedSize);

        res = cache.removeRange(createPosition(10), createPosition(20),  false);
        assertEquals((int) res.getLeft(), 10);
        assertEquals((long) res.getRight(), 20);
        expectedSize -= 20;
        assertEquals(cache.getSize(), expectedSize);
    }

    @Test
    public void testPutWhileClearIsCalledConcurrently() {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);
        int numberOfThreads = 8;
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            executor.scheduleWithFixedDelay(cache::clear, 0, 1, TimeUnit.MILLISECONDS);
        }
        for (int i = 0; i < 200000; i++) {
            putToCache(cache, i, Integer.toString(i));
        }
        executor.shutdown();
        // ensure that no clear operation got into endless loop
        Awaitility.await().untilAsserted(() -> assertTrue(executor.isTerminated()));
        // ensure that clear can be called and all entries are removed
        cache.clear();
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    @Test
    public void testPutSameObj() {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);
        ReferenceCountedEntry s0 = createCachedEntry(0, "zero");
        assertEquals(s0.refCnt(), 1);
        assertTrue(cache.put(s0.getPosition(), s0));
        assertFalse(cache.put(s0.getPosition(), s0));
    }

    @Test
    public void testRemoveEntryWithInvalidRefCount() {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);
        ReferenceCountedEntry value = createCachedEntry(1, "1");
        cache.put(value.getPosition(), value);
        // release the value to make the reference count invalid
        value.release();
        cache.clear();
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    @Test
    public void testInvalidMatchingKey() {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);
        ReferenceCountedEntry value = createCachedEntry(1, "1");
        cache.put(value.getPosition(), value);
        assertNotNull(cache.get(value.getPosition()));
        // change the entryId to make the entry invalid for the cache
        Reflections.getAllFields(value.getClass()).stream()
                .filter(field -> field.getName().equals("entryId"))
                .forEach(field -> {
                    field.setAccessible(true);
                    try {
                        field.set(value, 123);
                    } catch (IllegalAccessException e) {
                        fail("Failed to set matching key");
                    }
                });
        assertNull(cache.get(value.getPosition()));
        cache.clear();
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    @Test
    public void testGetKeyWithDifferentInstance() {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);
        Position key = createPosition(129);
        ReferenceCountedEntry value = createCachedEntry(key, "129");
        cache.put(key, value);
        // create a different instance of the key
        Position key2 = createPosition(129);
        // key and key2 are different instances but they are equal
        assertNotSame(key, key2);
        assertEquals(key, key2);
        // get the value using key2
        ReferenceCountedEntry value2 = cache.get(key2);
        // the value should be found
        assertEquals(value2.getData(), "129".getBytes());
    }

    @Test
    public void rangesSpanPagesAndLedgers() {
        RangeCache cache = new RangeCache(createRemovalQueue());
        int pageSize = RangeCache.PAGE_SIZE;
        // the last entries of a page, the first of the next, and the first entries of the next ledger
        long[][] positions = {{1, pageSize - 2}, {1, pageSize - 1}, {1, pageSize}, {1, 3L * pageSize}, {2, 0}, {2, 1}};
        for (long[] position : positions) {
            putToCache(cache, position[0], position[1]);
        }
        assertEquals(cache.getNumberOfEntries(), positions.length);
        // ledger 1's pages 0, 1 and 3, and ledger 2's page 0
        assertEquals(cache.getNumberOfPages(), 4);

        assertRange(cache, PositionFactory.create(1, pageSize - 1), PositionFactory.create(2, 0),
                PositionFactory.create(1, pageSize - 1), PositionFactory.create(1, pageSize),
                PositionFactory.create(1, 3L * pageSize), PositionFactory.create(2, 0));
        assertTrue(cache.exists(PositionFactory.create(1, pageSize)));
        assertFalse(cache.exists(PositionFactory.create(1, pageSize + 1)));

        // the end of a removed range is exclusive also at a page boundary
        assertEquals(cache.removeRange(PositionFactory.create(1, 0), PositionFactory.create(1, pageSize), false),
                Pair.of(2, (long) (Long.toString(pageSize - 2).length() + Long.toString(pageSize - 1).length())));
        assertNull(cache.get(PositionFactory.create(1, pageSize - 1)));
        assertEquals(cache.getNumberOfPages(), 3);

        // a range from before the first ledger, as invalidating the entries of the consumed ledgers does
        assertEquals(cache.removeRange(PositionFactory.create(-1, 0), PositionFactory.create(2, 1), false).getLeft(),
                3);
        assertRange(cache, PositionFactory.create(0, 0), PositionFactory.create(3, 0), PositionFactory.create(2, 1));
        assertEquals(cache.getNumberOfPages(), 1);
        cache.clear();
        assertEquals(cache.getNumberOfPages(), 0);
        assertEquals(cache.getSize(), 0);
    }

    @Test
    public void removesPagesWhenTheirEntriesAreEvicted() {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);
        List<ReferenceCountedEntry> values = new ArrayList<>();
        for (int i = 0; i < 3 * RangeCache.PAGE_SIZE; i++) {
            ReferenceCountedEntry value = createCachedEntry(i, "x");
            value.retain();
            values.add(value);
            assertTrue(cache.put(value.getPosition(), value));
        }
        assertEquals(cache.getNumberOfPages(), 3);
        assertEquals(removalQueue.evictLeastAccessedEntries(RangeCache.PAGE_SIZE + 1),
                Pair.of(RangeCache.PAGE_SIZE + 1, RangeCache.PAGE_SIZE + 1L));
        assertEquals(cache.getNumberOfPages(), 2);
        assertEquals(removalQueue.evictLeastAccessedEntries(Long.MAX_VALUE).getLeft(), 2 * RangeCache.PAGE_SIZE - 1);
        assertEquals(cache.getNumberOfPages(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);
        // the cache released its references, and an entry can be added to a removed page's range again
        assertThat(values).allSatisfy(value -> assertEquals(value.refCnt(), 1));
        putToCache(cache, 1, "1");
        assertEquals(new String(releaseRetained(cache, createPosition(1)).getData()), "1");
        cache.clear();
        values.forEach(ReferenceCountedEntry::release);
    }

    @Test
    public void inserterUpdatesTheCachedEntryAtAPosition() {
        RangeCache cache = new RangeCache(createRemovalQueue());
        RangeCache.Inserter inserter = cache.newInserter();
        assertFalse(inserter.updateIfCached(createPosition(1), entry -> fail("Not cached")));
        putToCache(cache, 1, "1");
        // the inserter looks the page up, and then uses it; the update gets the cached entry, retained meanwhile
        List<String> updated = new ArrayList<>();
        assertTrue(inserter.updateIfCached(createPosition(1), entry -> {
            assertEquals(entry.refCnt(), 2);
            updated.add(new String(entry.getData()));
        }));
        assertThat(updated).containsExactly("1");
        assertFalse(inserter.updateIfCached(createPosition(2), entry -> fail("Not cached")));
        assertFalse(inserter.updateIfCached(PositionFactory.create(1, 1), entry -> fail("Not cached")));
        cache.removeRange(createPosition(1), createPosition(1), true);
        assertFalse(inserter.updateIfCached(createPosition(1), entry -> fail("Not cached")));
        cache.clear();
    }

    @Test
    public void inserterAddsToANewPageAfterItsPageWasRemoved() {
        RangeCache cache = new RangeCache(createRemovalQueue());
        RangeCache.Inserter inserter = cache.newInserter();
        ReferenceCountedEntry first = createCachedEntry(0, "0");
        assertTrue(inserter.put(first.getPosition(), first, first.getLength()));
        // the page loses its only entry and is removed, while the inserter still remembers it
        assertEquals(cache.removeRange(createPosition(0), createPosition(0), true).getLeft(), 1);
        assertEquals(cache.getNumberOfPages(), 0);
        ReferenceCountedEntry second = createCachedEntry(1, "1");
        assertTrue(inserter.put(second.getPosition(), second, second.getLength()));
        assertEquals(cache.getNumberOfPages(), 1);
        assertEquals(new String(releaseRetained(cache, createPosition(1)).getData()), "1");
        // an entry before the inserter's page, as a read from storage inserts, and one in a later page
        ReferenceCountedEntry earlier = createCachedEntry(PositionFactory.create(-1, 5), "e");
        assertTrue(inserter.put(earlier.getPosition(), earlier, earlier.getLength()));
        ReferenceCountedEntry later = createCachedEntry(2 * RangeCache.PAGE_SIZE, "l");
        assertTrue(inserter.put(later.getPosition(), later, later.getLength()));
        assertRange(cache, PositionFactory.create(-1, 0), createPosition(1000), earlier.getPosition(),
                createPosition(1), later.getPosition());
        cache.clear();
        assertEquals(cache.getNumberOfPages(), 0);
    }

    @Test
    public void concurrentInsertsReadsAndRemovals() throws Exception {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);
        int numberOfEntries = 200_000;
        int numberOfReaders = 4;
        List<ReferenceCountedEntry> values = new ArrayList<>(numberOfEntries);
        for (int i = 0; i < numberOfEntries; i++) {
            ReferenceCountedEntry value = createCachedEntry(PositionFactory.create(i / 10_000, i % 10_000), "x");
            // the test's own reference, to check that the cache releases each of its references exactly once
            value.retain();
            values.add(value);
        }
        AtomicBoolean done = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(numberOfReaders + 2);
        List<Future<?>> futures = new ArrayList<>();
        for (int r = 0; r < numberOfReaders; r++) {
            int reader = r;
            futures.add(executor.submit(() -> {
                Random random = new Random(reader);
                while (!done.get()) {
                    int first = random.nextInt(numberOfEntries);
                    Position firstPosition = values.get(first).getPosition();
                    Position lastPosition = values.get(Math.min(first + 150, numberOfEntries - 1)).getPosition();
                    cache.forEachInRange(firstPosition, lastPosition, value -> {
                        if (value.refCnt() < 2 || value.getPosition().compareTo(firstPosition) < 0
                                || value.getPosition().compareTo(lastPosition) > 0) {
                            failure.compareAndSet(null, new AssertionError("Unexpected entry " + value));
                        }
                    });
                    ReferenceCountedEntry value = cache.get(firstPosition);
                    if (value != null) {
                        if (!value.matchesPosition(firstPosition)) {
                            failure.compareAndSet(null, new AssertionError("Unexpected entry " + value));
                        }
                        value.release();
                    }
                }
            }));
        }
        // the cursors' progress invalidates ranges while the eviction removes the oldest entries
        futures.add(executor.submit(() -> {
            Random random = new Random(-1);
            while (!done.get()) {
                Position last = values.get(random.nextInt(numberOfEntries)).getPosition();
                cache.removeRange(PositionFactory.create(last.getLedgerId(), Math.max(0, last.getEntryId() - 200)),
                        last, random.nextBoolean());
            }
        }));
        futures.add(executor.submit(() -> {
            while (!done.get()) {
                removalQueue.evictLeastAccessedEntries(1_000);
            }
        }));
        try {
            // the managed ledger adds its entries with one inserter, whose remembered page races with the removals
            RangeCache.Inserter inserter = cache.newInserter();
            for (ReferenceCountedEntry value : values) {
                inserter.put(value.getPosition(), value, value.getLength());
            }
        } finally {
            done.set(true);
        }
        for (Future<?> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }
        assertNull(failure.get());

        cache.clear();
        removalQueue.evictLeastAccessedEntries(Long.MAX_VALUE);
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);
        assertEquals(cache.getNumberOfPages(), 0);
        assertThat(values).allSatisfy(value -> assertEquals(value.refCnt(), 1));
        values.forEach(ReferenceCountedEntry::release);
    }

    @Test
    public void concurrentTailAndCatchUpInserts() throws Exception {
        RangeCacheRemovalQueue removalQueue = createRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);
        long catchUpLedger = 1;
        long tailLedger = 2;
        int catchUpLedgerEntries = 5_000;
        int tailEntries = 100_000;
        // every entry offered to the cache, with the test's own reference, to check that the cache releases each of
        // its references exactly once and doesn't keep an entry that it didn't insert
        Queue<ReferenceCountedEntry> offered = new ConcurrentLinkedQueue<>();
        AtomicBoolean done = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<?>> futures = new ArrayList<>();
        // catch-up reads insert runs of consecutive entries at random positions of the earlier ledger
        for (int t = 0; t < 2; t++) {
            int seed = t;
            futures.add(executor.submit(() -> {
                Random random = new Random(seed);
                while (!done.get()) {
                    int first = random.nextInt(catchUpLedgerEntries - 100);
                    int count = 1 + random.nextInt(100);
                    // a read from storage inserts its consecutive entries with an inserter of its own
                    RangeCache.Inserter inserter = cache.newInserter();
                    for (int i = first; i < first + count; i++) {
                        offer(inserter, offered, PositionFactory.create(catchUpLedger, i));
                    }
                    Position firstPosition = PositionFactory.create(catchUpLedger, first);
                    Position lastPosition = PositionFactory.create(catchUpLedger, first + count - 1);
                    cache.forEachInRange(firstPosition, lastPosition, value -> {
                        if (value.refCnt() < 2 || value.getPosition().compareTo(firstPosition) < 0
                                || value.getPosition().compareTo(lastPosition) > 0) {
                            failure.compareAndSet(null, new AssertionError("Unexpected entry " + value));
                        }
                    });
                    if (random.nextInt(4) == 0) {
                        cache.removeRange(firstPosition, lastPosition, true);
                    }
                }
            }));
        }
        futures.add(executor.submit(() -> {
            while (!done.get()) {
                removalQueue.evictLeastAccessedEntries(1_000);
            }
        }));
        try {
            RangeCache.Inserter tailInserter = cache.newInserter();
            for (int i = 0; i < tailEntries; i++) {
                offer(tailInserter, offered, PositionFactory.create(tailLedger, i));
                if (i % 1_000 == 999) {
                    // the cursors moved past the oldest tail entries
                    cache.removeRange(PositionFactory.create(tailLedger, 0),
                            PositionFactory.create(tailLedger, i - 500), false);
                }
            }
        } finally {
            done.set(true);
        }
        for (Future<?> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }
        assertNull(failure.get());

        cache.clear();
        removalQueue.evictLeastAccessedEntries(Long.MAX_VALUE);
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);
        assertEquals(cache.getNumberOfPages(), 0);
        assertThat(offered).allSatisfy(value -> assertEquals(value.refCnt(), 1));
        offered.forEach(ReferenceCountedEntry::release);
    }

    // offers a new entry to the cache, which takes over its reference when it inserts it
    private static void offer(RangeCache.Inserter inserter, Queue<ReferenceCountedEntry> offered, Position position) {
        ReferenceCountedEntry value = createCachedEntry(position, "x");
        value.retain();
        offered.add(value);
        if (!inserter.put(position, value, value.getLength())) {
            value.release();
        }
    }

    private void putToCache(RangeCache cache, long ledgerId, long entryId) {
        Position position = PositionFactory.create(ledgerId, entryId);
        assertTrue(cache.put(position, createCachedEntry(position, Long.toString(entryId))));
    }

    private static void assertRange(RangeCache cache, Position first, Position last, Position... expected) {
        List<ReferenceCountedEntry> range = new ArrayList<>(cache.getRange(first, last));
        try {
            assertThat(range).map(Entry::getPosition).containsExactly(expected);
        } finally {
            range.forEach(ReferenceCountedEntry::release);
        }
    }

    // gets the cached entry, releases the reference that get added and returns the entry
    private static ReferenceCountedEntry releaseRetained(RangeCache cache, Position position) {
        ReferenceCountedEntry value = cache.get(position);
        assertNotNull(value);
        value.release();
        return value;
    }
}
