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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RangeCacheRemovalQueueTest {
    private RangeCache rangeCache;

    @BeforeMethod
    public void setup() {
        rangeCache = mock(RangeCache.class);
        // mock removal so that it updates counters correctly
        doAnswer(invocation -> {
            RangeCacheEntryWrapper entryWrapper = invocation.getArgument(2);
            RangeCacheRemovalCounters counters = invocation.getArgument(3);
            counters.entryRemoved(entryWrapper.size);
            return true;
        }).when(rangeCache).removeEntry(any(), any(), any(), any(), anyBoolean());
    }

    @AfterMethod
    public void tearDown() {
        // Clean up any resources if needed
        reset(rangeCache);
        rangeCache = null;
    }

    private RangeCacheEntryWrapper addWrappedEntry(RangeCacheRemovalQueue q,
                                                   long ledgerId,
                                                   long entryId,
                                                   int expectedReads,
                                                   long size) {
        Position pos = PositionFactory.create(ledgerId, entryId);
        // Use real EntryImpl (ReferenceCountedEntry) with expectedReadCount
        EntryImpl entry = EntryImpl.create(ledgerId, entryId, Unpooled.EMPTY_BUFFER, expectedReads);

        // Create and add a real RangeCacheEntryWrapper (no mocks)
        return RangeCacheEntryWrapper.withNewInstance(
                rangeCache,
                pos,
                entry,
                size,
                wrapper -> {
                    boolean offered = q.addEntry(wrapper);
                    assertTrue(offered, "Entry should be added to the removal queue");
                    return wrapper;
                }
        );
    }

    @Test
    public void testAddEntryAndIsEmpty() {
        RangeCacheRemovalQueue q = new RangeCacheRemovalQueue(0, false);
        assertTrue(q.isEmpty());
        RangeCacheEntryWrapper w = addWrappedEntry(q, 1L, 1L, 0, 10);
        assertNotNull(w);
        assertFalse(q.isEmpty());
    }

    @Test
    public void testTimeBasedEvictionRespectsExpectedReadsRequeueThreshold() throws Exception {
        // Allow one requeue for entries that still have expected reads
        RangeCacheRemovalQueue q = new RangeCacheRemovalQueue(
                /*maxRequeueCountWhenHasExpectedReads*/ 1,
                /*extendTTLOfRecentlyAccessed*/ false);

        RangeCacheEntryWrapper w = addWrappedEntry(q, 2L, 1L, 1 /* has expected reads */, 10);
        assertNotNull(w);

        // Make it "expired" by setting timestamp far in the past
        long now = System.nanoTime();
        w.timestampNanos = now - 10_000_000L; // 10ms earlier
        long evictionThreshold = now - 1L;

        // First eviction - should requeue due to expected reads
        Pair<Integer, Long> result1 = q.evictLEntriesBeforeTimestamp(evictionThreshold);
        assertNotNull(result1);
        assertFalse(q.isEmpty(), "Entry should be requeued, not dropped");
        assertEquals(w.requeueCount, 1, "Entry should have been requeued once");

        // Make sure the entry is still retained; now it should be dropped after reaching threshold
        // Mark it expired again (in case markRequeued bumped timestamp)
        w.timestampNanos = System.nanoTime() - 10_000_000L;

        Pair<Integer, Long> result2 = q.evictLEntriesBeforeTimestamp(System.nanoTime() - 1L);
        assertNotNull(result2);
        assertTrue(q.isEmpty(), "Entry should be removed after exceeding requeue threshold");
    }

    @Test
    public void testExtendTTLOfRecentlyAccessedRequeuesOnTimeBasedEviction() {
        // Enable extend TTL for recently accessed entries
        RangeCacheRemovalQueue q = new RangeCacheRemovalQueue(
                /*maxRequeueCountWhenHasExpectedReads*/ 0,
                /*extendTTLOfRecentlyAccessed*/ true);

        RangeCacheEntryWrapper w = addWrappedEntry(q, 3L, 1L, 0 /* no expected reads */, 10);
        assertNotNull(w);

        // Simulate access
        w.accessed = true;

        // Make the entry expired
        long now = System.nanoTime();
        w.timestampNanos = now - 10_000_000L;
        long evictionThreshold = now - 1L;

        // First eviction should requeue because 'extendTTLOfRecentlyAccessed' is enabled and the entry was accessed
        Pair<Integer, Long> result1 = q.evictLEntriesBeforeTimestamp(evictionThreshold);
        assertNotNull(result1);
        assertFalse(q.isEmpty(), "Recently accessed entry should be requeued, not dropped");
        assertEquals(w.requeueCount, 1, "Entry should have been requeued once due to recent access");
        assertFalse(w.accessed, "Access flag should be cleared on requeue");

        // Expire again and evict - since it was not accessed after the requeue, it should be removed now
        w.timestampNanos = System.nanoTime() - 10_000_000L;
        Pair<Integer, Long> result2 = q.evictLEntriesBeforeTimestamp(System.nanoTime() - 1L);
        assertNotNull(result2);
        assertTrue(q.isEmpty(), "Entry should be removed if it is expired and not recently accessed again");
    }

    @Test
    public void testSizeBasedEvictionEvictsAsLongAsNeeded() {
        RangeCacheRemovalQueue q = new RangeCacheRemovalQueue(
                /*maxRequeueCountWhenHasExpectedReads*/ 1,
                /*extendTTLOfRecentlyAccessed*/ true);

        // Three entries, total = 40 + 50 + 30 = 120
        RangeCacheEntryWrapper e1 = addWrappedEntry(q, 11L, 1L, 0, 40); // recently accessed; first pass should requeue
        e1.accessed = true;
        RangeCacheEntryWrapper e2 = addWrappedEntry(q, 11L, 2L, 3, 50); // has expected reads; last resort
        RangeCacheEntryWrapper e3 = addWrappedEntry(q, 11L, 3L, 0, 30); // easiest to evict

        // Request to free everything in one go
        Pair<Integer, Long> r1 = q.evictLeastAccessedEntries(120);
        assertNotNull(r1);
        assertThat(r1.getRight().longValue()).isEqualTo(120L);
        assertThat(e1.key).isNull();
        assertThat(e2.key).isNull();
        assertThat(e3.key).isNull();
    }

    @Test
    public void testSizeBasedEvictionEvictsEasiestFirstAsLongAsNeeded() {
        RangeCacheRemovalQueue q = new RangeCacheRemovalQueue(
                /*maxRequeueCountWhenHasExpectedReads*/ 1,
                /*extendTTLOfRecentlyAccessed*/ true);

        // Three entries, total = 40 + 50 + 30 = 120
        RangeCacheEntryWrapper e1 = addWrappedEntry(q, 11L, 1L, 0, 40); // recently accessed; first pass should requeue
        e1.accessed = true;
        RangeCacheEntryWrapper e2 = addWrappedEntry(q, 11L, 2L, 3, 50); // has expected reads; last resort
        RangeCacheEntryWrapper e3 = addWrappedEntry(q, 11L, 3L, 0, 30); // easiest to evict

        // Request to free everything in one go
        Pair<Integer, Long> r1 = q.evictLeastAccessedEntries(20);
        assertNotNull(r1);
        assertThat(r1.getRight().longValue()).isEqualTo(30L);
        assertThat(e1.key).isNotNull();
        assertThat(e2.key).isNotNull();
        assertThat(e3.key).isNull();
    }

    @Test
    public void testSizeBasedEvictionEvictsExpectedReadsLast() {
        RangeCacheRemovalQueue q = new RangeCacheRemovalQueue(
                /*maxRequeueCountWhenHasExpectedReads*/ 5,
                /*extendTTLOfRecentlyAccessed*/ true);

        // Three entries, total = 40 + 50 + 30 = 120
        RangeCacheEntryWrapper e1 = addWrappedEntry(q, 11L, 1L, 0, 40); // recently accessed; first pass should requeue
        e1.accessed = true;
        RangeCacheEntryWrapper e2 = addWrappedEntry(q, 11L, 2L, 3, 50); // has expected reads; last resort
        RangeCacheEntryWrapper e3 = addWrappedEntry(q, 11L, 3L, 0, 30); // easiest to evict

        // Request to free everything in one go
        Pair<Integer, Long> r1 = q.evictLeastAccessedEntries(35);
        assertNotNull(r1);
        assertThat(r1.getRight().longValue()).isEqualTo(70L);
        assertThat(e1.key).isNull();
        assertThat(e2.key).isNotNull();
        assertThat(e3.key).isNull();
    }

    @Test
    public void testGetNonEvictableSizeCountsEntriesWithExpectedReads() {
        RangeCacheRemovalQueue q = new RangeCacheRemovalQueue(
                /*maxRequeueCountWhenHasExpectedReads*/ 0,
                /*extendTTLOfRecentlyAccessed*/ false);

        // Entry that has expected reads should be included
        RangeCacheEntryWrapper w1 = addWrappedEntry(q, 1L, 1L, 2 /* expectedReads > 0 */, 100);
        // Entry that has no expected reads should be excluded
        RangeCacheEntryWrapper w2 = addWrappedEntry(q, 1L, 2L, 1, 50);

        // Make sure wrappers were created
        assertNotNull(w1);
        assertNotNull(w2);

        Pair<Integer, Long> nonEvictable = q.getNonEvictableSize();
        assertEquals(nonEvictable.getLeft().intValue(), 2);
        assertEquals(nonEvictable.getRight().longValue(), 150L);

        // mark the second entry as read to simulate expected reads
        w2.value.getReadCountHandler().markRead();

        nonEvictable = q.getNonEvictableSize();
        assertEquals(nonEvictable.getLeft().intValue(), 1, "Only entries with expected reads should be counted");
        assertEquals(nonEvictable.getRight().longValue(), 100L, "Size should match the entry having expected reads");
    }
}