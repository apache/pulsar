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
package org.apache.pulsar.compaction;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-compaction")
public class CompactedTopicImplTest {

    private static final long DEFAULT_LEDGER_ID = 1;

    // Sparse ledger makes multi entry has same data, this is used to construct complex environments to verify that the
    // smallest position with the correct data is found.
    private static final TreeMap<Long, Long> ORIGIN_SPARSE_LEDGER = new TreeMap<>();

    static {
        ORIGIN_SPARSE_LEDGER.put(0L, 0L);
        ORIGIN_SPARSE_LEDGER.put(1L, 1L);
        ORIGIN_SPARSE_LEDGER.put(2L, 1001L);
        ORIGIN_SPARSE_LEDGER.put(3L, 1002L);
        ORIGIN_SPARSE_LEDGER.put(4L, 1003L);
        ORIGIN_SPARSE_LEDGER.put(10L, 1010L);
        ORIGIN_SPARSE_LEDGER.put(20L, 1020L);
        ORIGIN_SPARSE_LEDGER.put(50L, 1050L);
        ORIGIN_SPARSE_LEDGER.put(Long.MAX_VALUE, Long.MAX_VALUE);
    }

    @DataProvider(name = "argsForFindStartPointLoop")
    public Object[][] argsForFindStartPointLoop() {
        return new Object[][]{
                {0, 100, 0},// first value.
                {0, 100, 1},// second value.
                {0, 100, 1003},// not first value.
                {0, 100, 1015},// value not exists.
                {3, 40, 50},// less than first value & find in a range.
                {3, 40, 1002},// first value & find in a range.
                {3, 40, 1003},// second value & find in a range.
                {3, 40, 1010},// not first value & find in a range.
                {3, 40, 1015}// value not exists & find in a range.
        };
    }

    private static CacheLoader<Long, MessageIdData> mockCacheLoader(long start, long end, final long targetMessageId,
                                                                    AtomicLong bingoMarker) {
        // Mock ledger.
        final TreeMap<Long, Long> sparseLedger = new TreeMap<>();
        sparseLedger.putAll(ORIGIN_SPARSE_LEDGER.subMap(start, end + 1));
        sparseLedger.put(Long.MAX_VALUE, Long.MAX_VALUE);

        Function<Long, Long> findMessageIdFunc = entryId -> sparseLedger.ceilingEntry(entryId).getValue();

        // Calculate the correct position.
        for (long i = start; i <= end; i++) {
            if (findMessageIdFunc.apply(i) >= targetMessageId) {
                bingoMarker.set(i);
                break;
            }
        }

        return new CacheLoader<Long, MessageIdData>() {
            @Override
            public @Nullable MessageIdData load(@NonNull Long entryId) throws Exception {
                MessageIdData messageIdData = new MessageIdData();
                messageIdData.setLedgerId(DEFAULT_LEDGER_ID);
                messageIdData.setEntryId(findMessageIdFunc.apply(entryId));
                return messageIdData;
            }
        };
    }

    @Test(dataProvider = "argsForFindStartPointLoop")
    public void testFindStartPointLoop(long start, long end, long targetMessageId) {
        AtomicLong bingoMarker = new AtomicLong();
        // Mock cache.
        AsyncLoadingCache<Long, MessageIdData> cache = Caffeine.newBuilder()
                .buildAsync(mockCacheLoader(start, end, targetMessageId, bingoMarker));
        // Do test.
        PositionImpl targetPosition = PositionImpl.get(DEFAULT_LEDGER_ID, targetMessageId);
        CompletableFuture<Long> promise = new CompletableFuture<>();
        CompactedTopicImpl.findStartPointLoop(targetPosition, start, end, promise, cache);
        long result = promise.join();
        assertEquals(result, bingoMarker.get());
    }

    /**
     * Why should we check the recursion number of "findStartPointLoop", see: #17976
     */
    @Test
    public void testRecursionNumberOfFindStartPointLoop() {
        AtomicLong bingoMarker = new AtomicLong();
        long start = 0;
        long end = 100;
        long targetMessageId = 1;
        // Mock cache.
        AsyncLoadingCache<Long, MessageIdData> cache = Caffeine.newBuilder()
                .buildAsync(mockCacheLoader(start, end, targetMessageId, bingoMarker));
        AtomicInteger invokeCounterOfCacheGet = new AtomicInteger();
        AsyncLoadingCache<Long, MessageIdData> cacheWithCounter = spy(cache);
        doAnswer(invocation -> {
            invokeCounterOfCacheGet.incrementAndGet();
            return cache.get((Long) invocation.getArguments()[0]);
        }).when(cacheWithCounter).get(anyLong());
        // Because when "findStartPointLoop(...)" is executed, will trigger "cache.get()" three times, including
        // "cache.get(start)", "cache.get(mid)" and "cache.get(end)". Therefore, we can calculate the count of
        // executed "findStartPointLoop".
        Supplier<Integer> loopCounter = () -> invokeCounterOfCacheGet.get() / 3;
        // Do test.
        PositionImpl targetPosition = PositionImpl.get(DEFAULT_LEDGER_ID, targetMessageId);
        CompletableFuture<Long> promise = new CompletableFuture<>();
        CompactedTopicImpl.findStartPointLoop(targetPosition, start, end, promise, cacheWithCounter);
        // Do verify.
        promise.join();
        assertEquals(loopCounter.get().intValue(), 2);
    }
}
