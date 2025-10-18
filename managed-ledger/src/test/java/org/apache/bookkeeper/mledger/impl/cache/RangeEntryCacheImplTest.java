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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntSupplier;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryMBeanImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RangeEntryCacheImplTest {
    private RangeEntryCacheImpl rangeEntryCache;
    private PendingReadsManager pendingReadsManager;
    private ReadHandle lh;
    private IntSupplier expectedReadCount;

    @BeforeMethod
    public void setup() {
        RangeEntryCacheManagerImpl mockEntryCacheManager = mock(RangeEntryCacheManagerImpl.class);
        ManagedLedgerFactoryMBeanImpl mlFactoryMBean = mock(ManagedLedgerFactoryMBeanImpl.class);
        when(mockEntryCacheManager.getMlFactoryMBean()).thenReturn(mlFactoryMBean);
        ManagedLedgerImpl mockManagedLedger = mock(ManagedLedgerImpl.class);
        ManagedLedgerMBeanImpl mockManagedLedgerMBean = mock(ManagedLedgerMBeanImpl.class);
        when(mockManagedLedger.getMbean()).thenReturn(mockManagedLedgerMBean);
        when(mockManagedLedger.getName()).thenReturn("testManagedLedger");
        RangeCacheRemovalQueue mockRangeCacheRemovalQueue = mock(RangeCacheRemovalQueue.class);
        when(mockRangeCacheRemovalQueue.addEntry(any())).thenReturn(true);
        InflightReadsLimiter inflightReadsLimiter = mock(InflightReadsLimiter.class);
        when(mockEntryCacheManager.getInflightReadsLimiter()).thenReturn(inflightReadsLimiter);
        doAnswer(invocation -> {
            long permits = invocation.getArgument(0);
            InflightReadsLimiter.Handle
                    handle = new InflightReadsLimiter.Handle(permits, System.currentTimeMillis(), true);
            return Optional.of(handle);
        }).when(inflightReadsLimiter).acquire(anyLong(), any());
        pendingReadsManager = mock(PendingReadsManager.class);
        doAnswer(invocation -> {
            long firstEntry = invocation.getArgument(1);
            long lastEntry = invocation.getArgument(2);
            List<Entry> entries = new ArrayList<>((int) (lastEntry - firstEntry + 1));
            for (long entryId = firstEntry; entryId <= lastEntry; entryId++) {
                entries.add(EntryImpl.create(1, entryId, Unpooled.EMPTY_BUFFER));
            }
            return CompletableFuture.completedFuture(entries);
        }).when(pendingReadsManager).readEntries(any(), anyLong(), anyLong(), any());
        rangeEntryCache =
                new RangeEntryCacheImpl(mockEntryCacheManager, mockManagedLedger, false, mockRangeCacheRemovalQueue,
                        EntryLengthFunction.DEFAULT, pendingReadsManager);
        lh = mock(ReadHandle.class);
        when(lh.getId()).thenReturn(1L);
        expectedReadCount = () -> 1;
    }

    @Test
    public void testPartialCachingWithMiddleEntryInCache() {
        Entry entry = EntryImpl.create(1, 50, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(49L), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(51L), eq(99L), any());
    }

    @Test
    public void testPartialCachingWithFirstEntryInCache() {
        Entry entry = EntryImpl.create(1, 0, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(1L), eq(99L), any());
    }

    @Test
    public void testPartialCachingWithLastEntryInCache() {
        Entry entry = EntryImpl.create(1, 99, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(98L), any());
    }

    @Test
    public void testPartialCachingWithMiddleRangeInCache() {
        Entry entry = EntryImpl.create(1, 50, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        entry = EntryImpl.create(1, 51, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(49L), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(52L), eq(99L), any());
    }

    @Test
    public void testPartialCachingWithFirstRangeInCache() {
        Entry entry = EntryImpl.create(1, 0, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        entry = EntryImpl.create(1, 1, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(2L), eq(99L), any());
    }

    @Test
    public void testPartialCachingWithLastRangeInCache() {
        Entry entry = EntryImpl.create(1, 98, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        EntryImpl.create(1, 99, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(97L), any());
    }

    @Test
    public void testPartialCachingWithMultipleEntriesInCache() {
        Entry entry = EntryImpl.create(1, 5, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        entry = EntryImpl.create(1, 15, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        entry = EntryImpl.create(1, 75, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        entry = EntryImpl.create(1, 76, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        entry = EntryImpl.create(1, 78, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(4L), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(6L), eq(14L), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(16L), eq(74L), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(77L), eq(77L), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(79L), eq(99L), any());
    }

    @Test
    public void testPartialCachingWithMultipleEntriesInCacheWhilePartialReadFails() {
        Entry entry = EntryImpl.create(1, 50, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        doAnswer(invocation -> {
            System.out.println("Injecting test failure for readEntries");
            return CompletableFuture.failedFuture(new ManagedLedgerException("Injected test failure"));
        }).when(pendingReadsManager).readEntries(any(), eq(51L), eq(99L), any());
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(49L), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(51L), eq(99L), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(99L), any());
    }

    private void performReadAndValidateResult() {
        final var future = rangeEntryCache.asyncReadEntry(lh, 0, 99, expectedReadCount);
        assertThat(future).isCompleted().satisfies(f -> {
            List<Entry> entries = f.getNow(null);
            assertThat(entries).hasSize(100);
            for (int i = 0; i < 100; i++) {
                Entry e = entries.get(i);
                assertThat(e.getLedgerId()).isEqualTo(1L);
                assertThat(e.getEntryId()).isEqualTo(i);
            }
        });
    }
}
