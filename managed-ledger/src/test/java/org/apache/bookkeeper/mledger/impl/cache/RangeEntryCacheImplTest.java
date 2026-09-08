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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReferenceCountedEntry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryMBeanImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RangeEntryCacheImplTest {
    private RangeEntryCacheImpl rangeEntryCache;
    private RangeEntryCacheManagerImpl mockEntryCacheManager;
    private ManagedLedgerImpl mockManagedLedger;
    private RangeCacheRemovalQueue mockRangeCacheRemovalQueue;
    private PendingReadsManager pendingReadsManager;
    private ReadHandle lh;
    private IntSupplier expectedReadCount;

    @BeforeMethod
    public void setup() {
        mockEntryCacheManager = mock(RangeEntryCacheManagerImpl.class);
        ManagedLedgerFactoryMBeanImpl mlFactoryMBean = mock(ManagedLedgerFactoryMBeanImpl.class);
        when(mockEntryCacheManager.getMlFactoryMBean()).thenReturn(mlFactoryMBean);
        mockManagedLedger = mock(ManagedLedgerImpl.class);
        ManagedLedgerMBeanImpl mockManagedLedgerMBean = mock(ManagedLedgerMBeanImpl.class);
        when(mockManagedLedger.getMbean()).thenReturn(mockManagedLedgerMBean);
        when(mockManagedLedger.getName()).thenReturn("testManagedLedger");
        mockRangeCacheRemovalQueue = mock(RangeCacheRemovalQueue.class);
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
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(4);
            Object ctx = invocation.getArgument(5);
            List<Entry> entries = new ArrayList<>((int) (lastEntry - firstEntry + 1));
            for (long entryId = firstEntry; entryId <= lastEntry; entryId++) {
                entries.add(EntryImpl.create(1, entryId, Unpooled.EMPTY_BUFFER));
            }
            callback.readEntriesComplete(entries, ctx);
            return null;
        }).when(pendingReadsManager).readEntries(any(), anyLong(), anyLong(), any(), any(), any());
        rangeEntryCache = createRangeEntryCache(false);
        lh = mock(ReadHandle.class);
        when(lh.getId()).thenReturn(1L);
        expectedReadCount = () -> 1;
    }

    private RangeEntryCacheImpl createRangeEntryCache(boolean copyEntries) {
        return new RangeEntryCacheImpl(mockEntryCacheManager, mockManagedLedger, copyEntries,
                mockRangeCacheRemovalQueue, EntryLengthFunction.DEFAULT, pendingReadsManager);
    }

    private static ByteBuf serializeMessage(String producerName) {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName(producerName)
                .setSequenceId(7)
                .setPublishTime(123456789L);
        return Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata,
                Unpooled.copiedBuffer("payload", StandardCharsets.UTF_8));
    }

    @Test
    public void testInsertParsesMessageMetadata() {
        ByteBuf headersAndPayload = serializeMessage("producer");
        EntryImpl entry = EntryImpl.create(1, 50, headersAndPayload);
        headersAndPayload.release();
        assertThat(entry.getMessageMetadata()).isNull();

        assertThat(rangeEntryCache.insert(entry)).isTrue();
        entry.release();

        // the metadata is parsed once at insert time instead of lazily on the first cache read. This has to be
        // asserted before reading the entry back, because reading it would itself trigger the lazy path
        assertThat(rangeEntryCache.getEntries().isMessageMetadataInitialized(PositionFactory.create(1, 50)))
                .isTrue();

        ReferenceCountedEntry cached = rangeEntryCache.getEntries().get(PositionFactory.create(1, 50));
        assertThat(cached).isNotNull();
        assertThat(cached.getMessageMetadata()).isNotNull();
        assertThat(cached.getMessageMetadata().getProducerName()).isEqualTo("producer");
        assertThat(cached.getMessageMetadata().getSequenceId()).isEqualTo(7);
        cached.release();
    }

    @Test
    public void testInsertReusesTheMessageMetadataTheEntryAlreadyCarries() {
        ByteBuf headersAndPayload = serializeMessage("in-buffer");
        EntryImpl entry = EntryImpl.create(1, 50, headersAndPayload);
        headersAndPayload.release();
        // deliberately disagrees with the buffer, so that a silent re-parse cannot pass this test
        MessageMetadata suppliedByTheCaller = new MessageMetadata()
                .setProducerName("from-publish-path")
                .setSequenceId(7)
                .setPublishTime(123456789L);
        entry.setMessageMetadata(suppliedByTheCaller);

        assertThat(rangeEntryCache.insert(entry)).isTrue();
        entry.release();

        // the cached entry reuses the instance instead of parsing the same bytes a second time
        ReferenceCountedEntry cached = rangeEntryCache.getEntries().get(PositionFactory.create(1, 50));
        assertThat(cached).isNotNull();
        assertThat(cached.getMessageMetadata()).isSameAs(suppliedByTheCaller);
        assertThat(cached.getMessageMetadata().getProducerName()).isEqualTo("from-publish-path");
        cached.release();
    }

    @Test
    public void testCachedEntryMetadataStaysReadableWhenEntriesAreCopied() {
        RangeEntryCacheImpl copyingCache = createRangeEntryCache(true);
        ByteBuf headersAndPayload = serializeMessage("producer");
        EntryImpl entry = EntryImpl.create(1, 50, headersAndPayload);
        headersAndPayload.release();

        assertThat(copyingCache.insert(entry)).isTrue();
        // the source entry, and with it the buffer the metadata was parsed from, is released right after the
        // insert, exactly as OpAddEntry does on the write path
        entry.release();
        assertThat(headersAndPayload.refCnt()).isZero();

        ReferenceCountedEntry cached = copyingCache.getEntries().get(PositionFactory.create(1, 50));
        assertThat(cached).isNotNull();
        // MessageMetadata decodes its string and bytes fields lazily from the buffer it was parsed from, so the
        // cached entry must not share metadata that was parsed from the now released source buffer
        assertThat(cached.getMessageMetadata()).isNotNull();
        assertThat(cached.getMessageMetadata().getProducerName()).isEqualTo("producer");
        assertThat(cached.getMessageMetadata().getSequenceId()).isEqualTo(7);
        cached.release();
    }

    @Test
    public void testPartialCachingWithMiddleEntryInCache() {
        Entry entry = EntryImpl.create(1, 50, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(49L), any(), any(), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(51L), eq(99L), any(), any(), any());
    }

    @Test
    public void testPartialCachingWithFirstEntryInCache() {
        Entry entry = EntryImpl.create(1, 0, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(1L), eq(99L), any(), any(), any());
    }

    @Test
    public void testPartialCachingWithLastEntryInCache() {
        Entry entry = EntryImpl.create(1, 99, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(98L), any(), any(), any());
    }

    @Test
    public void testPartialCachingWithMiddleRangeInCache() {
        Entry entry = EntryImpl.create(1, 50, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        entry = EntryImpl.create(1, 51, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(49L), any(), any(), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(52L), eq(99L), any(), any(), any());
    }

    @Test
    public void testPartialCachingWithFirstRangeInCache() {
        Entry entry = EntryImpl.create(1, 0, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        entry = EntryImpl.create(1, 1, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(2L), eq(99L), any(), any(), any());
    }

    @Test
    public void testPartialCachingWithLastRangeInCache() {
        Entry entry = EntryImpl.create(1, 98, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        EntryImpl.create(1, 99, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(97L), any(), any(), any());
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
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(4L), any(), any(), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(6L), eq(14L), any(), any(), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(16L), eq(74L), any(), any(), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(77L), eq(77L), any(), any(), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(79L), eq(99L), any(), any(), any());
    }

    @Test
    public void testPartialCachingWithMultipleEntriesInCacheWhilePartialReadFails() {
        Entry entry = EntryImpl.create(1, 50, Unpooled.EMPTY_BUFFER);
        rangeEntryCache.insert(entry);
        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(4);
            Object ctx = invocation.getArgument(5);
            System.out.println("Injecting test failure for readEntries");
            callback.readEntriesFailed(new ManagedLedgerException("Injected test failure"), ctx);
            return null;
        }).when(pendingReadsManager).readEntries(any(), eq(51L), eq(99L), any(), any(), any());
        performReadAndValidateResult();
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(49L), any(), any(), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(51L), eq(99L), any(), any(), any());
        verify(pendingReadsManager, times(1)).readEntries(any(), eq(0L), eq(99L), any(), any(), any());
    }

    @Test
    public void testReadFromStorageRetriesWhenHandleClosed() {
        RangeEntryCacheManagerImpl mockEntryCacheManager = mock(RangeEntryCacheManagerImpl.class);
        ManagedLedgerFactoryMBeanImpl mlFactoryMBean = mock(ManagedLedgerFactoryMBeanImpl.class);
        when(mockEntryCacheManager.getMlFactoryMBean()).thenReturn(mlFactoryMBean);
        ManagedLedgerImpl mockManagedLedger = mock(ManagedLedgerImpl.class);
        ManagedLedgerMBeanImpl mockManagedLedgerMBean = mock(ManagedLedgerMBeanImpl.class);
        when(mockManagedLedger.getMbean()).thenReturn(mockManagedLedgerMBean);
        when(mockManagedLedger.getName()).thenReturn("testManagedLedger");
        when(mockManagedLedger.getExecutor()).thenReturn(mock(java.util.concurrent.ExecutorService.class));
        when(mockManagedLedger.getOptionalLedgerInfo(1L)).thenReturn(Optional.empty());
        RangeCacheRemovalQueue mockRangeCacheRemovalQueue = mock(RangeCacheRemovalQueue.class);
        when(mockRangeCacheRemovalQueue.addEntry(any())).thenReturn(true);
        InflightReadsLimiter inflightReadsLimiter = mock(InflightReadsLimiter.class);
        when(mockEntryCacheManager.getInflightReadsLimiter()).thenReturn(inflightReadsLimiter);
        doAnswer(invocation -> {
            long permits = invocation.getArgument(0);
            InflightReadsLimiter.Handle handle = new InflightReadsLimiter.Handle(permits, System.currentTimeMillis(),
                    true);
            return Optional.of(handle);
        }).when(inflightReadsLimiter).acquire(anyLong(), any());

        RangeEntryCacheImpl cache = new RangeEntryCacheImpl(mockEntryCacheManager, mockManagedLedger, false,
                mockRangeCacheRemovalQueue, EntryLengthFunction.DEFAULT, mock(PendingReadsManager.class));

        ReadHandle readHandle = mock(ReadHandle.class);
        when(readHandle.getId()).thenReturn(1L);
        when(mockManagedLedger.reopenReadHandle(1L)).thenReturn(CompletableFuture.completedFuture(readHandle));

        LedgerEntryImpl ledgerEntry = LedgerEntryImpl.create(1L, 0L, 1, Unpooled.wrappedBuffer(new byte[] {1}));
        LedgerEntries ledgerEntries = mock(LedgerEntries.class);
        List<LedgerEntry> entryList = List.of((LedgerEntry) ledgerEntry);
        when(ledgerEntries.iterator()).thenReturn(entryList.iterator());

        AtomicInteger readAttempts = new AtomicInteger();
        when(readHandle.readAsync(0L, 0L)).thenAnswer(invocation -> {
            if (readAttempts.getAndIncrement() == 0) {
                return CompletableFuture.failedFuture(new ManagedLedgerException.OffloadReadHandleClosedException());
            }
            return CompletableFuture.completedFuture(ledgerEntries);
        });

        CompletableFuture<List<Entry>> future = cache.readFromStorage(readHandle, 0L, 0L, () -> 1);
        assertThat(future).isCompleted().satisfies(f -> {
            List<Entry> entries = f.getNow(null);
            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).getLedgerId()).isEqualTo(1L);
            assertThat(entries.get(0).getEntryId()).isEqualTo(0L);
        });
        assertThat(readAttempts.get()).isEqualTo(2);
    }

    private void performReadAndValidateResult() {
        CompletableFuture<List<Entry>> future = new CompletableFuture<>();
        rangeEntryCache.asyncReadEntry(lh, 0, 99, expectedReadCount, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                future.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
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