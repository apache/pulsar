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
import static org.mockito.Mockito.never;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
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
    private ManagedLedgerConfig managedLedgerConfig;
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
        managedLedgerConfig = new ManagedLedgerConfig();
        when(mockManagedLedger.getConfig()).thenReturn(managedLedgerConfig);
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

    @Test
    public void testReadPermitsReleasedOnlyAfterLastEntryReference() {
        InflightReadsLimiter limiter = mockEntryCacheManager.getInflightReadsLimiter();
        InflightReadsLimiter.Handle handle = new InflightReadsLimiter.Handle(300, 0, true);
        CompletableFuture<List<Entry>> result = new CompletableFuture<>();
        rangeEntryCache.doAsyncReadEntriesWithAcquiredPermits(lh,
                PositionFactory.create(1, 0), PositionFactory.create(1, 2), 3, expectedReadCount,
                new AsyncCallbacks.ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                        result.complete(entries);
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        result.completeExceptionally(exception);
                    }
                }, null, handle, 300);
        assertThat(result).isCompleted();
        List<Entry> entries = result.getNow(null);
        try {
            assertThat(entries).hasSize(3);
            ((ReferenceCountedEntry) entries.get(0)).retain();
            entries.get(2).release();
            entries.get(0).release();
            entries.get(1).release();
            verify(limiter, never()).release(any());
            entries.get(0).release();
            verify(limiter, times(1)).release(handle);
        } finally {
            for (Entry entry : entries) {
                ReferenceCountedEntry referenceCountedEntry = (ReferenceCountedEntry) entry;
                if (referenceCountedEntry.refCnt() > 0) {
                    referenceCountedEntry.release(referenceCountedEntry.refCnt());
                }
            }
        }
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
        ByteBuf payload = Unpooled.copiedBuffer("payload", StandardCharsets.UTF_8);
        try {
            // serializeMetadataAndPayload copies the payload instead of taking ownership of it
            return Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        } finally {
            payload.release();
        }
    }

    @Test
    public void testInsertParsesMessageMetadata() {
        ByteBuf headersAndPayload = serializeMessage("producer");
        EntryImpl entry = EntryImpl.create(1, 50, headersAndPayload);
        headersAndPayload.release();
        assertThat(entry.getMessageMetadata()).isNull();

        assertThat(rangeEntryCache.insert(entry)).isTrue();
        entry.release();

        // the metadata is parsed once at insert time. Reading the entry back out of the cache doesn't parse
        // anything any more, so this asserts what insert actually stored
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
    public void testReadFromStorageDoesNotShareSourceMetadataWithTheCopiedCacheEntry() {
        RangeEntryCacheImpl copyingCache = createRangeEntryCache(true);
        when(mockManagedLedger.getExecutor()).thenReturn(mock(ExecutorService.class));
        // without ledger info, ReadEntryUtils reads through ReadHandle#readAsync
        when(mockManagedLedger.getOptionalLedgerInfo(1L)).thenReturn(Optional.empty());

        ByteBuf headersAndPayload = serializeMessage("producer");
        LedgerEntryImpl ledgerEntry =
                LedgerEntryImpl.create(1L, 0L, headersAndPayload.readableBytes(), headersAndPayload);
        LedgerEntries ledgerEntries = mock(LedgerEntries.class);
        when(ledgerEntries.iterator()).thenReturn(List.<LedgerEntry>of(ledgerEntry).iterator());
        when(lh.readAsync(0L, 0L)).thenReturn(CompletableFuture.completedFuture(ledgerEntries));

        CompletableFuture<List<Entry>> future = copyingCache.readFromStorage(lh, 0L, 0L, expectedReadCount);
        assertThat(future).isCompleted();
        List<Entry> readEntries = future.getNow(null);
        assertThat(readEntries).hasSize(1);
        Entry sourceEntry = readEntries.get(0);

        // unlike the write path, the read path parses the metadata of the entry it returns before inserting it,
        // so this is the case where insert receives an entry that already carries a MessageMetadata. Only the
        // eagerly decoded sequenceId is read from it here, since reading a string field would decode it from
        // the buffer and keep the decoded value, hiding the very problem this test is about
        MessageMetadata sourceMetadata = sourceEntry.getMessageMetadata();
        assertThat(sourceMetadata).isNotNull();
        assertThat(sourceMetadata.getSequenceId()).isEqualTo(7);

        ReferenceCountedEntry cached = copyingCache.getEntries().get(PositionFactory.create(1, 0));
        assertThat(cached).isNotNull();
        // the cached entry is backed by a copy of the payload, so it must not share the metadata that was parsed
        // from the source buffer
        assertThat(cached.getMessageMetadata()).isNotNull().isNotSameAs(sourceMetadata);

        // overwrite the source payload while it is still referenced, the way the pooled buffer behind it gets
        // overwritten once it has been recycled. This turns a leftover dependency on the source buffer into a
        // wrong value rather than into a read that only fails when the released memory happens to be reused
        headersAndPayload.setZero(headersAndPayload.readerIndex(), headersAndPayload.readableBytes());
        // metadata parsed from the source buffer does decode the overwritten bytes, which keeps the assertions
        // below from turning vacuous should MessageMetadata ever stop decoding these fields lazily
        assertThat(sourceMetadata.getProducerName()).isNotEqualTo("producer");

        // the read path releases the entries it returned once dispatch is done, while the cached copy stays
        sourceEntry.release();
        // readFromStorage closes the LedgerEntries, but that is a mock here, so the reference the read result
        // holds is dropped explicitly instead
        ledgerEntry.close();
        assertThat(headersAndPayload.refCnt()).isZero();

        // MessageMetadata decodes its string and bytes fields lazily from the buffer it was parsed from, so the
        // cached entry stays readable only because its metadata was parsed from the buffer the cache owns
        assertThat(cached.getMessageMetadata().getProducerName()).isEqualTo("producer");
        assertThat(cached.getMessageMetadata().getSequenceId()).isEqualTo(7);
        cached.release();
    }

    @Test
    public void testInsertDoesNotParseMessageMetadataWhenTheEntriesArentPulsarMessages() {
        // the transaction log and the pending ack store keep entries that are not Pulsar messages, so the entry
        // cache must not try to parse message metadata out of them
        managedLedgerConfig.setPulsarMessageEntries(false);

        ByteBuf headersAndPayload = serializeMessage("producer");
        EntryImpl entry = EntryImpl.create(1, 50, headersAndPayload);
        headersAndPayload.release();

        assertThat(rangeEntryCache.insert(entry)).isTrue();
        entry.release();

        ReferenceCountedEntry cached = rangeEntryCache.getEntries().get(PositionFactory.create(1, 50));
        assertThat(cached).isNotNull();
        assertThat(cached.getMessageMetadata()).isNull();
        cached.release();

        // reading the entry back through the cache must not parse it either. This is what pins the removal of
        // the lazy initialization that RangeCacheEntryWrapper used to do under its write lock, which would have
        // defeated skipping the parse at insert time
        Entry readBack = readSingleEntryFromCache(1, 50);
        assertThat(readBack.getMessageMetadata()).isNull();
        readBack.release();
        // the read has to have been served from the cache. A miss would fall through to the mocked storage,
        // which hands back an entry that carries no metadata either, making the assertion above vacuous
        verify(pendingReadsManager, never()).readEntries(any(), anyLong(), anyLong(), any(), any(), any());

        // control: the very same bytes are parsed when the managed ledger does hold Pulsar messages, so the
        // assertions above can't pass merely because the payload happens to be unparseable
        managedLedgerConfig.setPulsarMessageEntries(true);
        ByteBuf controlHeadersAndPayload = serializeMessage("producer");
        EntryImpl controlEntry = EntryImpl.create(1, 51, controlHeadersAndPayload);
        controlHeadersAndPayload.release();
        assertThat(rangeEntryCache.insert(controlEntry)).isTrue();
        controlEntry.release();

        ReferenceCountedEntry cachedControl = rangeEntryCache.getEntries().get(PositionFactory.create(1, 51));
        assertThat(cachedControl).isNotNull();
        assertThat(cachedControl.getMessageMetadata()).isNotNull();
        assertThat(cachedControl.getMessageMetadata().getProducerName()).isEqualTo("producer");
        cachedControl.release();
    }

    @Test
    public void testReadFromStorageDoesNotParseMessageMetadataWhenTheEntriesArentPulsarMessages() {
        when(mockManagedLedger.getExecutor()).thenReturn(mock(ExecutorService.class));
        // without ledger info, ReadEntryUtils reads through ReadHandle#readAsync
        when(mockManagedLedger.getOptionalLedgerInfo(1L)).thenReturn(Optional.empty());
        managedLedgerConfig.setPulsarMessageEntries(false);

        Entry entryWithoutParsing = readSingleEntryFromStorage(0L);
        assertThat(entryWithoutParsing.getMessageMetadata()).isNull();
        entryWithoutParsing.release();

        // control: the same bytes read over the same path do get parsed when the entries are Pulsar messages
        managedLedgerConfig.setPulsarMessageEntries(true);
        Entry entryWithParsing = readSingleEntryFromStorage(1L);
        assertThat(entryWithParsing.getMessageMetadata()).isNotNull();
        assertThat(entryWithParsing.getMessageMetadata().getProducerName()).isEqualTo("producer");
        entryWithParsing.release();
    }

    /**
     * Reads a single entry back through the cache read path, which is the caller that used to trigger the lazy
     * metadata initialization inside {@link RangeCacheEntryWrapper}.
     *
     * @apiNote the returned entry must be released by the caller
     */
    private Entry readSingleEntryFromCache(long ledgerId, long entryId) {
        CompletableFuture<Entry> future = new CompletableFuture<>();
        rangeEntryCache.asyncReadEntry(lh, PositionFactory.create(ledgerId, entryId),
                new AsyncCallbacks.ReadEntryCallback() {
                    @Override
                    public void readEntryComplete(Entry entry, Object ctx) {
                        future.complete(entry);
                    }

                    @Override
                    public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                        future.completeExceptionally(exception);
                    }
                }, null);
        assertThat(future).isCompleted();
        return future.getNow(null);
    }

    /**
     * Reads a single freshly serialized Pulsar message through {@link RangeEntryCacheImpl#readFromStorage}.
     *
     * @apiNote the returned entry must be released by the caller
     */
    private Entry readSingleEntryFromStorage(long entryId) {
        ByteBuf headersAndPayload = serializeMessage("producer");
        LedgerEntryImpl ledgerEntry =
                LedgerEntryImpl.create(1L, entryId, headersAndPayload.readableBytes(), headersAndPayload);
        LedgerEntries ledgerEntries = mock(LedgerEntries.class);
        when(ledgerEntries.iterator()).thenReturn(List.<LedgerEntry>of(ledgerEntry).iterator());
        when(lh.readAsync(entryId, entryId)).thenReturn(CompletableFuture.completedFuture(ledgerEntries));

        CompletableFuture<List<Entry>> future = rangeEntryCache.readFromStorage(lh, entryId, entryId,
                expectedReadCount);
        assertThat(future).isCompleted();
        List<Entry> readEntries = future.getNow(null);
        assertThat(readEntries).hasSize(1);
        // readFromStorage closes the LedgerEntries, but that is a mock here, so the reference the read result
        // holds is dropped explicitly instead
        ledgerEntry.close();
        return readEntries.get(0);
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
        when(mockManagedLedger.getConfig()).thenReturn(new ManagedLedgerConfig());
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
