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
package org.apache.bookkeeper.mledger.impl;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.cache.EntryCache;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EntryCacheTest extends MockedBookKeeperTestCase {

    private ManagedLedgerImpl ml;

    @Override
    protected void setUpTestCase() throws Exception {
        ml = mock(ManagedLedgerImpl.class);
        when(ml.getName()).thenReturn("name");
        when(ml.getExecutor()).thenReturn(executor);
        when(ml.getMbean()).thenReturn(new ManagedLedgerMBeanImpl(ml));
        when(ml.getConfig()).thenReturn(new ManagedLedgerConfig());
    }

    @Test(timeOut = 5000)
    public void testRead() throws Exception {
        ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        byte[] data = new byte[10];
        for (int i = 0; i < 10; i++) {
            entryCache.insert(EntryImpl.create(0, i, data));
        }

        final CountDownLatch counter = new CountDownLatch(1);

        entryCache.asyncReadEntry(lh, 0, 9, false, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertEquals(entries.size(), 10);
                entries.forEach(Entry::release);
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                Assert.fail("should not have failed");
            }
        }, null);
        counter.await();

        // Verify no entries were read from bookkeeper
        verify(lh, never()).readAsync(anyLong(), anyLong());
    }

    @Test(timeOut = 5000)
    public void testReadMissingBefore() throws Exception {
        ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        byte[] data = new byte[10];
        for (int i = 3; i < 10; i++) {
            entryCache.insert(EntryImpl.create(0, i, data));
        }

        final CountDownLatch counter = new CountDownLatch(1);

        entryCache.asyncReadEntry(lh, 0, 9, false, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertEquals(entries.size(), 10);
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                Assert.fail("should not have failed");
            }
        }, null);
        counter.await();
    }

    @Test(timeOut = 5000)
    public void testReadMissingAfter() throws Exception {
        ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        byte[] data = new byte[10];
        for (int i = 0; i < 8; i++) {
            entryCache.insert(EntryImpl.create(0, i, data));
        }

        final CountDownLatch counter = new CountDownLatch(1);

        entryCache.asyncReadEntry(lh, 0, 9, false, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertEquals(entries.size(), 10);
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                Assert.fail("should not have failed");
            }
        }, null);
        counter.await();
    }

    @Test(timeOut = 5000)
    public void testReadMissingMiddle() throws Exception {
        ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        byte[] data = new byte[10];
        entryCache.insert(EntryImpl.create(0, 0, data));
        entryCache.insert(EntryImpl.create(0, 1, data));
        entryCache.insert(EntryImpl.create(0, 8, data));
        entryCache.insert(EntryImpl.create(0, 9, data));

        final CountDownLatch counter = new CountDownLatch(1);

        entryCache.asyncReadEntry(lh, 0, 9, false, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertEquals(entries.size(), 10);
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                Assert.fail("should not have failed");
            }
        }, null);
        counter.await();
    }

    @Test(timeOut = 5000)
    public void testReadMissingMultiple() throws Exception {
        ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        byte[] data = new byte[10];
        entryCache.insert(EntryImpl.create(0, 0, data));
        entryCache.insert(EntryImpl.create(0, 2, data));
        entryCache.insert(EntryImpl.create(0, 5, data));
        entryCache.insert(EntryImpl.create(0, 8, data));

        final CountDownLatch counter = new CountDownLatch(1);

        entryCache.asyncReadEntry(lh, 0, 9, false, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertEquals(entries.size(), 10);
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                Assert.fail("should not have failed");
            }
        }, null);
        counter.await();
    }

    @Test
    public void testCachedReadReturnsDifferentByteBuffer() throws Exception {
        ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        CompletableFuture<List<Entry>> cacheMissFutureEntries = new CompletableFuture<>();

        entryCache.asyncReadEntry(lh, 0, 1, true, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                cacheMissFutureEntries.complete(entries);
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                cacheMissFutureEntries.completeExceptionally(exception);
            }
        }, null);

        List<Entry> cacheMissEntries = cacheMissFutureEntries.get();
        // Ensure first entry is 0 and
        assertEquals(cacheMissEntries.size(), 2);
        assertEquals(cacheMissEntries.get(0).getEntryId(), 0);
        assertEquals(cacheMissEntries.get(0).getDataBuffer().readerIndex(), 0);

        // Move the reader index to simulate consumption
        cacheMissEntries.get(0).getDataBuffer().readerIndex(10);

        CompletableFuture<List<Entry>> cacheHitFutureEntries = new CompletableFuture<>();

        entryCache.asyncReadEntry(lh, 0, 1, true, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                cacheHitFutureEntries.complete(entries);
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                cacheHitFutureEntries.completeExceptionally(exception);
            }
        }, null);

        List<Entry> cacheHitEntries = cacheHitFutureEntries.get();
        assertEquals(cacheHitEntries.get(0).getEntryId(), 0);
        assertEquals(cacheHitEntries.get(0).getDataBuffer().readerIndex(), 0);
    }

    @Test(timeOut = 5000)
    public void testReadWithError() throws Exception {
        final ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        doAnswer((invocation) -> {
                CompletableFuture<LedgerEntries> future = new CompletableFuture<>();
                future.completeExceptionally(new BKNoSuchLedgerExistsException());
                return future;
            }).when(lh).readAsync(anyLong(), anyLong());

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        byte[] data = new byte[10];
        entryCache.insert(EntryImpl.create(0, 2, data));

        final CountDownLatch counter = new CountDownLatch(1);

        entryCache.asyncReadEntry(lh, 0, 9, false, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                Assert.fail("should not complete");
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                counter.countDown();
            }
        }, null);
        counter.await();
    }

    static ReadHandle getLedgerHandle() {
        final ReadHandle lh = mock(ReadHandle.class);
        doAnswer((invocation) -> {
                Object[] args = invocation.getArguments();
                long firstEntry = (Long) args[0];
                long lastEntry = (Long) args[1];

                List<LedgerEntry> entries = new ArrayList<>();
                for (int i = 0; i <= (lastEntry - firstEntry); i++) {
                    entries.add(LedgerEntryImpl.create(0, i, 10, Unpooled.wrappedBuffer(new byte[10])));
                }
                LedgerEntries ledgerEntries = mock(LedgerEntries.class);
                doAnswer((invocation2) -> entries.iterator()).when(ledgerEntries).iterator();
                return CompletableFuture.completedFuture(ledgerEntries);
            }).when(lh).readAsync(anyLong(), anyLong());

        return lh;
    }

}
