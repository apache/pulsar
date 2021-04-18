/**
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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import io.netty.buffer.Unpooled;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntryCacheTest extends MockedBookKeeperTestCase {

    private ManagedLedgerImpl ml;

    @Override
    protected void setUpTestCase() throws Exception {
        ml = mock(ManagedLedgerImpl.class);
        when(ml.getName()).thenReturn("name");
        when(ml.getExecutor()).thenReturn(executor);
        when(ml.getMBean()).thenReturn(new ManagedLedgerMBeanImpl(ml));
    }

    @Test(timeOut = 5000)
    public void testRead() throws Exception {
        ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        byte[] data = new byte[10];
        for (int i = 0; i < 10; i++) {
            entryCache.insert(EntryImpl.create(0, i, data));
        }

        final CountDownLatch counter = new CountDownLatch(1);

        entryCache.asyncReadEntry(lh, 0, 9, false, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertEquals(entries.size(), 10);
                entries.forEach(e -> e.release());
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
        final LedgerEntry ledgerEntry = mock(LedgerEntry.class, Mockito.CALLS_REAL_METHODS);
        doReturn(Unpooled.wrappedBuffer(new byte[10])).when(ledgerEntry).getEntryBuffer();
        doReturn((long) 10).when(ledgerEntry).getLength();

        doAnswer((invocation) -> {
                Object[] args = invocation.getArguments();
                long firstEntry = (Long) args[0];
                long lastEntry = (Long) args[1];

                Vector<LedgerEntry> entries = new Vector<LedgerEntry>();
                for (int i = 0; i <= (lastEntry - firstEntry); i++) {
                    entries.add(ledgerEntry);
                }
                LedgerEntries ledgerEntries = mock(LedgerEntries.class);
                doAnswer((invocation2) -> entries.iterator()).when(ledgerEntries).iterator();
                return CompletableFuture.completedFuture(ledgerEntries);
            }).when(lh).readAsync(anyLong(), anyLong());

        return lh;
    }

}
