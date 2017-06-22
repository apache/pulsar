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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.netty.buffer.Unpooled;

@Test
public class EntryCacheTest extends MockedBookKeeperTestCase {

    private ManagedLedgerImpl ml;

    @BeforeMethod
    public void setUp(Method method) throws Exception {
        super.setUp(method);
        ml = mock(ManagedLedgerImpl.class);
        when(ml.getName()).thenReturn("name");
        when(ml.getExecutor()).thenReturn(executor);
        when(ml.getMBean()).thenReturn(new ManagedLedgerMBeanImpl(ml));
    }

    @Test(timeOut = 5000)
    void testRead() throws Exception {
        LedgerHandle lh = getLedgerHandle();
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
        verify(lh, never()).asyncReadEntries(anyLong(), anyLong(), any(ReadCallback.class), any());
    }

    @Test(timeOut = 5000)
    void testReadMissingBefore() throws Exception {
        LedgerHandle lh = getLedgerHandle();
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
    void testReadMissingAfter() throws Exception {
        LedgerHandle lh = getLedgerHandle();
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
    void testReadMissingMiddle() throws Exception {
        LedgerHandle lh = getLedgerHandle();
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
    void testReadMissingMultiple() throws Exception {
        LedgerHandle lh = getLedgerHandle();
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
    void testReadWithError() throws Exception {
        final LedgerHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        doAnswer(new Answer<Object>() {
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                ReadCallback callback = (ReadCallback) args[2];
                Object ctx = args[3];
                callback.readComplete(BKException.Code.NoSuchLedgerExistsException, lh, null, ctx);
                return null;
            }
        }).when(lh).asyncReadEntries(anyLong(), anyLong(), any(ReadCallback.class), any());

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

    private static LedgerHandle getLedgerHandle() {
        final LedgerHandle lh = mock(LedgerHandle.class);
        final LedgerEntry ledgerEntry = mock(LedgerEntry.class, Mockito.CALLS_REAL_METHODS);
        doReturn(new byte[10]).when(ledgerEntry).getEntry();
        doReturn(Unpooled.wrappedBuffer(new byte[10])).when(ledgerEntry).getEntryBuffer();
        doReturn((long) 10).when(ledgerEntry).getLength();

        doAnswer(new Answer<Object>() {
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                long firstEntry = (Long) args[0];
                long lastEntry = (Long) args[1];
                ReadCallback callback = (ReadCallback) args[2];
                Object ctx = args[3];

                Vector<LedgerEntry> entries = new Vector<LedgerEntry>();
                for (int i = 0; i <= (lastEntry - firstEntry); i++) {
                    entries.add(ledgerEntry);
                }
                callback.readComplete(0, lh, entries.elements(), ctx);
                return null;
            }
        }).when(lh).asyncReadEntries(anyLong(), anyLong(), any(ReadCallback.class), any());

        return lh;
    }

}
