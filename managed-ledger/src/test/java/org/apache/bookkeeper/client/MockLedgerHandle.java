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
package org.apache.bookkeeper.client;

import com.google.common.collect.Lists;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.security.GeneralSecurityException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock BK {@link LedgerHandle}. Used by {@link MockBookKeeper}.
 */
public class MockLedgerHandle extends LedgerHandle {

    final ArrayList<LedgerEntryImpl> entries = Lists.newArrayList();
    final MockBookKeeper bk;
    final long id;
    final DigestType digest;
    final byte[] passwd;
    final ReadHandle readHandle;
    long lastEntry = -1;
    boolean fenced = false;

    MockLedgerHandle(MockBookKeeper bk, long id, DigestType digest, byte[] passwd) throws GeneralSecurityException {
        super(bk, id, new LedgerMetadata(3, 3, 2, DigestType.MAC, "".getBytes()), DigestType.MAC, "".getBytes(),
                EnumSet.noneOf(WriteFlag.class));
        this.bk = bk;
        this.id = id;
        this.digest = digest;
        this.passwd = Arrays.copyOf(passwd, passwd.length);

        readHandle = new MockReadHandle(bk, id, getLedgerMetadata(), entries);
    }

    @Override
    public void asyncClose(CloseCallback cb, Object ctx) {
        if (bk.getProgrammedFailStatus()) {
            cb.closeComplete(bk.failReturnCode, this, ctx);
            return;
        }

        fenced = true;
        try {
            bk.executor.execute(() -> cb.closeComplete(0, this, ctx));
        } catch (RejectedExecutionException e) {
            cb.closeComplete(0, this, ctx);
        }

    }

    @Override
    public void asyncReadEntries(final long firstEntry, final long lastEntry, final ReadCallback cb, final Object ctx) {
        if (bk.isStopped()) {
            cb.readComplete(-1, MockLedgerHandle.this, null, ctx);
            return;
        }

        bk.executor.execute(new Runnable() {
            public void run() {
                if (bk.getProgrammedFailStatus()) {
                    cb.readComplete(bk.failReturnCode, MockLedgerHandle.this, null, ctx);
                    return;
                } else if (bk.isStopped()) {
                    log.debug("Bookkeeper is closed!");
                    cb.readComplete(-1, MockLedgerHandle.this, null, ctx);
                    return;
                }

                log.debug("readEntries: first={} last={} total={}", firstEntry, lastEntry, entries.size());
                final Queue<LedgerEntry> seq = new ArrayDeque<LedgerEntry>();
                long entryId = firstEntry;
                while (entryId <= lastEntry && entryId < entries.size()) {
                    seq.add(new LedgerEntry(entries.get((int) entryId++).duplicate()));
                }

                log.debug("Entries read: {}", seq);

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }

                cb.readComplete(0, MockLedgerHandle.this, new Enumeration<LedgerEntry>() {
                    public boolean hasMoreElements() {
                        return !seq.isEmpty();
                    }

                    public LedgerEntry nextElement() {
                        return seq.remove();
                    }

                }, ctx);
            }
        });
    }

    @Override
    public long addEntry(byte[] data) throws InterruptedException, BKException {
        try {
            bk.checkProgrammedFail();
        } catch (BKException e) {
            fenced = true;
            throw e;
        }

        if (fenced) {
            throw BKException.create(BKException.Code.LedgerFencedException);
        }

        if (bk.isStopped()) {
            throw BKException.create(BKException.Code.NoBookieAvailableException);
        }

        lastEntry = entries.size();
        entries.add(LedgerEntryImpl.create(ledgerId, lastEntry, data.length, Unpooled.wrappedBuffer(data)));
        return lastEntry;
    }

    @Override
    public void asyncAddEntry(final byte[] data, final AddCallback cb, final Object ctx) {
        asyncAddEntry(data, 0, data.length, cb, ctx);
    }

    @Override
    public void asyncAddEntry(final byte[] data, final int offset, final int length, final AddCallback cb,
            final Object ctx) {
        asyncAddEntry(Unpooled.wrappedBuffer(data, offset, length), cb, ctx);
    }

    @Override
    public void asyncAddEntry(final ByteBuf data, final AddCallback cb, final Object ctx) {
        if (bk.isStopped()) {
            cb.addComplete(-1, MockLedgerHandle.this, INVALID_ENTRY_ID, ctx);
            return;
        }

        data.retain();
        bk.executor.execute(new Runnable() {
            public void run() {
                if (bk.getProgrammedFailStatus()) {
                    fenced = true;
                    data.release();
                    cb.addComplete(bk.failReturnCode, MockLedgerHandle.this, INVALID_ENTRY_ID, ctx);
                    return;
                }
                if (bk.isStopped()) {
                    data.release();
                    cb.addComplete(-1, MockLedgerHandle.this, INVALID_ENTRY_ID, ctx);
                    return;
                }

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }

                if (fenced) {
                    data.release();
                    cb.addComplete(BKException.Code.LedgerFencedException, MockLedgerHandle.this,
                            LedgerHandle.INVALID_ENTRY_ID, ctx);
                } else {
                    lastEntry = entries.size();
                    byte[] storedData = new byte[data.readableBytes()];
                    data.readBytes(storedData);
                    entries.add(LedgerEntryImpl.create(ledgerId, lastEntry,
                                                       storedData.length, Unpooled.wrappedBuffer(storedData)));
                    data.release();
                    cb.addComplete(0, MockLedgerHandle.this, lastEntry, ctx);
                }
            }
        });
    }

    @Override
    public long getId() {
        return ledgerId;
    }

    @Override
    public long getLastAddConfirmed() {
        return lastEntry;
    }

    @Override
    public long getLength() {
        long length = 0;
        for (LedgerEntryImpl entry : entries) {
            length += entry.getLength();
        }

        return length;
    }


    // ReadHandle interface
    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        return readHandle.readAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return readHandle.readUnconfirmedAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return readHandle.readLastAddConfirmedAsync();
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return readHandle.tryReadLastAddConfirmedAsync();
    }

    @Override
    public boolean isClosed() {
        return readHandle.isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                      long timeOutInMillis,
                                                                                      boolean parallel) {
        return readHandle.readLastAddConfirmedAndEntryAsync(entryId, timeOutInMillis, parallel);
    }

    private static final Logger log = LoggerFactory.getLogger(MockLedgerHandle.class);

}
