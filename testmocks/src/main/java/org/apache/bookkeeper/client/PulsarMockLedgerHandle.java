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
package org.apache.bookkeeper.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.security.GeneralSecurityException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock BK {@link LedgerHandle}. Used by {@link PulsarMockBookKeeper}.
 */
public class PulsarMockLedgerHandle extends LedgerHandle {

    final ArrayList<LedgerEntryImpl> entries = Lists.newArrayList();
    final PulsarMockBookKeeper bk;
    final long id;
    final DigestType digest;
    final byte[] passwd;
    final ReadHandle readHandle;
    long lastEntry = -1;
    @VisibleForTesting
    @Getter
    boolean fenced = false;

    public PulsarMockLedgerHandle(PulsarMockBookKeeper bk, long id,
                           DigestType digest, byte[] passwd) throws GeneralSecurityException {
        super(bk.getClientCtx(), id, new Versioned<>(createMetadata(id, digest, passwd), new LongVersion(0L)),
              digest, passwd, WriteFlag.NONE);
        this.bk = bk;
        this.id = id;
        this.digest = digest;
        this.passwd = Arrays.copyOf(passwd, passwd.length);

        readHandle = new PulsarMockReadHandle(bk, id, getLedgerMetadata(), entries, bk::getReadHandleInterceptor);
    }

    @Override
    public void asyncClose(CloseCallback cb, Object ctx) {
        bk.getProgrammedFailure().thenComposeAsync((res) -> {
            fenced = true;

            Versioned<LedgerMetadata> current = getVersionedLedgerMetadata();
            Versioned<LedgerMetadata> newMetadata = new Versioned<>(
                    LedgerMetadataBuilder.from(current.getValue())
                            .withClosedState().withLastEntryId(getLastAddConfirmed())
                            .withLength(getLength()).build(),
                    new LongVersion(((LongVersion) current.getVersion()).getLongVersion() + 1));
            setLedgerMetadata(current, newMetadata);
            return FutureUtils.value(null);
        }, bk.executor).whenCompleteAsync((res, exception) -> {
            if (exception != null) {
                cb.closeComplete(PulsarMockBookKeeper.getExceptionCode(exception), null, ctx);
            } else {
                cb.closeComplete(BKException.Code.OK, this, ctx);
            }
        }, bk.executor);
    }

    @Override
    public void asyncReadEntries(final long firstEntry, final long lastEntry, final ReadCallback cb, final Object ctx) {
        bk.getProgrammedFailure().thenComposeAsync((res) -> {
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

                Enumeration<LedgerEntry> entries = new Enumeration<LedgerEntry>() {
                        @Override
                        public boolean hasMoreElements() {
                            return !seq.isEmpty();
                        }

                        @Override
                        public LedgerEntry nextElement() {
                            return seq.remove();
                        }
                    };
                return FutureUtils.value(entries);
            }).whenCompleteAsync((res, exception) -> {
                    if (exception != null) {
                        cb.readComplete(PulsarMockBookKeeper.getExceptionCode(exception),
                                PulsarMockLedgerHandle.this, null, ctx);
                    } else {
                        cb.readComplete(BKException.Code.OK, PulsarMockLedgerHandle.this, res, ctx);
                    }
                }, bk.executor);
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
        bk.getAddEntryFailure().thenComposeAsync((res) -> {
                Long delayMillis = bk.addEntryDelaysMillis.poll();
                if (delayMillis == null) {
                    delayMillis = 1L;
                }

                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                }

                if (fenced) {
                    return FutureUtils.exception(new BKException.BKLedgerFencedException());
                } else {
                    lastEntry = entries.size();
                    byte[] storedData = new byte[data.readableBytes()];
                    data.readBytes(storedData);
                    entries.add(LedgerEntryImpl.create(ledgerId, lastEntry,
                                                       storedData.length, Unpooled.wrappedBuffer(storedData)));
                    return FutureUtils.value(lastEntry);
                }

            }, bk.executor).whenCompleteAsync((entryId, exception) -> {
                    data.release();
                    if (exception != null) {
                        fenced = true;
                        cb.addComplete(PulsarMockBookKeeper.getExceptionCode(exception),
                                       PulsarMockLedgerHandle.this, LedgerHandle.INVALID_ENTRY_ID, ctx);
                    } else {
                        Long responseDelayMillis = bk.addEntryResponseDelaysMillis.poll();
                        if (responseDelayMillis != null) {
                            try {
                                Thread.sleep(responseDelayMillis);
                            } catch (InterruptedException e) {
                            }
                        }
                        cb.addComplete(BKException.Code.OK, PulsarMockLedgerHandle.this, entryId, ctx);
                    }
                }, bk.executor);
    }

    @Override
    public long getId() {
        return ledgerId;
    }

    @Override
    public long getLastAddConfirmed() {
        if (bk.checkReturnEmptyLedger()) {
            return -1;
        } else {
            return lastEntry;
        }
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

    private static LedgerMetadata createMetadata(long id, DigestType digest, byte[] passwd) {
        List<BookieId> ensemble = new ArrayList<>(PulsarMockBookKeeper.getMockEnsemble());
        return LedgerMetadataBuilder.create()
            .withDigestType(digest.toApiDigestType())
            .withPassword(passwd)
            .withId(id)
            .newEnsembleEntry(0L, ensemble)
            .build();
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarMockLedgerHandle.class);

}
