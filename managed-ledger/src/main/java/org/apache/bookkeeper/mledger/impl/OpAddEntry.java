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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.RecyclableDuplicateByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

/**
 * Handles the life-cycle of an addEntry() operation
 *
 */
class OpAddEntry extends SafeRunnable implements AddCallback, CloseCallback {
    private ManagedLedgerImpl ml;
    private LedgerHandle ledger;
    private long entryId;

    @SuppressWarnings("unused")
    private volatile AddEntryCallback callback;
    private Object ctx;
    private boolean closeWhenDone;
    private long startTime;
    ByteBuf data;
    private int dataLength;

    private static final AtomicReferenceFieldUpdater<OpAddEntry, AddEntryCallback> callbackUpdater = AtomicReferenceFieldUpdater
            .newUpdater(OpAddEntry.class, AddEntryCallback.class, "callback");

    public static OpAddEntry create(ManagedLedgerImpl ml, ByteBuf data, AddEntryCallback callback, Object ctx) {
        OpAddEntry op = RECYCLER.get();
        op.ml = ml;
        op.ledger = null;
        op.data = data.retain();
        op.dataLength = data.readableBytes();
        op.callback = callback;
        op.ctx = ctx;
        op.closeWhenDone = false;
        op.entryId = -1;
        op.startTime = System.nanoTime();
        ml.mbean.addAddEntrySample(op.dataLength);
        if (log.isDebugEnabled()) {
            log.debug("Created new OpAddEntry {}", op);
        }
        return op;
    }

    public void setLedger(LedgerHandle ledger) {
        this.ledger = ledger;
    }

    public void setCloseWhenDone(boolean closeWhenDone) {
        this.closeWhenDone = closeWhenDone;
    }

    public void initiate() {
        ByteBuf duplicateBuffer = RecyclableDuplicateByteBuf.create(data);
        // duplicatedBuffer has refCnt=1 at this point

        ledger.asyncAddEntry(duplicateBuffer, this, ctx);

        // Internally, asyncAddEntry() is refCnt neutral to respect to the passed buffer and it will keep a ref on it
        // until is done using it. We need to release this buffer here to balance the 1 refCnt added at the creation
        // time.
        duplicateBuffer.release();
    }

    public void failed(ManagedLedgerException e) {
        AddEntryCallback cb = callbackUpdater.getAndSet(this, null);
        if (cb != null) {
            cb.addFailed(e, ctx);
            ml.mbean.recordAddEntryError();
        }
    }

    @Override
    public void addComplete(int rc, final LedgerHandle lh, long entryId, Object ctx) {
        checkArgument(ledger.getId() == lh.getId());
        checkArgument(this.ctx == ctx);

        this.entryId = entryId;
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] write-complete: ledger-id={} entry-id={} size={} rc={}", this, ml.getName(),
                    lh.getId(), entryId, dataLength, rc);
        }

        if (rc != BKException.Code.OK) {
            // If we get a write error, we will try to create a new ledger and re-submit the pending writes. If the
            // ledger creation fails (persistent bk failure, another instanche owning the ML, ...), then the writes will
            // be marked as failed.
            ml.mbean.recordAddEntryError();

            ml.getExecutor().submitOrdered(ml.getName(), SafeRun.safeRun(() -> {
                // Force the creation of a new ledger. Doing it in a background thread to avoid acquiring ML lock
                // from a BK callback.
                ml.ledgerClosed(lh);
            }));
        } else {
            // Trigger addComplete callback in a thread hashed on the managed ledger name
            ml.getExecutor().submitOrdered(ml.getName(), this);
        }
    }

    // Called in exector hashed on managed ledger name, once the add operation is complete
    @Override
    public void safeRun() {
        // Remove this entry from the head of the pending queue
        OpAddEntry firstInQueue = ml.pendingAddEntries.poll();
        checkArgument(this == firstInQueue);

        ManagedLedgerImpl.NUMBER_OF_ENTRIES_UPDATER.incrementAndGet(ml);
        ManagedLedgerImpl.TOTAL_SIZE_UPDATER.addAndGet(ml, dataLength);
        if (ml.hasActiveCursors()) {
            // Avoid caching entries if no cursor has been created
            EntryImpl entry = EntryImpl.create(ledger.getId(), entryId, data);
            // EntryCache.insert: duplicates entry by allocating new entry and data. so, recycle entry after calling
            // insert
            ml.entryCache.insert(entry);
            entry.release();
        }

        // We are done using the byte buffer
        data.release();

        PositionImpl lastEntry = PositionImpl.get(ledger.getId(), entryId);
        ManagedLedgerImpl.ENTRIES_ADDED_COUNTER_UPDATER.incrementAndGet(ml);
        ml.lastConfirmedEntry = lastEntry;

        if (closeWhenDone) {
            log.info("[{}] Closing ledger {} for being full", ml.getName(), ledger.getId());
            ledger.asyncClose(this, ctx);
        } else {
            updateLatency();
            AddEntryCallback cb = callbackUpdater.getAndSet(this, null);
            if (cb != null) {
                cb.addComplete(PositionImpl.get(ledger.getId(), entryId), ctx);
                ml.notifyCursors();
                this.recycle();
            }
        }
    }

    @Override
    public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
        checkArgument(ledger.getId() == lh.getId());

        if (rc == BKException.Code.OK) {
            log.debug("Successfuly closed ledger {}", lh.getId());
        } else {
            log.warn("Error when closing ledger {}. Status={}", lh.getId(), BKException.getMessage(rc));
        }

        ml.ledgerClosed(lh);
        updateLatency();

        AddEntryCallback cb = callbackUpdater.getAndSet(this, null);
        if (cb != null) {
            cb.addComplete(PositionImpl.get(lh.getId(), entryId), ctx);
            ml.notifyCursors();
            this.recycle();
        }
    }

    private void updateLatency() {
        ml.mbean.addAddEntryLatencySample(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    }

    private final Handle recyclerHandle;

    private OpAddEntry(Handle recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<OpAddEntry> RECYCLER = new Recycler<OpAddEntry>() {
        protected OpAddEntry newObject(Recycler.Handle recyclerHandle) {
            return new OpAddEntry(recyclerHandle);
        }
    };

    public void recycle() {
        ml = null;
        ledger = null;
        data = null;
        dataLength = -1;
        callback = null;
        ctx = null;
        closeWhenDone = false;
        entryId = -1;
        startTime = -1;
        RECYCLER.recycle(this, recyclerHandle);
    }

    private static final Logger log = LoggerFactory.getLogger(OpAddEntry.class);
}
