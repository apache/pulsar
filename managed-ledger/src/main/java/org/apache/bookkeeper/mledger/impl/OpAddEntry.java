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

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;


/**
 * Handles the life-cycle of an addEntry() operation.
 *
 */
@Slf4j
public class OpAddEntry implements AddCallback, CloseCallback, Runnable {
    protected ManagedLedgerImpl ml;
    LedgerHandle ledger;
    long entryId;
    private int numberOfMessages;

    @SuppressWarnings("unused")
    private static final AtomicReferenceFieldUpdater<OpAddEntry, AddEntryCallback> callbackUpdater =
            AtomicReferenceFieldUpdater.newUpdater(OpAddEntry.class, AddEntryCallback.class, "callback");
    protected volatile AddEntryCallback callback;
    Object ctx;
    volatile long addOpCount;
    private static final AtomicLongFieldUpdater<OpAddEntry> ADD_OP_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(OpAddEntry.class, "addOpCount");
    private boolean closeWhenDone;
    private long startTime;
    volatile long lastInitTime;
    @SuppressWarnings("unused")
    ByteBuf data;
    private int dataLength;
    private ManagedLedgerInterceptor.PayloadProcessorHandle payloadProcessorHandle = null;

    private static final AtomicReferenceFieldUpdater<OpAddEntry, OpAddEntry.State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(OpAddEntry.class, OpAddEntry.State.class, "state");
    volatile State state;

    @Setter
    private AtomicBoolean timeoutTriggered;

    enum State {
        OPEN,
        INITIATED,
        COMPLETED,
        CLOSED
    }

    public static OpAddEntry createNoRetainBuffer(ManagedLedgerImpl ml, ByteBuf data, AddEntryCallback callback,
                                                  Object ctx, AtomicBoolean timeoutTriggered) {
        OpAddEntry op = createOpAddEntryNoRetainBuffer(ml, data, callback, ctx, timeoutTriggered);
        if (log.isDebugEnabled()) {
            log.debug("Created new OpAddEntry {}", op);
        }
        return op;
    }

    public static OpAddEntry createNoRetainBuffer(ManagedLedgerImpl ml, ByteBuf data, int numberOfMessages,
                                                  AddEntryCallback callback, Object ctx,
                                                  AtomicBoolean timeoutTriggered) {
        OpAddEntry op = createOpAddEntryNoRetainBuffer(ml, data, callback, ctx, timeoutTriggered);
        op.numberOfMessages = numberOfMessages;
        if (log.isDebugEnabled()) {
            log.debug("Created new OpAddEntry {}", op);
        }
        return op;
    }

    private static OpAddEntry createOpAddEntryNoRetainBuffer(ManagedLedgerImpl ml, ByteBuf data,
                                                             AddEntryCallback callback, Object ctx,
                                                             AtomicBoolean timeoutTriggered) {
        OpAddEntry op = RECYCLER.get();
        op.ml = ml;
        op.ledger = null;
        op.data = data;
        op.dataLength = data.readableBytes();
        op.callback = callback;
        op.ctx = ctx;
        op.addOpCount = ManagedLedgerImpl.ADD_OP_COUNT_UPDATER.incrementAndGet(ml);
        op.closeWhenDone = false;
        op.entryId = -1;
        op.startTime = System.nanoTime();
        op.state = State.OPEN;
        op.payloadProcessorHandle = null;
        op.timeoutTriggered = timeoutTriggered;
        ml.mbean.addAddEntrySample(op.dataLength);
        return op;
    }

    public void setLedger(LedgerHandle ledger) {
        this.ledger = ledger;
    }

    public void setCloseWhenDone(boolean closeWhenDone) {
        this.closeWhenDone = closeWhenDone;
    }

    public void initiate() {
        if (STATE_UPDATER.compareAndSet(OpAddEntry.this, State.OPEN, State.INITIATED)) {
            ByteBuf duplicateBuffer = data.retainedDuplicate();

            // internally asyncAddEntry() will take the ownership of the buffer and release it at the end
            addOpCount = ManagedLedgerImpl.ADD_OP_COUNT_UPDATER.incrementAndGet(ml);
            lastInitTime = System.nanoTime();
            if (ml.getManagedLedgerInterceptor() != null) {
                long originalDataLen = data.readableBytes();
                try {
                    payloadProcessorHandle = ml.getManagedLedgerInterceptor()
                            .processPayloadBeforeLedgerWrite(this, duplicateBuffer);
                } catch (Exception e) {
                    ml.pendingAddEntries.remove(this);
                    ReferenceCountUtil.safeRelease(duplicateBuffer);
                    log.error("[{}] Error processing payload before ledger write", ml.getName(), e);
                    this.failed(new ManagedLedgerException.ManagedLedgerInterceptException(e));
                    return;
                }
                if (payloadProcessorHandle != null) {
                    duplicateBuffer = payloadProcessorHandle.getProcessedPayload();
                    // If data len of entry changes, correct "dataLength" and "currentLedgerSize".
                    if (originalDataLen != duplicateBuffer.readableBytes()) {
                        this.dataLength = duplicateBuffer.readableBytes();
                        this.ml.currentLedgerSize += (dataLength - originalDataLen);
                    }
                }
            }
            ledger.asyncAddEntry(duplicateBuffer, this, addOpCount);
        } else {
            log.warn("[{}] initiate with unexpected state {}, expect OPEN state.", ml.getName(), state);
        }
    }

    public void initiateShadowWrite() {
        if (STATE_UPDATER.compareAndSet(OpAddEntry.this, State.OPEN, State.INITIATED)) {
            addOpCount = ManagedLedgerImpl.ADD_OP_COUNT_UPDATER.incrementAndGet(ml);
            lastInitTime = System.nanoTime();
            //Use entryId in PublishContext and call addComplete directly.
            this.addComplete(BKException.Code.OK, ledger, ((Position) ctx).getEntryId(), addOpCount);
        } else {
            log.warn("[{}] initiate with unexpected state {}, expect OPEN state.", ml.getName(), state);
        }
    }

    public void failed(ManagedLedgerException e) {
        AddEntryCallback cb = callbackUpdater.getAndSet(this, null);
        ml.afterFailedAddEntry(this.getNumberOfMessages());
        if (cb != null) {
            ReferenceCountUtil.release(data);
            cb.addFailed(e, ctx);
            ml.mbean.recordAddEntryError();
            if (payloadProcessorHandle != null) {
                payloadProcessorHandle.release();
            }
        }
    }

    @Override
    public void addComplete(int rc, final LedgerHandle lh, long entryId, Object ctx) {
        if (!STATE_UPDATER.compareAndSet(OpAddEntry.this, State.INITIATED, State.COMPLETED)) {
            log.warn("[{}] The add op is terminal legacy callback for entry {}-{} adding.", ml.getName(), lh.getId(),
                    entryId);
            // Since there is a thread is coping this object, do not recycle this object to avoid other problems.
            // For example: we recycled this object, other thread get a null "opAddEntry.{variable_name}".
            // Recycling is not mandatory, JVM GC will collect it.
            return;
        }

        if (ledger != null && lh != null) {
            if (ledger.getId() != lh.getId()) {
                log.warn("[{}] ledgerId {} doesn't match with acked ledgerId {}", ml.getName(), ledger.getId(),
                        lh.getId());
            }
            checkArgument(ledger.getId() == lh.getId(), "ledgerId %s doesn't match with acked ledgerId %s",
                    ledger.getId(), lh.getId());
        }

        if (!checkAndCompleteOp(ctx)) {
            // means callback might have been completed by different thread (timeout task thread).. so do nothing
            return;
        }

        this.entryId = entryId;
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] write-complete: ledger-id={} entry-id={} size={} rc={}", this, ml.getName(),
                    lh == null ? -1 : lh.getId(), entryId, dataLength, rc);
        }

        if (rc != BKException.Code.OK || timeoutTriggered.get()) {
            handleAddFailure(lh);
        } else {
            // Trigger addComplete callback in a thread hashed on the managed ledger name
            ml.getExecutor().execute(this);
        }
    }

    // Called in executor hashed on managed ledger name, once the add operation is complete
    @Override
    public void run() {
        if (payloadProcessorHandle != null) {
            payloadProcessorHandle.release();
        }
        // Remove this entry from the head of the pending queue
        OpAddEntry firstInQueue = ml.pendingAddEntries.poll();
        if (firstInQueue == null) {
            return;
        }
        if (this != firstInQueue) {
            firstInQueue.failed(new ManagedLedgerException("Unexpected add entry op when complete the add entry op."));
            return;
        }

        ManagedLedgerImpl.NUMBER_OF_ENTRIES_UPDATER.incrementAndGet(ml);
        ManagedLedgerImpl.TOTAL_SIZE_UPDATER.addAndGet(ml, dataLength);

        long ledgerId = ledger != null ? ledger.getId() : ((Position) ctx).getLedgerId();
        if (ml.hasActiveCursors()) {
            // Avoid caching entries if no cursor has been created
            EntryImpl entry = EntryImpl.create(ledgerId, entryId, data);
            // EntryCache.insert: duplicates entry by allocating new entry and data. so, recycle entry after calling
            // insert
            ml.entryCache.insert(entry);
            entry.release();
        }

        PositionImpl lastEntry = PositionImpl.get(ledgerId, entryId);
        ManagedLedgerImpl.ENTRIES_ADDED_COUNTER_UPDATER.incrementAndGet(ml);
        ml.lastConfirmedEntry = lastEntry;

        if (closeWhenDone) {
            log.info("[{}] Closing ledger {} for being full", ml.getName(), ledgerId);
            // `data` will be released in `closeComplete`
            if (ledger != null) {
                ledger.asyncClose(this, ctx);
            }
        } else {
            updateLatency();
            AddEntryCallback cb = callbackUpdater.getAndSet(this, null);
            if (cb != null) {
                cb.addComplete(lastEntry, data.asReadOnly(), ctx);
                ml.notifyCursors();
                ml.notifyWaitingEntryCallBacks();
                ReferenceCountUtil.release(data);
                this.recycle();
            } else {
                ReferenceCountUtil.release(data);
            }
        }
    }

    @Override
    public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
        checkArgument(ledger.getId() == lh.getId(), "ledgerId %s doesn't match with acked ledgerId %s", ledger.getId(),
                lh.getId());

        if (rc == BKException.Code.OK) {
            log.debug("Successfully closed ledger {}", lh.getId());
        } else {
            log.warn("Error when closing ledger {}. Status={}", lh.getId(), BKException.getMessage(rc));
        }

        ml.ledgerClosed(lh);
        updateLatency();

        AddEntryCallback cb = callbackUpdater.getAndSet(this, null);
        if (cb != null) {
            cb.addComplete(PositionImpl.get(lh.getId(), entryId), data.asReadOnly(), ctx);
            ml.notifyCursors();
            ml.notifyWaitingEntryCallBacks();
            ReferenceCountUtil.release(data);
            this.recycle();
        } else {
            ReferenceCountUtil.release(data);
        }
    }

    private void updateLatency() {
        ml.mbean.addAddEntryLatencySample(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        ml.mbean.addLedgerAddEntryLatencySample(System.nanoTime() - lastInitTime, TimeUnit.NANOSECONDS);
    }

    /**
     * Checks if add-operation is completed.
     *
     * @return true if task is not already completed else returns false.
     */
    private boolean checkAndCompleteOp(Object ctx) {
        long addOpCount = (ctx instanceof Long) ? (long) ctx : -1;
        if (addOpCount != -1 && ADD_OP_COUNT_UPDATER.compareAndSet(this, addOpCount, -1)) {
            return true;
        }
        log.info("Add-entry already completed for {}-{}", ledger != null ? ledger.getId() : -1, entryId);
        return false;
    }

    /**
     * It handles add failure on the given ledger. it can be triggered when add-entry fails or times out.
     *
     * @param lh
     */
    void handleAddFailure(final LedgerHandle lh) {
        // If we get a write error, we will try to create a new ledger and re-submit the pending writes. If the
        // ledger creation fails (persistent bk failure, another instance owning the ML, ...), then the writes will
        // be marked as failed.
        ManagedLedgerImpl finalMl = this.ml;
        finalMl.mbean.recordAddEntryError();

        finalMl.getExecutor().execute(() -> {
            // Force the creation of a new ledger. Doing it in a background thread to avoid acquiring ML lock
            // from a BK callback.
            finalMl.ledgerClosed(lh);
        });
    }

    OpAddEntry duplicateAndClose(AtomicBoolean timeoutTriggered) {
        STATE_UPDATER.set(OpAddEntry.this, State.CLOSED);
        OpAddEntry duplicate =
                OpAddEntry.createNoRetainBuffer(ml, data, getNumberOfMessages(), callback, ctx, timeoutTriggered);
        return duplicate;
    }

    public State getState() {
        return state;
    }

    public ByteBuf getData() {
        return data;
    }

    public int getNumberOfMessages() {
        return numberOfMessages;
    }

    public Object getCtx() {
        return ctx;
    }

    public void setNumberOfMessages(int numberOfMessages) {
        this.numberOfMessages = numberOfMessages;
    }

    public void setData(ByteBuf data) {
        this.dataLength = data.readableBytes();
        this.data = data;
    }

    private final Handle<OpAddEntry> recyclerHandle;

    private OpAddEntry(Handle<OpAddEntry> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<OpAddEntry> RECYCLER = new Recycler<OpAddEntry>() {
        @Override
        protected OpAddEntry newObject(Recycler.Handle<OpAddEntry> recyclerHandle) {
            return new OpAddEntry(recyclerHandle);
        }
    };

    public void recycle() {
        ml = null;
        ledger = null;
        data = null;
        numberOfMessages = 0;
        dataLength = -1;
        callback = null;
        ctx = null;
        addOpCount = -1;
        closeWhenDone = false;
        entryId = -1;
        startTime = -1;
        lastInitTime = -1;
        payloadProcessorHandle = null;
        timeoutTriggered = null;
        recyclerHandle.recycle(this);
    }

    @Override
    public String toString() {
        ManagedLedgerImpl ml = this.ml;
        LedgerHandle ledger = this.ledger;
        return "OpAddEntry{"
                + "mlName=" + (ml != null ? ml.getName() : "null")
                + ", ledgerId=" + (ledger != null ? String.valueOf(ledger.getId()) : "null")
                + ", entryId=" + entryId
                + ", startTime=" + startTime
                + ", dataLength=" + dataLength
                + '}';
    }
}
