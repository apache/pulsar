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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCounted;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.EntryReadCountHandler;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReferenceCountedEntry;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.util.AbstractCASReferenceCounted;

public final class EntryImpl extends AbstractCASReferenceCounted
        implements ReferenceCountedEntry, Comparable<EntryImpl> {

    private static final Recycler<EntryImpl> RECYCLER = new Recycler<EntryImpl>() {
        @Override
        protected EntryImpl newObject(Handle<EntryImpl> handle) {
            return new EntryImpl(handle);
        }
    };

    private final Handle<EntryImpl> recyclerHandle;
    private long ledgerId;
    private long entryId;
    private Position position;
    ByteBuf data;
    private EntryReadCountHandler readCountHandler;
    private boolean decreaseReadCountOnRelease = true;

    private Runnable onDeallocate;

    public static EntryImpl create(LedgerEntry ledgerEntry, int expectedReadCount) {
        EntryImpl entry = RECYCLER.get();
        entry.ledgerId = ledgerEntry.getLedgerId();
        entry.entryId = ledgerEntry.getEntryId();
        entry.data = ledgerEntry.getEntryBuffer();
        entry.data.retain();
        entry.readCountHandler = EntryReadCountHandlerImpl.maybeCreate(expectedReadCount);
        entry.setRefCnt(1);
        return entry;
    }

    public static EntryImpl create(LedgerEntry ledgerEntry, ManagedLedgerInterceptor interceptor,
                                   int expectedReadCount) {
        ManagedLedgerInterceptor.PayloadProcessorHandle processorHandle = null;
        if (interceptor != null) {
            ByteBuf duplicateBuffer = ledgerEntry.getEntryBuffer().retainedDuplicate();
            processorHandle = interceptor
                    .processPayloadBeforeEntryCache(duplicateBuffer);
            if (processorHandle != null) {
                ledgerEntry  = LedgerEntryImpl.create(ledgerEntry.getLedgerId(), ledgerEntry.getEntryId(),
                        ledgerEntry.getLength(), processorHandle.getProcessedPayload());
            } else {
                duplicateBuffer.release();
            }
        }
        EntryImpl returnEntry = create(ledgerEntry, expectedReadCount);
        if (processorHandle != null) {
            processorHandle.release();
            ledgerEntry.close();
        }
        return returnEntry;
    }

    @VisibleForTesting
    public static EntryImpl create(long ledgerId, long entryId, byte[] data) {
        return create(ledgerId, entryId, data, 0);
    }

    @VisibleForTesting
    public static EntryImpl create(long ledgerId, long entryId, byte[] data, int expectedReadCount) {
        EntryImpl entry = RECYCLER.get();
        entry.ledgerId = ledgerId;
        entry.entryId = entryId;
        entry.data = Unpooled.wrappedBuffer(data);
        entry.readCountHandler = EntryReadCountHandlerImpl.maybeCreate(expectedReadCount);
        entry.setRefCnt(1);
        return entry;
    }

    public static EntryImpl create(long ledgerId, long entryId, ByteBuf data) {
        return create(ledgerId, entryId, data, 0);
    }

    public static EntryImpl create(long ledgerId, long entryId, ByteBuf data, int expectedReadCount) {
        EntryImpl entry = RECYCLER.get();
        entry.ledgerId = ledgerId;
        entry.entryId = entryId;
        entry.data = data;
        entry.data.retain();
        entry.readCountHandler = EntryReadCountHandlerImpl.maybeCreate(expectedReadCount);
        entry.setRefCnt(1);
        return entry;
    }

    public static EntryImpl create(Position position, ByteBuf data, int expectedReadCount) {
        EntryImpl entry = RECYCLER.get();
        entry.position = PositionFactory.create(position);
        entry.ledgerId = position.getLedgerId();
        entry.entryId = position.getEntryId();
        entry.data = data;
        entry.data.retain();
        entry.readCountHandler = EntryReadCountHandlerImpl.maybeCreate(expectedReadCount);
        entry.setRefCnt(1);
        return entry;
    }

    public static EntryImpl createWithRetainedDuplicate(Position position, ByteBuf data, int expectedReadCount) {
        EntryImpl entry = RECYCLER.get();
        entry.position = PositionFactory.create(position);
        entry.ledgerId = position.getLedgerId();
        entry.entryId = position.getEntryId();
        entry.data = data.retainedDuplicate();
        entry.readCountHandler = EntryReadCountHandlerImpl.maybeCreate(expectedReadCount);
        entry.setRefCnt(1);
        return entry;
    }

    public static EntryImpl createWithRetainedDuplicate(Position position, ByteBuf data,
                                                        EntryReadCountHandler entryReadCountHandler) {
        EntryImpl entry = RECYCLER.get();
        entry.position = PositionFactory.create(position);
        entry.ledgerId = position.getLedgerId();
        entry.entryId = position.getEntryId();
        entry.data = data.retainedDuplicate();
        entry.readCountHandler = entryReadCountHandler;
        entry.setRefCnt(1);
        return entry;
    }

    public static EntryImpl create(EntryImpl other) {
        EntryImpl entry = RECYCLER.get();
        // handle case where other.position is null due to lazy initialization
        entry.position = other.position != null ? PositionFactory.create(other.position) : null;
        entry.ledgerId = other.ledgerId;
        entry.entryId = other.entryId;
        entry.data = other.data.retainedDuplicate();
        entry.readCountHandler = other.readCountHandler;
        entry.setRefCnt(1);
        return entry;
    }

    public static EntryImpl create(Entry other) {
        EntryImpl entry = RECYCLER.get();
        entry.position = PositionFactory.create(other.getPosition());
        entry.ledgerId = other.getLedgerId();
        entry.entryId = other.getEntryId();
        entry.data = other.getDataBuffer().retainedDuplicate();
        entry.readCountHandler = other.getReadCountHandler();
        entry.setRefCnt(1);
        return entry;
    }

    private EntryImpl(Recycler.Handle<EntryImpl> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public void onDeallocate(Runnable r) {
        if (this.onDeallocate == null) {
            this.onDeallocate = r;
        } else {
            // this is not expected to happen
            Runnable previous = this.onDeallocate;
            this.onDeallocate = () -> {
                try {
                    previous.run();
                } finally {
                    r.run();
                }
            };
        }
    }

    @Override
    public ByteBuf getDataBuffer() {
        return data;
    }

    @Override
    public byte[] getData() {
        byte[] array = new byte[data.readableBytes()];
        data.getBytes(data.readerIndex(), array);
        return array;
    }

    // Only for test
    @Override
    public byte[] getDataAndRelease() {
        byte[] array = getData();
        release();
        return array;
    }

    @Override
    public int getLength() {
        return data.readableBytes();
    }

    @Override
    public Position getPosition() {
        if (position == null) {
            position = PositionFactory.create(ledgerId, entryId);
        }
        return position;
    }

    @Override
    public long getLedgerId() {
        return ledgerId;
    }

    @Override
    public long getEntryId() {
        return entryId;
    }

    @Override
    public int compareTo(EntryImpl other) {
        if (this.ledgerId != other.ledgerId) {
            return this.ledgerId < other.ledgerId ? -1 : 1;
        }

        if (this.entryId != other.entryId) {
            return this.entryId < other.entryId ? -1 : 1;
        }

        return 0;
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }

    @Override
    protected void deallocate() {
        if (decreaseReadCountOnRelease && readCountHandler != null) {
            readCountHandler.markRead();
        }
        // This method is called whenever the ref-count of the EntryImpl reaches 0, so that now we can recycle it
        if (onDeallocate != null) {
            try {
                onDeallocate.run();
            } finally {
                onDeallocate = null;
            }
        }
        data.release();
        data = null;
        ledgerId = -1;
        entryId = -1;
        position = null;
        readCountHandler = null;
        decreaseReadCountOnRelease = true;
        recyclerHandle.recycle(this);
    }

    @Override
    public boolean matchesPosition(Position key) {
        return key != null && key.compareTo(ledgerId, entryId) == 0;
    }

    @Override
    public EntryReadCountHandler getReadCountHandler() {
        return readCountHandler;
    }

    public void setDecreaseReadCountOnRelease(boolean enabled) {
        decreaseReadCountOnRelease = enabled;
    }

    @Override
    public String toString() {
        return getClass().getName() + "@" + System.identityHashCode(this)
                + "{ledgerId=" + ledgerId + ", entryId=" + entryId + '}';
    }
}