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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import lombok.CustomLog;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.EntryReadCountHandler;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReferenceCountedEntry;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.util.AbstractCASReferenceCounted;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

@CustomLog
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
    private static final VarHandle READ_COUNT_HANDLER;

    static {
        try {
            READ_COUNT_HANDLER = MethodHandles.lookup()
                    .findVarHandle(EntryImpl.class, "readCountHandler", EntryReadCountHandler.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private boolean decreaseReadCountOnRelease = true;
    // Cache readers publish metadata lazily; entry copies must see a fully initialized instance.
    @Getter @Setter
    private volatile MessageMetadata messageMetadata;
    private boolean messageMetadataInitializationFailed;

    private Runnable onDeallocate;

    public static EntryImpl create(LedgerEntry ledgerEntry, int expectedReadCount) {
        EntryImpl entry = RECYCLER.get();
        entry.ledgerId = ledgerEntry.getLedgerId();
        entry.entryId = ledgerEntry.getEntryId();
        entry.data = ledgerEntry.getEntryBuffer();
        entry.data.retain();
        entry.readCountHandler = EntryReadCountHandlerImpl.maybeCreate(expectedReadCount);
        // Reset the lazily-cached position LAST, after the id assignments: a recycled object can
        // carry a stale Position materialized by a getPosition() call that raced past the recycle
        // (deallocation nulls the field, but a late reader re-materializes it from the reset ids
        // as (-1, -1)), and any racy lazy rebuild must observe the fresh legitimate ids.
        entry.position = null;
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
        // Reset the lazily-cached position: see create(LedgerEntry, int).
        entry.position = null;
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
        // Reset the lazily-cached position: see create(LedgerEntry, int).
        entry.position = null;
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
                                                        EntryReadCountHandler entryReadCountHandler,
                                                        MessageMetadata messageMetadata) {
        EntryImpl entry = RECYCLER.get();
        entry.position = PositionFactory.create(position);
        entry.ledgerId = position.getLedgerId();
        entry.entryId = position.getEntryId();
        entry.data = data.retainedDuplicate();
        entry.readCountHandler = entryReadCountHandler;
        entry.messageMetadata = messageMetadata;
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
        entry.readCountHandler = other.getReadCountHandler();
        entry.messageMetadata = other.messageMetadata;
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
        entry.messageMetadata = other.getMessageMetadata();
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
        EntryReadCountHandler handler = getReadCountHandler();
        if (decreaseReadCountOnRelease && handler != null) {
            handler.markRead();
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
        messageMetadata = null;
        messageMetadataInitializationFailed = false;
        recyclerHandle.recycle(this);
    }

    @Override
    public boolean matchesPosition(Position key) {
        return key != null && key.compareTo(ledgerId, entryId) == 0;
    }

    @Override
    public EntryReadCountHandler getReadCountHandler() {
        // a cached entry can take a later addition's handler, see updateExpectedReadCount
        return (EntryReadCountHandler) READ_COUNT_HANDLER.getAcquire(this);
    }

    /**
     * Takes the read count handler of an entry that was added at this entry's position while this one is cached.
     * A cached entry's data is immutable and kept, but its expected read count is the latest addition's, such as a
     * read from storage, which knows how many cursors are expected to read the entry, also when it has none. The
     * cached entry shares the latest addition's handler, as an inserted entry does, so that the reads of the entries
     * that the addition returned, counted when they're released, count for the cached entry too.
     *
     * @param latest the read count handler of the latest addition, or null when it has no expected reads
     */
    public void updateExpectedReadCount(EntryReadCountHandler latest) {
        READ_COUNT_HANDLER.setRelease(this, latest);
    }

    public void setDecreaseReadCountOnRelease(boolean enabled) {
        decreaseReadCountOnRelease = enabled;
    }

    public synchronized void initializeMessageMetadataIfNeeded(String managedLedgerName) {
        if (messageMetadata == null && !messageMetadataInitializationFailed) {
            try {
                MessageMetadata msgMetadata = new MessageMetadata();
                Commands.parseMessageMetadata(data.duplicate(), msgMetadata);
                // Copies of this entry share the instance across threads. Decode its lazily decoded fields now, so
                // that readers don't decode them concurrently: a lazily decoded field is cached in a plain field, and
                // LightProto creates an ASCII string without a constructor, so another thread could see the cached
                // string before its contents. The volatile write below publishes the decoded fields.
                msgMetadata.materialize();
                this.messageMetadata = msgMetadata;
            } catch (Throwable t) {
                // The entry bytes are immutable; another cache reader cannot make a failed parse succeed.
                messageMetadataInitializationFailed = true;
                log.warn().attr("managedLedgerName", managedLedgerName)
                        .attr("ledgerId", ledgerId)
                        .attr("entryId", entryId)
                        .exception(t)
                        .log("Failed to parse message metadata for entry");
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getName() + "@" + System.identityHashCode(this)
                + "{ledgerId=" + ledgerId + ", entryId=" + entryId + '}';
    }
}