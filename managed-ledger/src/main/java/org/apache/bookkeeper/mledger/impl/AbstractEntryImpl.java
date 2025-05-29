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

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.util.AbstractCASReferenceCounted;

public abstract class AbstractEntryImpl<T extends AbstractEntryImpl<T>> extends AbstractCASReferenceCounted
        implements Entry, Comparable<T> {
    protected final Recycler.Handle<T> recyclerHandle;
    protected long timestamp;
    protected long ledgerId;
    protected long entryId;
    private ByteBuf data;
    private int length = -1;
    private Position position;
    private Runnable onDeallocate;

    public AbstractEntryImpl(Recycler.Handle<T> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public ByteBuf getDataBuffer() {
        return data;
    }

    protected void setDataBuffer(ByteBuf data) {
        this.data = data;
        this.length = data.readableBytes();
    }

    @Override
    public byte[] getData() {
        ByteBuf data = getDataBuffer().duplicate();
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
        if (length == -1) {
            throw new IllegalStateException("Entry has no length. Call setDataBuffer to set the data buffer first.");
        }
        return length;
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
    public int compareTo(T other) {
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
    protected final void deallocate() {
        beforeDeallocate();
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
        length = -1;
        timestamp = -1;
        ledgerId = -1;
        entryId = -1;
        position = null;
        beforeRecycle();
        recyclerHandle.recycle(self());
    }

    /**
     * This method is called just before the object is deallocated.
     * Subclasses can override this method to run actions before the fields
     * of the object are cleared and the object gets recycled.
     */
    protected void beforeDeallocate() {
        // No-op
    }

    /**
     * This method is called just before the object is recycled. Subclasses can override this methods to cleanup
     * the object before it is returned to the pool.
     */
    protected void beforeRecycle() {
        // No-op
    }

    @SuppressWarnings("unchecked")
    protected T self() {
        return (T) this;
    }
}
