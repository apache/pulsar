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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCounted;

import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.util.AbstractCASReferenceCounted;

public final class EntryImpl extends AbstractCASReferenceCounted implements Entry, Comparable<EntryImpl>, ReferenceCounted {

    private static final Recycler<EntryImpl> RECYCLER = new Recycler<EntryImpl>() {
        @Override
        protected EntryImpl newObject(Handle<EntryImpl> handle) {
            return new EntryImpl(handle);
        }
    };

    private final Handle<EntryImpl> recyclerHandle;
    private long timestamp;
    private long ledgerId;
    private long entryId;
    ByteBuf data;

    public static EntryImpl create(LedgerEntry ledgerEntry) {
        EntryImpl entry = RECYCLER.get();
        entry.timestamp = System.nanoTime();
        entry.ledgerId = ledgerEntry.getLedgerId();
        entry.entryId = ledgerEntry.getEntryId();
        entry.data = ledgerEntry.getEntryBuffer();
        entry.data.retain();
        entry.setRefCnt(1);
        return entry;
    }

    @VisibleForTesting
    public static EntryImpl create(long ledgerId, long entryId, byte[] data) {
        EntryImpl entry = RECYCLER.get();
        entry.timestamp = System.nanoTime();
        entry.ledgerId = ledgerId;
        entry.entryId = entryId;
        entry.data = Unpooled.wrappedBuffer(data);
        entry.setRefCnt(1);
        return entry;
    }

    public static EntryImpl create(long ledgerId, long entryId, ByteBuf data) {
        EntryImpl entry = RECYCLER.get();
        entry.timestamp = System.nanoTime();
        entry.ledgerId = ledgerId;
        entry.entryId = entryId;
        entry.data = data;
        entry.data.retain();
        entry.setRefCnt(1);
        return entry;
    }

    public static EntryImpl create(PositionImpl position, ByteBuf data) {
        EntryImpl entry = RECYCLER.get();
        entry.timestamp = System.nanoTime();
        entry.ledgerId = position.getLedgerId();
        entry.entryId = position.getEntryId();
        entry.data = data;
        entry.data.retain();
        entry.setRefCnt(1);
        return entry;
    }

    public static EntryImpl create(EntryImpl other) {
        EntryImpl entry = RECYCLER.get();
        entry.timestamp = System.nanoTime();
        entry.ledgerId = other.ledgerId;
        entry.entryId = other.entryId;
        entry.data = other.data.retainedDuplicate();
        entry.setRefCnt(1);
        return entry;
    }

    private EntryImpl(Recycler.Handle<EntryImpl> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public long getTimestamp() {
        return timestamp;
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
    public PositionImpl getPosition() {
        return new PositionImpl(ledgerId, entryId);
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
        return ComparisonChain.start().compare(ledgerId, other.ledgerId).compare(entryId, other.entryId).result();
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }

    @Override
    protected void deallocate() {
        // This method is called whenever the ref-count of the EntryImpl reaches 0, so that now we can recycle it
        data.release();
        data = null;
        timestamp = -1;
        ledgerId = -1;
        entryId = -1;
        recyclerHandle.recycle(this);
    }

}
