/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.util.ReferenceCounted;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.RecyclableDuplicateByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

final class EntryImpl implements Entry, Comparable<EntryImpl>, ReferenceCounted {

    private static final Recycler<EntryImpl> RECYCLER = new Recycler<EntryImpl>() {
        @Override
        protected EntryImpl newObject(Handle handle) {
            return new EntryImpl(handle);
        }
    };
    
    private final Handle recyclerHandle;
    private PositionImpl position;
    private ByteBuf data;

    public static EntryImpl create(LedgerEntry ledgerEntry) {
        EntryImpl entry = RECYCLER.get();
        entry.position = new PositionImpl(ledgerEntry.getLedgerId(), ledgerEntry.getEntryId());
        entry.data = ledgerEntry.getEntryBuffer();
        return entry;
    }

    // Used just for tests
    public static EntryImpl create(long ledgerId, long entryId, byte[] data) {
        EntryImpl entry = RECYCLER.get();
        entry.position = new PositionImpl(ledgerId, entryId);
        entry.data = Unpooled.wrappedBuffer(data);
        return entry;
    }

    public static EntryImpl create(long ledgerId, long entryId, ByteBuf data) {
        EntryImpl entry = RECYCLER.get();
        entry.position = new PositionImpl(ledgerId, entryId);
        entry.data = data;
        return entry;
    }

    public static EntryImpl create(PositionImpl position, ByteBuf data) {
        EntryImpl entry = RECYCLER.get();
        entry.position = position;
        entry.data = data;
        return entry;
    }

    public static EntryImpl create(EntryImpl other) {
        EntryImpl entry = RECYCLER.get();
        entry.position = new PositionImpl(other.position);
        entry.data = RecyclableDuplicateByteBuf.create(other.data);
        return entry;
    }

    private EntryImpl(Recycler.Handle recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    @Override
    public ByteBuf getDataBuffer() {
        return data;
    }

    @Override
    public byte[] getData() {
        byte[] array = new byte[(int) data.readableBytes()];
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
        return position;
    }

    @Override
    public int compareTo(EntryImpl other) {
        return position.compareTo(other.getPosition());
    }

    @Override
    public void retain() {
        data.retain();
    }

    @Override
    public void release() {
        if(data.release()) {
            // recycle only if data buf is released and no other object will use the data 
            recycle();    
        }
    }

    @Override
    public void recycle() {
        this.data = null;
        this.position = null;
        if (recyclerHandle != null) {
            RECYCLER.recycle(this, recyclerHandle);
        }
    }
}
