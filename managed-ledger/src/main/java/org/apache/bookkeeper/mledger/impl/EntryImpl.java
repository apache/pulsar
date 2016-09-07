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

final class EntryImpl implements Entry, Comparable<EntryImpl>, ReferenceCounted {

    private final PositionImpl position;
    private final ByteBuf data;

    EntryImpl(LedgerEntry ledgerEntry) {
        this.position = new PositionImpl(ledgerEntry.getLedgerId(), ledgerEntry.getEntryId());
        this.data = ledgerEntry.getEntryBuffer();
    }

    // Used just for tests
    EntryImpl(long ledgerId, long entryId, byte[] data) {
        this.position = new PositionImpl(ledgerId, entryId);
        this.data = Unpooled.wrappedBuffer(data);
    }

    EntryImpl(long ledgerId, long entryId, ByteBuf data) {
        this.position = new PositionImpl(ledgerId, entryId);
        this.data = data;
    }

    EntryImpl(PositionImpl position, ByteBuf data) {
        this.position = position;
        this.data = data;
    }

    EntryImpl(EntryImpl other) {
        this.position = new PositionImpl(other.position);
        this.data = RecyclableDuplicateByteBuf.create(other.data);
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

    public void retain() {
        data.retain();
    }

    @Override
    public void release() {
        data.release();
    }

    public int refCnt() {
        return data.refCnt();
    }
}
