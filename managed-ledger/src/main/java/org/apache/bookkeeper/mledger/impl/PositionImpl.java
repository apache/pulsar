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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.RangeSet;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.NestedPositionInfo;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

public class PositionImpl implements Position, Comparable<PositionImpl> {

    private long ledgerId;
    private long entryId;

    private final Handle recyclerHandle;

    public PositionImpl(PositionInfo pi) {
        this.ledgerId = pi.getLedgerId();
        this.entryId = pi.getEntryId();
        this.recyclerHandle = null;
    }

    public PositionImpl(NestedPositionInfo npi) {
        this.ledgerId = npi.getLedgerId();
        this.entryId = npi.getEntryId();
        this.recyclerHandle = null;
    }

    public PositionImpl(long ledgerId, long entryId) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.recyclerHandle = null;
    }

    public PositionImpl(PositionImpl other) {
        this.ledgerId = other.ledgerId;
        this.entryId = other.entryId;
        this.recyclerHandle = null;
    }

    private PositionImpl(Handle recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public static PositionImpl get(long ledgerId, long entryId) {
        // PositionImpl position = RECYCLER.get();
        // position.ledgerId = ledgerId;
        // position.entryId = entryId;
        // return position;
        return new PositionImpl(ledgerId, entryId);
    }

    public static PositionImpl get(PositionImpl other) {
        // PositionImpl position = RECYCLER.get();
        // position.ledgerId = other.ledgerId;
        // position.entryId = other.entryId;
        // return position;
        return new PositionImpl(other);
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    @Override
    public PositionImpl getNext() {
        return PositionImpl.get(ledgerId, entryId + 1);
    }

    /**
     * String representation of virtual cursor - LedgerId:EntryId
     */
    @Override
    public String toString() {
        return String.format("%d:%d", ledgerId, entryId);
    }

    @Override
    public int compareTo(PositionImpl other) {
        checkNotNull(other);

        return ComparisonChain.start().compare(this.ledgerId, other.ledgerId).compare(this.entryId, other.entryId)
                .result();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ledgerId, entryId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PositionImpl) {
            PositionImpl other = (PositionImpl) obj;
            return ledgerId == other.ledgerId && entryId == other.entryId;
        }

        return false;
    }

    public PositionInfo getPositionInfo() {
        return PositionInfo.newBuilder().setLedgerId(ledgerId).setEntryId(entryId).build();
    }

    private static final Recycler<PositionImpl> RECYCLER = new Recycler<PositionImpl>() {
        protected PositionImpl newObject(Recycler.Handle handle) {
            return new PositionImpl(handle);
        }
    };

    public void recycle() {
        if (recyclerHandle != null) {
            RECYCLER.recycle(this, recyclerHandle);
        }
    }
}
