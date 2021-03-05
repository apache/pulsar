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

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.NestedPositionInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;

public class PositionImpl implements Position, Comparable<PositionImpl> {

    protected long ledgerId;
    protected long entryId;
    protected long[] ackSet;

    public static final PositionImpl earliest = new PositionImpl(-1, -1);
    public static final PositionImpl latest = new PositionImpl(Long.MAX_VALUE, Long.MAX_VALUE);

    public PositionImpl(PositionInfo pi) {
        this.ledgerId = pi.getLedgerId();
        this.entryId = pi.getEntryId();
    }

    public PositionImpl(NestedPositionInfo npi) {
        this.ledgerId = npi.getLedgerId();
        this.entryId = npi.getEntryId();
    }

    public PositionImpl(long ledgerId, long entryId) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
    }

    public PositionImpl(long ledgerId, long entryId, long[] ackSet) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.ackSet = ackSet;
    }

    public PositionImpl(PositionImpl other) {
        this.ledgerId = other.ledgerId;
        this.entryId = other.entryId;
    }

    public static PositionImpl get(long ledgerId, long entryId) {
        return new PositionImpl(ledgerId, entryId);
    }

    public static PositionImpl get(long ledgerId, long entryId, long[] ackSet) {
        return new PositionImpl(ledgerId, entryId, ackSet);
    }

    public static PositionImpl get(PositionImpl other) {
        return new PositionImpl(other);
    }

    public long[] getAckSet() {
        return ackSet;
    }

    public void setAckSet(long[] ackSet) {
        this.ackSet = ackSet;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    @Override
    public PositionImpl getNext() {
        if (entryId < 0) {
            return PositionImpl.get(ledgerId, 0);
        } else {
            return PositionImpl.get(ledgerId, entryId + 1);
        }
    }

    /**
     * String representation of virtual cursor - LedgerId:EntryId.
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

    public boolean hasAckSet() {
        return ackSet != null;
    }

    public PositionInfo getPositionInfo() {
        return PositionInfo.newBuilder().setLedgerId(ledgerId).setEntryId(entryId).build();
    }
}
