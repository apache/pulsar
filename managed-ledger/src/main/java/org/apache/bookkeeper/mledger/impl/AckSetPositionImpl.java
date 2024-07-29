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

import java.util.Optional;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;

/**
 * Position implementation that includes the ack set.
 * Use {@link AckSetStateUtil#createPositionWithAckSet(long, long, long[])} to create instances.
 */
public class AckSetPositionImpl implements Position, AckSetState {
    private final Optional<AckSetState> ackSetStateExtension = Optional.of(this);
    protected final long ledgerId;
    protected final long entryId;
    protected volatile long[] ackSet;

    public AckSetPositionImpl(long ledgerId, long entryId, long[] ackSet) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.ackSet = ackSet;
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
    public Position getNext() {
        if (entryId < 0) {
            return PositionFactory.create(ledgerId, 0);
        } else {
            return PositionFactory.create(ledgerId, entryId + 1);
        }
    }

    @Override
    public String toString() {
        return ledgerId + ":" + entryId + " (ackSet " + (ackSet == null ? "is null" :
                "with long[] size of " + ackSet.length) + ")";
    }

    @Override
    public int hashCode() {
        return hashCodeForPosition();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Position && compareTo((Position) obj) == 0;
    }

    @Override
    public <T> Optional<T> getExtension(Class<T> extensionClass) {
        if (extensionClass == AckSetState.class) {
            return (Optional<T>) ackSetStateExtension;
        }
        return Position.super.getExtension(extensionClass);
    }
}
