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

import static org.apache.bookkeeper.mledger.impl.EntryCountEstimator.estimateEntryCountByBytesSize;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.util.ManagedLedgerUtils;

public final class RandomReader implements AutoCloseable {
    private final ManagedLedgerImpl ledger;
    private final RandomReaders owner;
    private final boolean populateCache;
    private final AtomicBoolean closed = new AtomicBoolean();

    RandomReader(ManagedLedgerImpl ledger, RandomReaders owner, boolean populateCache) {
        this.ledger = ledger;
        this.owner = owner;
        this.populateCache = populateCache;
    }

    public CompletableFuture<List<Entry>> read(Position startPosition, int numberOfEntries) {
        return read(startPosition, numberOfEntries, PositionFactory.LATEST,
                ManagedLedgerUtils.NO_MAX_SIZE_LIMIT);
    }

    public CompletableFuture<List<Entry>> read(Position startPosition, int numberOfEntries, Position maxPosition,
                                               long maxSizeBytes) {
        if (closed.get() || owner.isClosed()) {
            return CompletableFuture.failedFuture(
                    new ManagedLedgerAlreadyClosedException("Random reader is already closed"));
        }
        if (startPosition == null || numberOfEntries <= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid parameters"));
        }

        Position normalizedMaxPosition = maxPosition != null ? maxPosition : PositionFactory.LATEST;
        Position normalizedStartPosition = normalizeStartPosition(startPosition);
        if (normalizedStartPosition == null || normalizedMaxPosition.compareTo(normalizedStartPosition) < 0) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        int effectiveCount = numberOfEntries;
        if (maxSizeBytes != ManagedLedgerUtils.NO_MAX_SIZE_LIMIT) {
            effectiveCount = Math.min(numberOfEntries,
                    estimateEntryCountByBytesSize(numberOfEntries, maxSizeBytes, normalizedStartPosition, ledger));
        }

        return OpRandomReadEntries.read(
                ledger, normalizedStartPosition, effectiveCount, normalizedMaxPosition, populateCache);
    }

    public CompletableFuture<List<Entry>> read(Position startPosition, int numberOfEntries, Position maxPosition) {
        return read(startPosition, numberOfEntries, maxPosition, ManagedLedgerUtils.NO_MAX_SIZE_LIMIT);
    }

    public CompletableFuture<List<Entry>> read(Position startPosition, int numberOfEntries, long maxSizeBytes) {
        return read(startPosition, numberOfEntries, PositionFactory.LATEST, maxSizeBytes);
    }

    private Position normalizeStartPosition(Position startPosition) {
        if (PositionFactory.EARLIEST.equals(startPosition)) {
            Map.Entry<Long, LedgerInfo> firstLedger = ledger.getLedgersInfo().firstEntry();
            return firstLedger != null ? PositionFactory.create(firstLedger.getKey(), 0) : null;
        }

        Position lastPosition = ledger.getLastConfirmedEntry();
        if (PositionFactory.LATEST.equals(startPosition)) {
            return lastPosition != null ? lastPosition.getNext() : null;
        }
        if (lastPosition == null || startPosition.compareTo(lastPosition) > 0) {
            return null;
        }
        return ledger.isValidPosition(startPosition)
                ? startPosition
                : ledger.getNextValidPosition(startPosition);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            owner.unregister(populateCache);
        }
    }

    boolean isClosed() {
        return closed.get() || owner.isClosed();
    }
}
