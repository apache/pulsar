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

import com.google.common.base.MoreObjects;
import com.google.common.collect.Range;
import java.util.Map;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonDurableCursorImpl extends ManagedCursorImpl {

    NonDurableCursorImpl(BookKeeper bookkeeper, ManagedLedgerConfig config, ManagedLedgerImpl ledger, String cursorName,
                         PositionImpl startCursorPosition, CommandSubscribe.InitialPosition initialPosition) {
        super(bookkeeper, config, ledger, cursorName);

        // Compare with "latest" position marker by using only the ledger id. Since the C++ client is using 48bits to
        // store the entryId, it's not able to pass a Long.max() as entryId. In this case there's no point to require
        // both ledgerId and entryId to be Long.max()
        if (startCursorPosition == null || startCursorPosition.compareTo(ledger.lastConfirmedEntry) > 0) {
            // Start from last entry
            switch (initialPosition) {
                case Latest:
                    initializeCursorPosition(ledger.getLastPositionAndCounter());
                    break;
                case Earliest:
                    initializeCursorPosition(ledger.getFirstPositionAndCounter());
                    break;
            }
        } else if (startCursorPosition.getLedgerId() == PositionImpl.earliest.getLedgerId()) {
            // Start from invalid ledger to read from first available entry
            recoverCursor(ledger.getPreviousPosition(ledger.getFirstPosition()));
        } else {
            // Since the cursor is positioning on the mark-delete position, we need to take 1 step back from the desired
            // read-position
            recoverCursor(startCursorPosition);
        }

        log.info("[{}] Created non-durable cursor read-position={} mark-delete-position={}", ledger.getName(),
                readPosition, markDeletePosition);
    }

    private void recoverCursor(PositionImpl mdPosition) {
        Pair<PositionImpl, Long> lastEntryAndCounter = ledger.getLastPositionAndCounter();
        this.readPosition = ledger.getNextValidPosition(mdPosition);
        markDeletePosition = mdPosition;

        // Initialize the counter such that the difference between the messages written on the ML and the
        // messagesConsumed is equal to the current backlog (negated).
        if (null != this.readPosition) {
            long initialBacklog = readPosition.compareTo(lastEntryAndCounter.getLeft()) < 0
                ? ledger.getNumberOfEntries(Range.closed(readPosition, lastEntryAndCounter.getLeft())) : 0;
            messagesConsumedCounter = lastEntryAndCounter.getRight() - initialBacklog;
        } else {
            log.warn("Recovered a non-durable cursor from position {} but didn't find a valid read position {}",
                mdPosition, readPosition);
        }
    }

    @Override
    public boolean isDurable() {
        return false;
    }

    /// Overridden methods from ManagedCursorImpl. Void implementation to skip cursor persistence

    @Override
    void recover(final VoidCallback callback) {
        /// No-Op
    }

    @Override
    protected void internalAsyncMarkDelete(final PositionImpl newPosition, Map<String, Long> properties,
            final MarkDeleteCallback callback, final Object ctx) {
        // Bypass persistence of mark-delete position and individually deleted messages info

        MarkDeleteEntry mdEntry = new MarkDeleteEntry(newPosition, properties, callback, ctx);
        lastMarkDeleteEntry = mdEntry;
        // it is important to advance cursor so the retention can kick in as expected.
        ledger.updateCursor(NonDurableCursorImpl.this, mdEntry.newPosition);

        callback.markDeleteComplete(ctx);
    }

    @Override
    public void asyncClose(CloseCallback callback, Object ctx) {
        // No-Op
        callback.closeComplete(ctx);
    }

    public void asyncDeleteCursor(final String consumerName, final DeleteCursorCallback callback, final Object ctx) {
        /// No-Op
        callback.deleteCursorComplete(ctx);
    }

    @Override
    public synchronized String toString() {
        return MoreObjects.toStringHelper(this).add("ledger", ledger.getName()).add("ackPos", markDeletePosition)
                .add("readPos", readPosition).toString();
    }

    private static final Logger log = LoggerFactory.getLogger(NonDurableCursorImpl.class);
}
