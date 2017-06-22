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

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Range;

public class NonDurableCursorImpl extends ManagedCursorImpl {

    NonDurableCursorImpl(BookKeeper bookkeeper, ManagedLedgerConfig config, ManagedLedgerImpl ledger, String cursorName,
            PositionImpl startCursorPosition) {
        super(bookkeeper, config, ledger, cursorName);

        if (startCursorPosition == null || startCursorPosition.equals(PositionImpl.latest)) {
            // Start from last entry
            initializeCursorPosition(ledger.getLastPositionAndCounter());
        } else if (startCursorPosition.equals(PositionImpl.earliest)) {
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
        long initialBacklog = readPosition.compareTo(lastEntryAndCounter.first) < 0
                ? ledger.getNumberOfEntries(Range.closed(readPosition, lastEntryAndCounter.first)) : 0;
        messagesConsumedCounter = lastEntryAndCounter.second - initialBacklog;
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
    protected void internalAsyncMarkDelete(final PositionImpl newPosition, final MarkDeleteCallback callback,
            final Object ctx) {
        // Bypass persistence of mark-delete position and individually deleted messages info
        callback.markDeleteComplete(ctx);
    }

    @Override
    public void setActive() {
        /// No-Op
    }

    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public void setInactive() {
        /// No-Op
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
        return Objects.toStringHelper(this).add("ledger", ledger.getName()).add("ackPos", markDeletePosition)
                .add("readPos", readPosition).toString();
    }

    private static final Logger log = LoggerFactory.getLogger(NonDurableCursorImpl.class);
}
