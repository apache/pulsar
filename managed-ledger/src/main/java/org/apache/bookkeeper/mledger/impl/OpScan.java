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

import com.google.common.base.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ScanCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.PositionBound;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Slf4j
class OpScan implements ReadEntryCallback {
    private final ManagedCursorImpl cursor;
    private final ManagedLedgerImpl ledger;
    private final PositionImpl startPosition;
    private final ScanCallback callback;
    private final Predicate<Entry> condition;
    private final Object ctx;

    PositionImpl searchPosition;
    Position lastMatchedPosition = null;

    public OpScan(ManagedCursorImpl cursor, PositionImpl startPosition, Predicate<Entry> condition, ScanCallback callback, Object ctx) {
        this.cursor = cursor;
        this.ledger = cursor.ledger;
        this.startPosition = startPosition;
        this.callback = callback;
        this.condition = condition;
        this.ctx = ctx;
        this.searchPosition = startPosition;
    }


    @Override
    public void readEntryComplete(Entry entry, Object ctx) {
        final Position position = entry.getPosition();
        lastMatchedPosition = position;
        // filter out the entry if it has been already deleted
        // filterReadEntries will call entry.release if the entry is filtered out
        List<Entry> entries = this.cursor.filterReadEntries(List.of(entry));
        if (!entries.isEmpty()) {
            if (!condition.apply(entry)) {
                entry.release();
                // user code requested to stop our scan
                callback.scanComplete(lastMatchedPosition, OpScan.this.ctx);
                return;
            } else {
                entry.release();
            }
        }
        searchPosition = ledger.getPositionAfterN((PositionImpl) position, 1, PositionBound.startExcluded);
        if (log.isDebugEnabled()) {
            log.debug("readEntryComplete {} at {} next is {}", entry, position, searchPosition);
        }

        if (searchPosition.compareTo((PositionImpl) position) == 0) {
            // we have reached the end of the ledger, as we are not doing progress
            callback.scanComplete(lastMatchedPosition, OpScan.this.ctx);
            return;
        }
        find();
    }

    @Override
    public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
        callback.scanFailed(exception, Optional.ofNullable(searchPosition), OpScan.this.ctx);
    }

    public void find() {
        if (cursor != null ? cursor.hasMoreEntries(searchPosition) : ledger.hasMoreEntries(searchPosition)) {
            ledger.asyncReadEntry(searchPosition, this, null);
        } else {
            callback.scanComplete(lastMatchedPosition, OpScan.this.ctx);
        }
    }
}
