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

import com.google.common.collect.Range;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.PositionBound;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;

@Slf4j
public class ReadOnlyCursorImpl extends ManagedCursorImpl implements ReadOnlyCursor {

    public ReadOnlyCursorImpl(BookKeeper bookkeeper, ManagedLedgerConfig config, ManagedLedgerImpl ledger,
                              PositionImpl startPosition, String cursorName) {
        super(bookkeeper, config, ledger, cursorName);

        if (startPosition.equals(PositionImpl.EARLIEST)) {
            readPosition = ledger.getFirstPosition().getNext();
        } else {
            readPosition = startPosition;
        }

        if (ledger.getLastPosition().compareTo(readPosition) <= 0) {
            messagesConsumedCounter = 0;
        } else {
            messagesConsumedCounter = -getNumberOfEntries(Range.closed(readPosition, ledger.getLastPosition()));
        }

        this.state = State.NoLedger;
    }

    @Override
    public void skipEntries(int numEntriesToSkip) {
        log.info("[{}] Skipping {} entries on read-only cursor {}", ledger.getName(), numEntriesToSkip, getName());
        READ_POSITION_UPDATER.getAndUpdate(this, lastRead ->
                ledger.getPositionAfterN(lastRead, numEntriesToSkip, PositionBound.startIncluded).getNext());
    }

    @Override
    public void asyncClose(final AsyncCallbacks.CloseCallback callback, final Object ctx) {
        state = State.Closed;
        callback.closeComplete(ctx);
    }

    public MLDataFormats.ManagedLedgerInfo.LedgerInfo getCurrentLedgerInfo() {
        return this.ledger.getLedgersInfo().get(this.readPosition.getLedgerId());
    }

    @Override
    public long getNumberOfEntries(Range<PositionImpl> range) {
        return this.ledger.getNumberOfEntries(range);
    }
}
