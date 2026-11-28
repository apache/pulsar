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

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;

/**
 * A read-only cursor that owns the read-only managed ledger it reads from.
 *
 * <p>Closing the cursor also closes that ledger, which releases the read handles cached while reading entries
 * through the cursor. A cursor created on a caller-owned read-only managed ledger uses {@link ReadOnlyCursorImpl}
 * instead, since closing it must leave the ledger open.
 */
class OwnedReadOnlyCursorImpl extends ReadOnlyCursorImpl {

    OwnedReadOnlyCursorImpl(BookKeeper bookkeeper, ManagedLedgerImpl ledger, Position startPosition,
                            String cursorName) {
        super(bookkeeper, ledger, startPosition, cursorName);
    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        super.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ignored) {
                ledger.asyncClose(callback, ctx);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ignored) {
                callback.closeFailed(exception, ctx);
            }
        }, ctx);
    }
}
