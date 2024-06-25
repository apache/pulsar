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
package org.apache.bookkeeper.mledger.util;

import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.common.classification.InterfaceStability;

@InterfaceStability.Evolving
public class ManagedLedgerImplUtils {

    /**
     * Reverse find last valid position one-entry by one-entry.
     */
    public static CompletableFuture<Position> asyncGetLastValidPosition(final ManagedLedgerImpl ledger,
                                                                        final Predicate<Entry> predicate,
                                                                        final Position startPosition) {
        CompletableFuture<Position> future = new CompletableFuture<>();
        internalAsyncReverseFindPositionOneByOne(ledger, predicate, startPosition, future);
        return future;
    }

    private static void internalAsyncReverseFindPositionOneByOne(final ManagedLedgerImpl ledger,
                                                                 final Predicate<Entry> predicate,
                                                                 final Position position,
                                                                 final CompletableFuture<Position> future) {
        if (!ledger.isValidPosition(position)) {
            future.complete(position);
            return;
        }
        ledger.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                final Position position = entry.getPosition();
                try {
                    if (predicate.test(entry)) {
                        future.complete(position);
                        return;
                    }
                    Position previousPosition = ledger.getPreviousPosition(position);
                    internalAsyncReverseFindPositionOneByOne(ledger, predicate, previousPosition, future);
                } catch (Exception e) {
                    future.completeExceptionally(e);
                } finally {
                    entry.release();
                }
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
    }
}
