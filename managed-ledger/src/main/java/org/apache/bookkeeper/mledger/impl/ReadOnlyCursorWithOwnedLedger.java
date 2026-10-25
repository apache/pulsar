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

import com.google.common.collect.Range;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.pulsar.common.util.FutureUtil;

/** A factory-created cursor owns its otherwise inaccessible read-only managed ledger. */
final class ReadOnlyCursorWithOwnedLedger implements ReadOnlyCursor {
    private final ReadOnlyCursor cursor;
    private final ReadOnlyManagedLedger ledger;
    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    ReadOnlyCursorWithOwnedLedger(ReadOnlyCursor cursor, ReadOnlyManagedLedger ledger) {
        this.cursor = cursor;
        this.ledger = ledger;
    }

    @Override
    public List<Entry> readEntries(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException {
        return cursor.readEntries(numberOfEntriesToRead);
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback,
                                 Object ctx, Position maxPosition) {
        cursor.asyncReadEntries(numberOfEntriesToRead, callback, ctx, maxPosition);
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, long maxSizeBytes,
                                 AsyncCallbacks.ReadEntriesCallback callback, Object ctx, Position maxPosition) {
        cursor.asyncReadEntries(numberOfEntriesToRead, maxSizeBytes, callback, ctx, maxPosition);
    }

    @Override
    public Position getReadPosition() {
        return cursor.getReadPosition();
    }

    @Override
    public boolean hasMoreEntries() {
        return cursor.hasMoreEntries();
    }

    @Override
    public long getNumberOfEntries() {
        return cursor.getNumberOfEntries();
    }

    @Override
    public long getNumberOfEntries(Range<Position> range) {
        return cursor.getNumberOfEntries(range);
    }

    @Override
    public void skipEntries(int numEntriesToSkip) {
        cursor.skipEntries(numEntriesToSkip);
    }

    @Override
    public Position findNewestMatching(ManagedCursor.FindPositionConstraint constraint, Predicate<Entry> condition)
            throws InterruptedException, ManagedLedgerException {
        return cursor.findNewestMatching(constraint, condition);
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {
        Futures.CloseFuture result = new Futures.CloseFuture();
        asyncClose(result, null);
        try {
            result.get(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS);
        } catch (ExecutionException error) {
            throw ManagedLedgerException.getManagedLedgerException(FutureUtil.unwrapCompletionException(error));
        } catch (TimeoutException error) {
            throw new ManagedLedgerException("Timeout during close operation");
        }
    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        if (closeStarted.compareAndSet(false, true)) {
            CompletableFuture<Void> cursorClose = FutureUtil.supplySafely(() -> {
                Futures.CloseFuture result = new Futures.CloseFuture();
                cursor.asyncClose(result, null);
                return result;
            });
            FutureUtil.completeAfter(closeFuture, cursorClose.handle((__, error) -> error)
                    .thenCompose(cursorError -> FutureUtil.supplySafely(ledger::asyncClose)
                            .handle((__, ledgerError) -> {
                                // Always release the owned ledger, including when closing the cursor fails.
                                if (cursorError != null) {
                                    throw FutureUtil.wrapToCompletionException(cursorError);
                                }
                                if (ledgerError != null) {
                                    throw FutureUtil.wrapToCompletionException(ledgerError);
                                }
                                return null;
                            })));
        }
        closeFuture.whenComplete((__, error) -> {
            if (error == null) {
                callback.closeComplete(ctx);
            } else {
                callback.closeFailed(ManagedLedgerException
                        .getManagedLedgerException(FutureUtil.unwrapCompletionException(error)), ctx);
            }
        });
    }
}
