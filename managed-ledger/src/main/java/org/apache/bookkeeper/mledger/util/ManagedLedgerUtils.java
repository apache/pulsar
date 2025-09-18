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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * This util class contains some future-based methods to replace callback-based APIs. With a callback-based API, if any
 * exception is thrown in the callback, the callback will never have a chance to be called. While with a future-based
 * API, if any exception is thrown in future's callback (e.g. `thenApply`), the future will eventually be completed
 * exceptionally. In addition, future-based API is easier for users to switch a different executor to execute the
 * callback (e.g. `thenApplyAsync`).
 */
@InterfaceStability.Evolving
public class ManagedLedgerUtils {

    public static final long NO_MAX_SIZE_LIMIT = -1L;

    public static CompletableFuture<ManagedCursor> openCursor(ManagedLedger ml, String cursorName) {
        final var future = new CompletableFuture<ManagedCursor>();
        ml.asyncOpenCursor(cursorName, new AsyncCallbacks.OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                future.complete(cursor);
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return future;
    }

    public static CompletableFuture<List<Entry>> readEntries(ManagedCursor cursor, int numberOfEntriesToRead,
                                                             Position maxPosition) {
        return readEntries(cursor, numberOfEntriesToRead, NO_MAX_SIZE_LIMIT, maxPosition);
    }

    public static CompletableFuture<List<Entry>> readEntries(ManagedCursor cursor, int numberOfEntriesToRead,
                                                             long maxBytes, Position maxPosition) {
        final var future = new CompletableFuture<List<Entry>>();
        cursor.asyncReadEntries(numberOfEntriesToRead, maxBytes, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                future.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null, (PositionImpl) maxPosition);
        return future;
    }

    public static CompletableFuture<List<Entry>> readEntriesWithSkipOrWait(
            ManagedCursor cursor, int maxEntries, long maxSizeBytes, PositionImpl maxPosition,
            @Nullable Predicate<PositionImpl> skipCondition) {
        final var future = new CompletableFuture<List<Entry>>();
        cursor.asyncReadEntriesWithSkipOrWait(maxEntries, maxSizeBytes, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                future.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null, maxPosition, skipCondition);
        return future;
    }

    public static CompletableFuture<Void> markDelete(ManagedCursor cursor, Position position,
                                                     Map<String, Long> properties) {
        final var future = new CompletableFuture<Void>();
        cursor.asyncMarkDelete(position, properties, new AsyncCallbacks.MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                future.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return future;
    }
}
