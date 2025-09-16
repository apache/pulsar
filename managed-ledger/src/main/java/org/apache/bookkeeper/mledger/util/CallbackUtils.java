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
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

/**
 * Currently, the managed ledger APIs mostly accept a callback and a context that is passed to the callback, if the
 * callback is called twice unexpectedly, the caller side might fail, e.g. the context is a recyclable object whose
 * `recycle()` method is called when it's passed to the callback.
 * This util class delegates the callback and the corresponding context object to a {@link CompletableFuture}, which
 * guarantees the callback can be called exactly once in {@link CompletableFuture#whenComplete} when the future is
 * completed.
 */
public class CallbackUtils {

    public static CompletableFuture<List<Entry>> wrap(AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        final var future = new CompletableFuture<List<Entry>>();
        future.whenComplete((entries, throwable) -> {
            if (throwable == null) {
                callback.readEntriesComplete(entries, ctx);
            } else {
                callback.readEntriesFailed(ManagedLedgerException.getManagedLedgerException(throwable), ctx);
            }
        });
        return future;
    }

    public static CompletableFuture<Entry> wrap(AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
        final var future = new CompletableFuture<Entry>();
        future.whenComplete((entries, throwable) -> {
            if (throwable == null) {
                callback.readEntryComplete(entries, ctx);
            } else {
                callback.readEntryFailed(ManagedLedgerException.getManagedLedgerException(throwable), ctx);
            }
        });
        return future;
    }
}
