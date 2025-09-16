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
package org.apache.bookkeeper.mledger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * It wraps the read entries' callback and context to ensure that the callback can only be triggered once.
 */
public class ReadEntriesContext {

    private AsyncCallbacks.ReadEntriesCallback callback;
    private final Object ctx;
    private Consumer<List<Entry>> beforeCompleteCallback = null;

    public ReadEntriesContext(AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        this.callback = callback;
        this.ctx = ctx;
    }

    public void complete(List<Entry> entries) {
        AsyncCallbacks.ReadEntriesCallback callback;
        Consumer<List<Entry>> beforeCompleteCallback;
        synchronized (this) {
            beforeCompleteCallback = this.beforeCompleteCallback;
            this.beforeCompleteCallback = null;
            callback = this.callback;
            this.callback = null;
        }
        if (beforeCompleteCallback != null) {
            beforeCompleteCallback.accept(entries);
        }
        if (callback != null) {
            callback.readEntriesComplete(entries, ctx);
        }
    }

    public void fail(Throwable throwable) {
        AsyncCallbacks.ReadEntriesCallback callback;
        Consumer<List<Entry>> beforeCompleteCallback;
        synchronized (this) {
            beforeCompleteCallback = this.beforeCompleteCallback;
            this.beforeCompleteCallback = null;
            callback = this.callback;
            this.callback = null;
        }
        if (beforeCompleteCallback != null) {
            beforeCompleteCallback.accept(List.of());
        }
        if (callback != null) {
            callback.readEntriesFailed(ManagedLedgerException.getManagedLedgerException(throwable), ctx);
        }
    }

    public synchronized void setBeforeCompleteCallback(Consumer<List<Entry>> beforeCompleteCallback) {
        this.beforeCompleteCallback = beforeCompleteCallback;
    }

    public static ReadEntriesContext fromFuture(CompletableFuture<List<Entry>> future) {
        return new ReadEntriesContext(new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                future.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
    }
}
