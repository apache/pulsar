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
package org.apache.bookkeeper.mledger.util;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

/**
 * Conveniences to use with {@link CompletableFuture}.
 */
public class Futures {

    public static CompletableFuture<Void> NULL_PROMISE = CompletableFuture.completedFuture(null);

    /**
     * Adapts a {@link CloseCallback} to a {@link CompletableFuture}.
     */
    public static class CloseFuture extends CompletableFuture<Void> implements CloseCallback {

        @Override
        public void closeComplete(Object ctx) {
            complete(null);
        }

        @Override
        public void closeFailed(ManagedLedgerException exception, Object ctx) {
            completeExceptionally(exception);
        }
    }

    public static CompletableFuture<Void> waitForAll(List<CompletableFuture<Void>> futures) {
        final CompletableFuture<Void> compositeFuture = new CompletableFuture<>();
        final AtomicInteger count = new AtomicInteger(futures.size());

        for (CompletableFuture<Void> future : futures) {
            future.whenComplete((r, ex) -> {
                if (ex != null) {
                    compositeFuture.completeExceptionally(ex);
                } else if (count.decrementAndGet() == 0) {
                    // All the pending futures did complete
                    compositeFuture.complete(null);
                }
            });
        }

        if (futures.isEmpty()) {
            compositeFuture.complete(null);
        }

        return compositeFuture;
    }
}
