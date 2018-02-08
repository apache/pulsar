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
package org.apache.pulsar.client.util;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.client.api.PulsarClientException;

public class FutureUtil {

    /**
     * Return a future that represents the completion of the futures in the provided list
     *
     * @param futures
     * @return
     */
    public static <T> CompletableFuture<T> waitForAll(List<CompletableFuture<T>> futures) {
        if (futures.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        final CompletableFuture<T> compositeFuture = new CompletableFuture<>();
        final AtomicInteger count = new AtomicInteger(futures.size());
        final AtomicReference<Throwable> exception = new AtomicReference<>();

        for (CompletableFuture<T> future : futures) {
            future.whenComplete((r, ex) -> {
                if (ex != null) {
                    exception.compareAndSet(null, ex);
                }
                if (count.decrementAndGet() == 0) {
                    // All the pending futures did complete
                    if (exception.get() != null) {
                        compositeFuture.completeExceptionally(exception.get());
                    } else {
                        compositeFuture.complete(null);
                    }
                }
            });
        }

        return compositeFuture;
    }

    public static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        if (t instanceof PulsarClientException) {
            future.completeExceptionally(t);
        } else {
            future.completeExceptionally(new PulsarClientException(t));
        }
        return future;
    }
}
