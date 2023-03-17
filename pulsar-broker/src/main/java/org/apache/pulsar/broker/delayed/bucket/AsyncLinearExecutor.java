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
package org.apache.pulsar.broker.delayed.bucket;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Make Asynchronous tasks linear execution, avoid data races cause operate failures.
 */
class AsyncLinearExecutor {

    private final Queue<Pair<Supplier<CompletableFuture<Void>>, CompletableFuture<Void>>> taskQueue =
            new ArrayDeque<>();
    private boolean inProgress = false;

    public CompletableFuture<Void> submitTask(Supplier<CompletableFuture<Void>> supplier) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        tryExecAsyncTask(Pair.of(supplier, future));
        return future;
    }

    private synchronized void tryExecAsyncTask(
            Pair<Supplier<CompletableFuture<Void>>, CompletableFuture<Void>> taskPair) {
        if (inProgress) {
            taskQueue.add(taskPair);
        } else {
            inProgress = true;
            CompletableFuture<Void> future = taskPair.getRight();
            taskPair.getLeft().get().whenComplete((__, ex) -> {
                if (ex != null) {
                    future.completeExceptionally(ex);
                } else {
                    future.complete(null);
                }
                synchronized (this) {
                    Pair<Supplier<CompletableFuture<Void>>, CompletableFuture<Void>> pair = taskQueue.poll();
                    inProgress = false;
                    if (pair != null) {
                        tryExecAsyncTask(pair);
                    }
                }
            });
        }
    }
}
