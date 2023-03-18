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
package org.apache.pulsar.common.util;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Make Asynchronous tasks sequential execution, avoid data races cause operate failures.
 * <p>
 * Note:The submitted task must be an asynchronous task, so task will use its own thread to execute,
 * and only use the current thread to start that asynchronous task.
 * It can only guarantee the order of start of tasks and will not execute two tasks at the same time, but
 * it cannot guarantee the order in which tasks are completed.
 * </p>
 */
@ThreadSafe
public class AsyncSequentialExecutor<T> {

    private final Queue<Pair<Supplier<CompletableFuture<T>>, CompletableFuture<T>>> taskQueue =
            new ArrayDeque<>();
    private boolean inProgress = false;

    public CompletableFuture<T> submitTask(Supplier<CompletableFuture<T>> supplier) {
        CompletableFuture<T> future = new CompletableFuture<>();
        tryStartAsyncTask(Pair.of(supplier, future));
        return future;
    }

    private synchronized void tryStartAsyncTask(
            Pair<Supplier<CompletableFuture<T>>, CompletableFuture<T>> taskPair) {
        if (inProgress) {
            taskQueue.add(taskPair);
        } else {
            inProgress = true;
            CompletableFuture<T> future = taskPair.getRight();
            taskPair.getLeft().get().whenComplete((v, ex) -> {
                synchronized (this) {
                    Pair<Supplier<CompletableFuture<T>>, CompletableFuture<T>> pair = taskQueue.poll();
                    inProgress = false;
                    if (pair != null) {
                        tryStartAsyncTask(pair);
                    }
                }
                if (ex != null) {
                    future.completeExceptionally(ex);
                } else {
                    future.complete(v);
                }
            });
        }
    }
}
