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
package org.apache.pulsar.common.util;

import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;
/**
 * This class is aimed at simplifying work with {@code CompletableFuture}.
 */
public class FutureUtil {

    /**
     * Return a future that represents the completion of the futures in the provided list.
     *
     * @param futures
     * @return
     */
    public static <T> CompletableFuture<Void> waitForAll(List<CompletableFuture<T>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * Return a futrue, which can be scheduled regularly.
     * @param executor Periodic execution method
     * @param command This is a functional interface and can therefore be used as the assignment
     * target for a lambda expression or method reference.
     * @param delay Delayable time
     * @param unit Set time unit
     * @return a new CompletableFuture that is completed when all of the given CompletableFutures complete
     */
    public static <T> CompletableFuture<T> schedule(
            ScheduledExecutorService executor,
            Supplier<T> command,
            long delay,
            TimeUnit unit
    ) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        ScheduledFuture timeout =  executor.schedule(
                (() -> {
                    try {
                        return completableFuture.complete(command.get());
                    } catch (Throwable t) {
                        return completableFuture.completeExceptionally(t);
                    }
                }),
                delay,
                unit
        );
        //When the captured futrue throws an exception, the worker thread clears the interruption status in time
        completableFuture.whenComplete((igrone ,exception1) -> {
            timeout.cancel(true);
        });

        return completableFuture;
    }

    public static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

    public static Throwable unwrapCompletionException(Throwable t) {
        if (t instanceof CompletionException) {
            return unwrapCompletionException(t.getCause());
        } else {
            return t;
        }
    }
}
