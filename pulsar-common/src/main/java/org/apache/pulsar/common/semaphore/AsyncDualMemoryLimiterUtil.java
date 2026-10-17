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
package org.apache.pulsar.common.semaphore;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.AsyncDualMemoryLimiterPermit;
import org.apache.pulsar.common.util.FutureUtil;

@UtilityClass
public class AsyncDualMemoryLimiterUtil {

    /**
     * Execute a function with acquired permits and ensure permits are released after completion.
     * This method handles the lifecycle of permits - acquisition, usage, and release, including error cases.
     *
     * @param permitsFuture             Future that will complete with the required permits
     * @param function                  Function to execute once permits are acquired that returns a CompletableFuture
     * @param permitAcquireErrorHandler Handler for permit acquisition errors that returns a CompletableFuture
     * @param releaser                  Consumer that handles releasing the permits
     * @param <T>                       The type of result returned by the function
     * @return CompletableFuture that completes with the result of the function execution
     */
    public static <T> CompletableFuture<T> withPermitsFuture(
            CompletableFuture<AsyncDualMemoryLimiterPermit>
                    permitsFuture,
            Function<AsyncDualMemoryLimiterPermit,
                    CompletableFuture<T>> function,
            Function<Throwable, CompletableFuture<T>>
                    permitAcquireErrorHandler,
            Consumer<AsyncDualMemoryLimiterPermit> releaser) {
        return withPermitsFuture(permitsFuture, () -> false, function, permitAcquireErrorHandler, releaser);
    }

    /**
     * Execute a function with acquired permits and ensure permits are released after completion.
     * This method handles the lifecycle of permits - acquisition, usage, and release, including error cases.
     * If the request has been cancelled by the time the permits are acquired, the permits are released
     * without executing the function, and the permit acquire error handler is called with
     * {@link AsyncSemaphore.PermitAcquireCancelledException}.
     *
     * @param permitsFuture             Future that will complete with the required permits
     * @param isCancelled               supplier that returns true if the request has been cancelled
     * @param function                  Function to execute once permits are acquired that returns a CompletableFuture
     * @param permitAcquireErrorHandler Handler for permit acquisition errors that returns a CompletableFuture
     * @param releaser                  Consumer that handles releasing the permits
     * @param <T>                       The type of result returned by the function
     * @return CompletableFuture that completes with the result of the function execution
     */
    public static <T> CompletableFuture<T> withPermitsFuture(
            CompletableFuture<AsyncDualMemoryLimiterPermit>
                    permitsFuture,
            BooleanSupplier isCancelled,
            Function<AsyncDualMemoryLimiterPermit,
                    CompletableFuture<T>> function,
            Function<Throwable, CompletableFuture<T>>
                    permitAcquireErrorHandler,
            Consumer<AsyncDualMemoryLimiterPermit> releaser) {
        return permitsFuture
                // combine the permits and error into a single pair so that it can be used in thenCompose
                .handle((permits, permitAcquireError) ->
                        Pair.of(permits, permitAcquireError))
                .thenCompose(permitsAndError -> {
                    if (permitsAndError.getRight() != null) {
                        // permits weren't acquired
                        return permitAcquireErrorHandler.apply(permitsAndError.getRight());
                    } else {
                        // permits were acquired
                        AsyncDualMemoryLimiterPermit permits = permitsAndError.getLeft();
                        if (isCancelled.getAsBoolean()) {
                            // the request was cancelled while waiting for the permits, skip the operation
                            releaser.accept(permits);
                            return permitAcquireErrorHandler.apply(newCancelledException());
                        }
                        try {
                            return function.apply(permits)
                                    .whenComplete((__, ___) ->
                                            // release the permits
                                            releaser.accept(permits));
                        } catch (Throwable t) {
                            // release the permits if an exception occurs before the function returns
                            releaser.accept(permits);
                            throw t;
                        }
                    }
                });
    }

    /**
     * Acquires permits and writes the command as a response to the channel.
     * Releases the permits after the response has been written to the socket or if the write fails.
     *
     * @param ctx the channel handler context used for writing the response
     * @param dualMemoryLimiter the memory limiter used to acquire and release memory permits
     * @param isCancelled supplier that indicates if the permit acquisition should be cancelled. When it returns true
     *                    after the permits have been acquired, the command isn't serialized or written, the permits
     *                    are released and permitAcquireErrorHandler is called with
     *                    AsyncSemaphore.PermitAcquireCancelledException
     * @param command the base command to serialize and write to the channel
     * @param permitAcquireErrorHandler handler for errors that occur during permit acquisition
     * @return a future that completes when the command has been written to the channel's outbound buffer
     */
    public static CompletableFuture<Void> acquireDirectMemoryPermitsAndWriteAndFlush(ChannelHandlerContext ctx,
                                                                                     AsyncDualMemoryLimiter
                                                                                             dualMemoryLimiter,
                                                                                     BooleanSupplier isCancelled,
                                                                                     BaseCommand command,
                                                                                     Function<Throwable,
                                                                                             CompletableFuture<Void>>
                                                                                             permitAcquireErrorHandler
                                                                                     ) {
        // Calculate serialized size before acquiring permits
        int serializedSize = command.getSerializedSize();
        // Acquire permits
        return requestPermits(() -> dualMemoryLimiter.acquire(serializedSize,
                        AsyncDualMemoryLimiter.LimitType.DIRECT_MEMORY, isCancelled))
                .handle((permits, t) -> {
                    if (t != null) {
                        return permitAcquireErrorHandler.apply(t);
                    }
                    if (isCancelled.getAsBoolean()) {
                        // the request was cancelled while waiting for the permits, skip serializing and writing
                        dualMemoryLimiter.release(permits);
                        return permitAcquireErrorHandler.apply(newCancelledException());
                    }
                    try {
                        // Serialize the response
                        ByteBuf outBuf = Commands.serializeWithPrecalculatedSerializedSize(command, serializedSize);
                        // Write the response
                        ctx.writeAndFlush(outBuf).addListener(future -> {
                            // Release permits after the response has been written to the socket
                            dualMemoryLimiter.release(permits);
                        });
                    } catch (Throwable e) {
                        // Return permits if an exception occurs before writeAndFlush is called successfully
                        dualMemoryLimiter.release(permits);
                        throw e;
                    }
                    return CompletableFuture.<Void>completedFuture(null);
                }).thenCompose(Function.identity());
    }

    /**
     * Requests permits so that all failures are reported through the returned future. The limiter rejects a request
     * that is larger than the limit by throwing {@link IllegalArgumentException}, which is converted to a future
     * that completes exceptionally with {@link AsyncSemaphore.PermitAcquireLimitExceededException}. Callers that
     * handle failures only through the future would otherwise never complete the request.
     *
     * @param permitRequest supplier that requests the permits from the limiter
     * @return future that completes with the permits or exceptionally when the permits can't be acquired
     */
    public static CompletableFuture<AsyncDualMemoryLimiterPermit> requestPermits(
            Supplier<CompletableFuture<AsyncDualMemoryLimiterPermit>> permitRequest) {
        try {
            return permitRequest.get();
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(
                    new AsyncSemaphore.PermitAcquireLimitExceededException(e.getMessage(), e));
        }
    }

    /**
     * Checks whether the permit request failed because the request was cancelled.
     *
     * @param throwable the failure, possibly wrapped in a {@link java.util.concurrent.CompletionException}
     * @return true if the request was cancelled
     */
    public static boolean isPermitAcquireCancelled(Throwable throwable) {
        return FutureUtil.unwrapCompletionException(throwable)
                instanceof AsyncSemaphore.PermitAcquireCancelledException;
    }

    private static AsyncSemaphore.PermitAcquireCancelledException newCancelledException() {
        return new AsyncSemaphore.PermitAcquireCancelledException("Request was cancelled before permits were used");
    }
}
