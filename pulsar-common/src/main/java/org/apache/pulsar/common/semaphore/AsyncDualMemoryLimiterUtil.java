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
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.AsyncDualMemoryLimiterPermit;

@UtilityClass
public class AsyncDualMemoryLimiterUtil {

    public static <T> CompletableFuture<T> withPermitsFuture(
            CompletableFuture<AsyncDualMemoryLimiterPermit>
                    permitsFuture,
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
     * Acquire permits and write the command as the response to the channel.
     * Releases the permits after the response has been written to the socket or the write fails.
     *
     * @param ctx               the channel handler context.
     * @param dualMemoryLimiter the memory limiter to acquire permits from.
     * @param command           the command to write to the channel.
     * @return a future that completes when the command has been written to the channel's outbound buffer.
     */
    public static CompletableFuture<Void> acquireDirectMemoryPermitsAndWriteAndFlush(ChannelHandlerContext ctx,
                                                                                     AsyncDualMemoryLimiter
                                                                                             dualMemoryLimiter,
                                                                                     BooleanSupplier isCancelled,
                                                                                     BaseCommand command,
                                                                                     Consumer<Throwable>
                                                                                             permitAcquireErrorHandler
                                                                                     ) {
        // Calculate serialized size before acquiring permits
        int serializedSize = command.getSerializedSize();
        // Acquire permits
        return dualMemoryLimiter.acquire(serializedSize, AsyncDualMemoryLimiter.LimitType.DIRECT_MEMORY, isCancelled)
                .whenComplete((permits, t) -> {
                    if (t != null) {
                        permitAcquireErrorHandler.accept(t);
                        return;
                    }
                    try {
                        // Serialize the response
                        ByteBuf outBuf = Commands.serializeWithPrecalculatedSerializedSize(command, serializedSize);
                        // Write the response
                        ctx.writeAndFlush(outBuf).addListener(future -> {
                            // Release permits after the response has been written to the socket
                            dualMemoryLimiter.release(permits);
                        });
                    } catch (Exception e) {
                        // Return permits if an exception occurs before writeAndFlush is called successfully
                        dualMemoryLimiter.release(permits);
                        throw e;
                    }
                }).thenAccept(__ -> {
                });
    }
}
