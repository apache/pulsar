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
package org.apache.pulsar.common.util.netty;

import io.netty.util.concurrent.Future;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Contains utility methods for working with Netty Futures
 */
public class NettyFutureUtil {
    /**
     * Converts a Netty {@link Future} to {@link CompletableFuture}
     *
     * @param future Netty future
     * @param <V>    value type
     * @return converted future instance
     */
    public static <V> CompletableFuture<V> toCompletableFuture(Future<V> future) {
        Objects.requireNonNull(future, "future cannot be null");

        CompletableFuture<V> adapter = new CompletableFuture<>();
        if (future.isDone()) {
            if (future.isSuccess()) {
                adapter.complete(future.getNow());
            } else {
                adapter.completeExceptionally(future.cause());
            }
        } else {
            future.addListener((Future<V> f) -> {
                if (f.isSuccess()) {
                    adapter.complete(f.getNow());
                } else {
                    adapter.completeExceptionally(f.cause());
                }
            });
        }
        return adapter;
    }

    /**
     * Converts a Netty {@link Future} to {@link CompletableFuture} with Void type
     *
     * @param future Netty future
     * @return converted future instance
     */
    public static CompletableFuture<Void> toCompletableFutureVoid(Future<?> future) {
        Objects.requireNonNull(future, "future cannot be null");

        CompletableFuture<Void> adapter = new CompletableFuture<>();
        if (future.isDone()) {
            if (future.isSuccess()) {
                adapter.complete(null);
            } else {
                adapter.completeExceptionally(future.cause());
            }
        } else {
            future.addListener(f -> {
                if (f.isSuccess()) {
                    adapter.complete(null);
                } else {
                    adapter.completeExceptionally(f.cause());
                }
            });
        }
        return adapter;
    }
}
