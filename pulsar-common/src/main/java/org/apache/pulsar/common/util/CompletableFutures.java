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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class CompletableFutures {
    public static <T> @Nonnull CompletionStage<T> apply(@Nonnull Supplier<T> fn) {
        try {
            final CompletableFuture<T> future = new CompletableFuture<>();
            if (fn == null) {
                future.completeExceptionally(new NullPointerException("Parameter can not be null."));
                return future;
            }
            future.complete(fn.get());
            return future;
        } catch (Throwable ex) {
            final CompletableFuture<T> future = new CompletableFuture<>();
            future.completeExceptionally(ex);
            return future;
        }
    }

    public static <T> @Nonnull CompletionStage<T> compose(@Nonnull Supplier<CompletionStage<T>> fn) {
        try {
            if (fn == null) {
                final CompletableFuture<T> future = new CompletableFuture<>();
                future.completeExceptionally(new NullPointerException("Parameter can not be null."));
                return future;
            }
            return fn.get();
        } catch (Throwable ex) {
            final CompletableFuture<T> future = new CompletableFuture<>();
            future.completeExceptionally(ex);
            return future;
        }
    }
}
