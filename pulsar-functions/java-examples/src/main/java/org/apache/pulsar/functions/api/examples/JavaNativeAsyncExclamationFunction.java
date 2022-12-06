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
package org.apache.pulsar.functions.api.examples;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class JavaNativeAsyncExclamationFunction implements Function<String, CompletableFuture<String>> {
    @Override
    public CompletableFuture<String> apply(String input) {
        CompletableFuture<String> future = new CompletableFuture();

        Executors.newCachedThreadPool().submit(() -> {
            try {
                Thread.sleep(500);
                future.complete(String.format("%s-!!", input));
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });

        return future;
    }
}
