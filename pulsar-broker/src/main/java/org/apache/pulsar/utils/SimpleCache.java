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
package org.apache.pulsar.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SimpleCache<K, V> {

    private final Map<K, V> cache = new HashMap<>();
    private final Map<K, ScheduledFuture<?>> futures = new HashMap<>();
    private final ScheduledExecutorService executor;
    private final long timeoutMs;

    public synchronized V get(final K key, final Supplier<V> valueSupplier, final Consumer<V> expireCallback) {
        final V value;
        V existingValue = cache.get(key);
        if (existingValue != null) {
            value = existingValue;
        } else {
            value = valueSupplier.get();
            cache.put(key, value);
        }
        final var future = futures.remove(key);
        if (future != null) {
            future.cancel(true);
        }
        futures.put(key, executor.schedule(() -> {
            synchronized (SimpleCache.this) {
                futures.remove(key);
                final var removedValue = cache.remove(key);
                if (removedValue != null) {
                    expireCallback.accept(removedValue);
                }
            }
        }, timeoutMs, TimeUnit.MILLISECONDS));
        return value;
    }
}
