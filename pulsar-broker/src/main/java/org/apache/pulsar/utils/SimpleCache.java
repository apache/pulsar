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
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;

public class SimpleCache<K, V> {

    private final Map<K, ExpirableValue<V>> cache = new HashMap<>();
    private final long timeoutMs;

    @RequiredArgsConstructor
    private class ExpirableValue<V> {

        private final V value;
        private final Consumer<V> expireCallback;
        private long deadlineMs;

        boolean tryExpire() {
            if (System.currentTimeMillis() >= deadlineMs) {
                expireCallback.accept(value);
                return true;
            } else {
                return false;
            }
        }

        void updateDeadline() {
            deadlineMs = System.currentTimeMillis() + timeoutMs;
        }
    }

    public SimpleCache(final ScheduledExecutorService scheduler, final long timeoutMs, final long frequencyMs) {
        this.timeoutMs = timeoutMs;
        scheduler.scheduleAtFixedRate(() -> {
            synchronized (SimpleCache.this) {
                final var keys = new HashSet<K>();
                cache.forEach((key, value) -> {
                    if (value.tryExpire()) {
                        keys.add(key);
                    }
                });
                cache.keySet().removeAll(keys);
            }
        }, frequencyMs, frequencyMs, TimeUnit.MILLISECONDS);
    }

    public synchronized V get(final K key, final Supplier<V> valueSupplier, final Consumer<V> expireCallback) {
        final var value = cache.get(key);
        if (value != null) {
            value.updateDeadline();
            return value.value;
        }

        final var newValue = new ExpirableValue<>(valueSupplier.get(), expireCallback);
        newValue.updateDeadline();
        cache.put(key, newValue);
        return newValue.value;
    }
}
