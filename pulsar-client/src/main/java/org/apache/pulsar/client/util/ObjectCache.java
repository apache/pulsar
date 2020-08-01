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
package org.apache.pulsar.client.util;

import java.util.function.Supplier;
import java.time.Clock;
import java.util.concurrent.TimeUnit;

public class ObjectCache<T> implements Supplier<T> {

    private final Supplier<T> supplier;
    private T cachedInstance;

    private final long cacheDurationMillis;
    private long lastRefreshTimestamp;
    private final Clock clock;

    public ObjectCache(Supplier<T> supplier, long cacheDuration, TimeUnit unit) {
        this(supplier, cacheDuration, unit, Clock.systemUTC());
    }

    ObjectCache(Supplier<T> supplier, long cacheDuration, TimeUnit unit, Clock clock) {
        this.supplier = supplier;
        this.cachedInstance = null;
        this.cacheDurationMillis = unit.toMillis(cacheDuration);
        this.clock = clock;
    }

    public synchronized T get() {
        long now = clock.millis();
        if (cachedInstance == null || (now - lastRefreshTimestamp) >= cacheDurationMillis) {
            cachedInstance = supplier.get();
            lastRefreshTimestamp = now;
        }

        return cachedInstance;
    }
}
