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
package org.apache.pulsar.common.stats;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CacheMetricsCollector {
    private static final RemovableCacheMetricsCollector CAFFEINE_COLLECTOR = new RemovableCacheMetricsCollector();
    public static final io.prometheus.client.cache.caffeine.CacheMetricsCollector CAFFEINE =
        CAFFEINE_COLLECTOR.register();

    /** Remove only this cache's metrics, preserving a newer cache registered with the same name. */
    public static void removeCache(String name, AsyncCache<?, ?> cache) {
        CAFFEINE_COLLECTOR.removeCache(name, cache.synchronous());
    }

    // The collector has the same simple name as this utility class, so its type must be qualified.
    private static final class RemovableCacheMetricsCollector
            extends io.prometheus.client.cache.caffeine.CacheMetricsCollector {
        private void removeCache(String name, Cache<?, ?> cache) {
            children.computeIfPresent(name, (key, current) -> current == cache ? null : current);
        }
    }
}
