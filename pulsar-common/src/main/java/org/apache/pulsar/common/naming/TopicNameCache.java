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
package org.apache.pulsar.common.naming;

import java.util.concurrent.TimeUnit;

/**
 * A cache for TopicName instances that allows deduplication and efficient memory usage.
 * It uses soft references to allow garbage collection of unused TopicName instances under heavy memory pressure.
 * This cache uses ConcurrentHashMap for lookups for performance over Guava Cache and Caffeine Cache
 * since there was a concern in https://github.com/apache/pulsar/pull/23052 about high CPU usage for cache lookups.
 */
class TopicNameCache extends NameCache<TopicName> {
    static final TopicNameCache INSTANCE = new TopicNameCache();
    // Configuration for the cache. These settings aren't currently exposed to end users.
    static int cacheMaxSize = 100000;
    static int reduceSizeByPercentage = 25;
    static long referenceQueuePurgeIntervalNanos = TimeUnit.SECONDS.toNanos(10);

    @Override
    protected TopicName createValue(String key) {
        return new TopicName(key);
    }

    @Override
    protected int getCacheMaxSize() {
        return cacheMaxSize;
    }

    @Override
    protected int getReduceSizeByPercentage() {
        return reduceSizeByPercentage;
    }

    @Override
    protected long getReferenceQueuePurgeIntervalNanos() {
        return referenceQueuePurgeIntervalNanos;
    }
}
