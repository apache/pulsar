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
package org.apache.bookkeeper.mledger;

import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * JMX Bean interface for ManagedLedgerFactory stats.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface ManagedLedgerFactoryMXBean {
    /**
     * Get the number of currently opened managed ledgers on the factory.
     */
    int getNumberOfManagedLedgers();

    /**
     * Get the size in byte used to store the entries payloads.
     */
    long getCacheUsedSize();

    /**
     * Get the configured maximum cache size.
     */
    long getCacheMaxSize();

    /**
     * Get the number of cache hits per second.
     */
    double getCacheHitsRate();

    /**
     * Get the number of cache misses per second.
     */
    double getCacheMissesRate();

    /**
     * Get the amount of data is retrieved from the cache in byte/s.
     */
    double getCacheHitsThroughput();

    /**
     * Get the amount of data is retrieved from the bookkeeper in byte/s.
     */
    double getCacheMissesThroughput();

    /**
     * Get the number of cache evictions during the last minute.
     */
    long getNumberOfCacheEvictions();

    /**
     * Cumulative number of entries inserted into the cache.
     */
    long getCacheInsertedEntriesCount();

    /**
     * Cumulative number of entries evicted from the cache.
     */
    long getCacheEvictedEntriesCount();

    /**
     * Current number of entries in the cache.
     */
    long getCacheEntriesCount();
}
