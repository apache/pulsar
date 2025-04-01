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
package org.apache.pulsar.broker.stats.metrics;

import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.impl.cache.PooledByteBufAllocatorStats;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.stats.Metrics;

public class ManagedLedgerCacheMetrics extends AbstractMetrics {

    private List<Metrics> metrics;
    public ManagedLedgerCacheMetrics(PulsarService pulsar) {
        super(pulsar);
        this.metrics = new ArrayList<>();
    }

    @Override
    public synchronized List<Metrics> generate() {

        // get the ML cache stats bean

        ManagedLedgerFactoryMXBean mlCacheStats = getManagedLedgerCacheStats();

        Metrics m = createMetrics();

        m.put("brk_ml_count", mlCacheStats.getNumberOfManagedLedgers());
        m.put("brk_ml_cache_used_size", mlCacheStats.getCacheUsedSize());
        m.put("brk_ml_cache_inserted_entries_total", mlCacheStats.getCacheInsertedEntriesCount());
        m.put("brk_ml_cache_evicted_entries_total", mlCacheStats.getCacheEvictedEntriesCount());
        m.put("brk_ml_cache_entries", mlCacheStats.getCacheEntriesCount());
        m.put("brk_ml_cache_evictions", mlCacheStats.getNumberOfCacheEvictions());
        m.put("brk_ml_cache_hits_rate", mlCacheStats.getCacheHitsRate());
        m.put("brk_ml_cache_misses_rate", mlCacheStats.getCacheMissesRate());
        m.put("brk_ml_cache_hits_throughput", mlCacheStats.getCacheHitsThroughput());
        m.put("brk_ml_cache_misses_throughput", mlCacheStats.getCacheMissesThroughput());

        var allocatorStats = new PooledByteBufAllocatorStats(RangeEntryCacheImpl.ALLOCATOR);
        m.put("brk_ml_cache_pool_allocated", allocatorStats.totalAllocated);
        m.put("brk_ml_cache_pool_used", allocatorStats.totalUsed);
        m.put("brk_ml_cache_pool_active_allocations", allocatorStats.activeAllocations);
        m.put("brk_ml_cache_pool_active_allocations_small", allocatorStats.activeAllocationsSmall);
        m.put("brk_ml_cache_pool_active_allocations_normal", allocatorStats.activeAllocationsNormal);
        m.put("brk_ml_cache_pool_active_allocations_huge", allocatorStats.activeAllocationsHuge);

        metrics.clear();
        metrics.add(m);
        return metrics;

    }

}
