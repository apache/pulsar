/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.stats.metrics;

import java.util.List;

import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.impl.EntryCacheImpl;

import com.google.common.collect.Lists;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.stats.Metrics;

import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PoolChunkListMetric;
import io.netty.buffer.PoolChunkMetric;
import io.netty.buffer.PooledByteBufAllocator;

public class ManagedLedgerCacheMetrics extends AbstractMetrics {

    private List<Metrics> metrics;
    public ManagedLedgerCacheMetrics(PulsarService pulsar) {
        super(pulsar);
        this.metrics = Lists.newArrayList();
    }

    @Override
    public synchronized List<Metrics> generate() {

        // get the ML cache stats bean

        ManagedLedgerFactoryMXBean mlCacheStats = getManagedLedgerCacheStats();

        Metrics m = createMetrics();

        m.put("brk_ml_count", mlCacheStats.getNumberOfManagedLedgers());
        m.put("brk_ml_cache_used_size", mlCacheStats.getCacheUsedSize());
        m.put("brk_ml_cache_evictions", mlCacheStats.getNumberOfCacheEvictions());
        m.put("brk_ml_cache_hits_rate", mlCacheStats.getCacheHitsRate());
        m.put("brk_ml_cache_misses_rate", mlCacheStats.getCacheMissesRate());
        m.put("brk_ml_cache_hits_throughput", mlCacheStats.getCacheHitsThroughput());
        m.put("brk_ml_cache_misses_throughput", mlCacheStats.getCacheMissesThroughput());

        PooledByteBufAllocator allocator = EntryCacheImpl.allocator;
        long activeAllocations = 0;
        long activeAllocationsTiny = 0;
        long activeAllocationsSmall = 0;
        long activeAllocationsNormal = 0;
        long activeAllocationsHuge = 0;
        long totalAllocated = 0;
        long totalUsed = 0;

        for (PoolArenaMetric arena : allocator.directArenas()) {
            activeAllocations += arena.numActiveAllocations();
            activeAllocationsTiny += arena.numActiveTinyAllocations();
            activeAllocationsSmall += arena.numActiveSmallAllocations();
            activeAllocationsNormal += arena.numActiveNormalAllocations();
            activeAllocationsHuge += arena.numActiveHugeAllocations();

            for (PoolChunkListMetric list : arena.chunkLists()) {
                for (PoolChunkMetric chunk : list) {
                    int size = chunk.chunkSize();
                    int used = size - chunk.freeBytes();

                    totalAllocated += size;
                    totalUsed += used;
                }
            }
        }

        m.put("brk_ml_cache_pool_allocated", totalAllocated);
        m.put("brk_ml_cache_pool_used", totalUsed);
        m.put("brk_ml_cache_pool_active_allocations", activeAllocations);
        m.put("brk_ml_cache_pool_active_allocations_tiny", activeAllocationsTiny);
        m.put("brk_ml_cache_pool_active_allocations_small", activeAllocationsSmall);
        m.put("brk_ml_cache_pool_active_allocations_normal", activeAllocationsNormal);
        m.put("brk_ml_cache_pool_active_allocations_huge", activeAllocationsHuge);

        metrics.clear();
        metrics.add(m);
        return metrics;

    }

}
