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
package org.apache.pulsar.broker.stats;

import java.util.stream.Collectors;

import org.apache.bookkeeper.mledger.impl.EntryCacheImpl;
import org.apache.pulsar.common.stats.AllocatorStats;
import org.apache.pulsar.common.stats.AllocatorStats.PoolArenaStats;
import org.apache.pulsar.common.stats.AllocatorStats.PoolChunkListStats;
import org.apache.pulsar.common.stats.AllocatorStats.PoolChunkStats;
import org.apache.pulsar.common.stats.AllocatorStats.PoolSubpageStats;

import com.google.common.collect.Lists;

import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PoolChunkListMetric;
import io.netty.buffer.PoolChunkMetric;
import io.netty.buffer.PoolSubpageMetric;
import io.netty.buffer.PooledByteBufAllocator;

public class AllocatorStatsGenerator {
    public static AllocatorStats generate(String allocatorName) {
        PooledByteBufAllocator allocator = null;
        if ("default".equals(allocatorName)) {
            allocator = PooledByteBufAllocator.DEFAULT;
        } else if ("ml-cache".equals(allocatorName)) {
            allocator = EntryCacheImpl.allocator;
        } else {
            throw new IllegalArgumentException("Invalid allocator name : " + allocatorName);
        }

        AllocatorStats stats = new AllocatorStats();
        stats.directArenas = allocator.directArenas().stream().map(x -> newPoolArenaStats(x))
                .collect(Collectors.toList());
        stats.heapArenas = allocator.heapArenas().stream().map(x -> newPoolArenaStats(x)).collect(Collectors.toList());

        stats.numDirectArenas = allocator.numDirectArenas();
        stats.numHeapArenas = allocator.numHeapArenas();
        stats.numThreadLocalCaches = allocator.numThreadLocalCaches();
        stats.normalCacheSize = allocator.normalCacheSize();
        stats.smallCacheSize = allocator.smallCacheSize();
        stats.tinyCacheSize = allocator.tinyCacheSize();
        return stats;
    }

    private static PoolArenaStats newPoolArenaStats(PoolArenaMetric m) {
        PoolArenaStats stats = new PoolArenaStats();
        stats.numTinySubpages = m.numTinySubpages();
        stats.numSmallSubpages = m.numSmallSubpages();
        stats.numChunkLists = m.numChunkLists();

        stats.tinySubpages = m.tinySubpages().stream().map(x -> newPoolSubpageStats(x)).collect(Collectors.toList());
        stats.smallSubpages = m.smallSubpages().stream().map(x -> newPoolSubpageStats(x)).collect(Collectors.toList());
        stats.chunkLists = m.chunkLists().stream().map(x -> newPoolChunkListStats(x)).collect(Collectors.toList());

        stats.numAllocations = m.numAllocations();
        stats.numTinyAllocations = m.numTinyAllocations();
        stats.numSmallAllocations = m.numSmallAllocations();
        stats.numNormalAllocations = m.numNormalAllocations();
        stats.numHugeAllocations = m.numHugeAllocations();
        stats.numDeallocations = m.numDeallocations();
        stats.numTinyDeallocations = m.numTinyDeallocations();
        stats.numSmallDeallocations = m.numSmallDeallocations();
        stats.numNormalDeallocations = m.numNormalDeallocations();
        stats.numHugeDeallocations = m.numHugeDeallocations();
        stats.numActiveAllocations = m.numActiveAllocations();
        stats.numActiveTinyAllocations = m.numActiveTinyAllocations();
        stats.numActiveSmallAllocations = m.numActiveSmallAllocations();
        stats.numActiveNormalAllocations = m.numActiveNormalAllocations();
        stats.numActiveHugeAllocations = m.numActiveHugeAllocations();
        return stats;
    }

    private static PoolSubpageStats newPoolSubpageStats(PoolSubpageMetric m) {
        PoolSubpageStats stats = new PoolSubpageStats();
        stats.maxNumElements = m.maxNumElements();
        stats.numAvailable = m.numAvailable();
        stats.elementSize = m.elementSize();
        stats.pageSize = m.pageSize();
        return stats;
    }

    private static PoolChunkListStats newPoolChunkListStats(PoolChunkListMetric m) {
        PoolChunkListStats stats = new PoolChunkListStats();
        stats.minUsage = m.minUsage();
        stats.maxUsage = m.maxUsage();
        stats.chunks = Lists.newArrayList();
        m.forEach(chunk -> stats.chunks.add(newPoolChunkStats(chunk)));
        return stats;
    }

    private static PoolChunkStats newPoolChunkStats(PoolChunkMetric m) {
        PoolChunkStats stats = new PoolChunkStats();
        stats.usage = m.usage();
        stats.chunkSize = m.chunkSize();
        stats.freeBytes = m.freeBytes();
        return stats;
    }
}
