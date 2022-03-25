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

import com.google.common.collect.Lists;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PoolChunkListMetric;
import io.netty.buffer.PoolChunkMetric;
import io.netty.buffer.PoolSubpageMetric;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.stream.Collectors;
import org.apache.bookkeeper.mledger.impl.EntryCacheImpl;
import org.apache.pulsar.common.stats.AllocatorStats;
import org.apache.pulsar.common.stats.AllocatorStats.PoolArenaStats;
import org.apache.pulsar.common.stats.AllocatorStats.PoolChunkListStats;
import org.apache.pulsar.common.stats.AllocatorStats.PoolChunkStats;
import org.apache.pulsar.common.stats.AllocatorStats.PoolSubpageStats;

public class AllocatorStatsGenerator {
    public static AllocatorStats generate(String allocatorName) {
        PooledByteBufAllocator allocator;
        if ("default".equals(allocatorName)) {
            allocator = PooledByteBufAllocator.DEFAULT;
        } else if ("ml-cache".equals(allocatorName)) {
            allocator = EntryCacheImpl.ALLOCATOR;
        } else {
            throw new IllegalArgumentException("Invalid allocator name : " + allocatorName);
        }

        AllocatorStats stats = new AllocatorStats();
        stats.directArenas = allocator.metric().directArenas().stream()
            .map(AllocatorStatsGenerator::newPoolArenaStats)
            .collect(Collectors.toList());
        stats.heapArenas = allocator.metric().heapArenas().stream()
            .map(AllocatorStatsGenerator::newPoolArenaStats)
            .collect(Collectors.toList());

        stats.numDirectArenas = allocator.metric().numDirectArenas();
        stats.numHeapArenas = allocator.metric().numHeapArenas();
        stats.numThreadLocalCaches = allocator.metric().numThreadLocalCaches();
        stats.normalCacheSize = allocator.metric().normalCacheSize();
        stats.smallCacheSize = allocator.metric().smallCacheSize();
        return stats;
    }

    private static PoolArenaStats newPoolArenaStats(PoolArenaMetric m) {
        PoolArenaStats stats = new PoolArenaStats();
        stats.numSmallSubpages = m.numSmallSubpages();
        stats.numChunkLists = m.numChunkLists();

        stats.smallSubpages = m.smallSubpages().stream()
            .map(AllocatorStatsGenerator::newPoolSubpageStats)
            .collect(Collectors.toList());
        stats.chunkLists = m.chunkLists().stream()
            .map(AllocatorStatsGenerator::newPoolChunkListStats)
            .collect(Collectors.toList());

        stats.numAllocations = m.numAllocations();
        stats.numSmallAllocations = m.numSmallAllocations();
        stats.numNormalAllocations = m.numNormalAllocations();
        stats.numHugeAllocations = m.numHugeAllocations();
        stats.numDeallocations = m.numDeallocations();
        stats.numSmallDeallocations = m.numSmallDeallocations();
        stats.numNormalDeallocations = m.numNormalDeallocations();
        stats.numHugeDeallocations = m.numHugeDeallocations();
        stats.numActiveAllocations = m.numActiveAllocations();
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
