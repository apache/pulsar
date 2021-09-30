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
package org.apache.pulsar.common.stats;

import java.util.List;

/**
 * Allocator statistics.
 */
public class AllocatorStats {
    public int numDirectArenas;
    public int numHeapArenas;
    public int numThreadLocalCaches;
    public int normalCacheSize;
    public int smallCacheSize;

    public List<PoolArenaStats> directArenas;
    public List<PoolArenaStats> heapArenas;

    /**
     * Pool arena statistics.
     */
    public static class PoolArenaStats {
        public int numSmallSubpages;
        public int numChunkLists;

        public List<PoolSubpageStats> smallSubpages;
        public List<PoolChunkListStats> chunkLists;
        public long numAllocations;
        public long numSmallAllocations;
        public long numNormalAllocations;
        public long numHugeAllocations;
        public long numDeallocations;
        public long numSmallDeallocations;
        public long numNormalDeallocations;
        public long numHugeDeallocations;
        public long numActiveAllocations;
        public long numActiveSmallAllocations;
        public long numActiveNormalAllocations;
        public long numActiveHugeAllocations;
    }

    /**
     * Pool subpage statistics.
     */
    public static class PoolSubpageStats {
        public int maxNumElements;
        public int numAvailable;
        public int elementSize;
        public int pageSize;
    }

    /**
     * Pool chunk list statistics.
     */
    public static class PoolChunkListStats {
        public int minUsage;
        public int maxUsage;
        public List<PoolChunkStats> chunks;
    }

    /**
     * Pool chunk statistics.
     */
    public static class PoolChunkStats {
        public int usage;
        public int chunkSize;
        public int freeBytes;
    }
}
