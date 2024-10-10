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
package org.apache.bookkeeper.mledger.impl.cache;

import io.netty.buffer.PooledByteBufAllocator;
import lombok.Value;

@Value
public class PooledByteBufAllocatorStats {

    public long activeAllocations;
    public long activeAllocationsSmall;
    public long activeAllocationsNormal;
    public long activeAllocationsHuge;

    public long totalAllocated;
    public long totalUsed;

    public PooledByteBufAllocatorStats(PooledByteBufAllocator allocator) {
        long activeAllocations = 0;
        long activeAllocationsSmall = 0;
        long activeAllocationsNormal = 0;
        long activeAllocationsHuge = 0;
        long totalAllocated = 0;
        long totalUsed = 0;

        for (var arena : allocator.metric().directArenas()) {
            activeAllocations += arena.numActiveAllocations();
            activeAllocationsSmall += arena.numActiveSmallAllocations();
            activeAllocationsNormal += arena.numActiveNormalAllocations();
            activeAllocationsHuge += arena.numActiveHugeAllocations();

            for (var list : arena.chunkLists()) {
                for (var chunk : list) {
                    int size = chunk.chunkSize();
                    int used = size - chunk.freeBytes();

                    totalAllocated += size;
                    totalUsed += used;
                }
            }
        }

        this.activeAllocations = activeAllocations;
        this.activeAllocationsSmall = activeAllocationsSmall;
        this.activeAllocationsNormal = activeAllocationsNormal;
        this.activeAllocationsHuge = activeAllocationsHuge;

        this.totalAllocated = totalAllocated;
        this.totalUsed = totalUsed;
    }
}
