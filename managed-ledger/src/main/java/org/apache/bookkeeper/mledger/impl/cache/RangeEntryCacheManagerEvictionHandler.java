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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheManagerImpl.MB;
import com.google.common.annotations.VisibleForTesting;
import lombok.CustomLog;
import org.apache.commons.lang3.tuple.Pair;

@CustomLog
class RangeEntryCacheManagerEvictionHandler {
    private final RangeEntryCacheManagerImpl manager;
    private final RangeCacheRemovalQueue rangeCacheRemovalQueue;

    public RangeEntryCacheManagerEvictionHandler(RangeEntryCacheManagerImpl manager,
                                                 RangeCacheRemovalQueue rangeCacheRemovalQueue) {
        this.manager = manager;
        this.rangeCacheRemovalQueue = rangeCacheRemovalQueue;
    }

    /**
     * Invalidate all entries in the cache which were created before the given timestamp.
     *
     * @param timestamp the timestamp before which entries will be invalidated
     */
    public void invalidateEntriesBeforeTimestampNanos(long timestamp) {
        Pair<Integer, Long> evictedPair = rangeCacheRemovalQueue.evictLEntriesBeforeTimestamp(timestamp);
        manager.entriesRemoved(evictedPair.getRight(), evictedPair.getLeft());
    }

    /**
     * Force the cache to drop entries to free space.
     *
     * @param sizeToFree the total memory size to free
     * @return a pair containing the number of entries evicted and their total size
     */
    public Pair<Integer, Long> evictEntries(long sizeToFree) {
        checkArgument(sizeToFree > 0);
        Pair<Integer, Long> evicted = rangeCacheRemovalQueue.evictLeastAccessedEntries(sizeToFree);
        int evictedEntries = evicted.getLeft();
        long evictedSize = evicted.getRight();
        log.debug().attr("sizeToFreeMb", sizeToFree / MB)
                .attr("evictedEntries", evictedEntries)
                .attr("evictedSizeMb", evictedSize / MB)
                .attr("currentSizeMb", () -> manager.getSize() / MB)
                .log("Doing cache eviction");
        manager.entriesRemoved(evictedSize, evictedEntries);
        return evicted;
    }

    @VisibleForTesting
    public Pair<Integer, Long> getNonEvictableSize() {
        return rangeCacheRemovalQueue.getNonEvictableSize();
    }
}
