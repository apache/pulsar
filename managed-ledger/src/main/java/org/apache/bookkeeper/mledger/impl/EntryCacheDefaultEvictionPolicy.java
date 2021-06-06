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
package org.apache.bookkeeper.mledger.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.reverseOrder;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default eviction policy.
 *
 * <p/>This policy consider only the bigger caches for doing eviction.
 *
 * <p/>The PercentOfSizeToConsiderForEviction parameter should always be bigger than the cacheEvictionWatermak,
 * otherwisethe eviction cycle will free less memory than what was required.
 */
public class EntryCacheDefaultEvictionPolicy implements EntryCacheEvictionPolicy {

    private static final double PercentOfSizeToConsiderForEviction = 0.5;

    @Override
    public void doEviction(List<EntryCache> caches, long sizeToFree) {
        checkArgument(sizeToFree > 0);
        checkArgument(!caches.isEmpty());

        caches.sort(reverseOrder());

        long totalSize = 0;
        for (EntryCache cache : caches) {
            totalSize += cache.getSize();
        }

        // This algorithm apply the eviction only the group of caches whose combined size reaches the
        // PercentOfSizeToConsiderForEviction
        List<EntryCache> cachesToEvict = Lists.newArrayList();
        long cachesToEvictTotalSize = 0;
        long sizeToConsiderForEviction = (long) (totalSize * PercentOfSizeToConsiderForEviction);
        log.debug("Need to gather at least {} from caches", sizeToConsiderForEviction);

        int cacheIdx = 0;
        while (cachesToEvictTotalSize < sizeToConsiderForEviction) {
            // This condition should always be true, considering that we cannot free more size that what we have in
            // cache
            checkArgument(cacheIdx < caches.size());

            EntryCache entryCache = caches.get(cacheIdx++);
            cachesToEvictTotalSize += entryCache.getSize();
            cachesToEvict.add(entryCache);

            log.debug("Added cache {} with size {}", entryCache.getName(), entryCache.getSize());
        }

        int evictedEntries = 0;
        long evictedSize = 0;

        for (EntryCache entryCache : cachesToEvict) {
            // To each entryCache chosen to for eviction, we'll ask to evict a proportional amount of data
            long singleCacheSizeToFree = (long) (sizeToFree * (entryCache.getSize() / (double) cachesToEvictTotalSize));

            if (singleCacheSizeToFree == 0) {
                // If the size of this cache went to 0, it probably means that its entries has been removed from the
                // cache since the time we've computed the ranking
                continue;
            }

            Pair<Integer, Long> evicted = entryCache.evictEntries(singleCacheSizeToFree);
            evictedEntries += evicted.getLeft();
            evictedSize += evicted.getRight();
        }

        log.info("Completed cache eviction. Removed {} entries from {} caches. ({} Mb)", evictedEntries,
                cachesToEvict.size(), evictedSize / EntryCacheManager.MB);
    }

    private static final Logger log = LoggerFactory.getLogger(EntryCacheDefaultEvictionPolicy.class);
}
