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
package org.apache.pulsar.sql.presto.util;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Cache size allocator.
 */
public class NoStrictCacheSizeAllocator implements CacheSizeAllocator {

    private final long maxCacheSize;
    private final LongAdder availableCacheSize;
    private final ReentrantReadWriteLock lock;

    public NoStrictCacheSizeAllocator(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        this.availableCacheSize = new LongAdder();
        this.availableCacheSize.add(maxCacheSize);
        this.lock = new ReentrantReadWriteLock();
    }

    public long getAvailableCacheSize() {
        try {
            lock.readLock().lock();
            if (availableCacheSize.longValue() < 0) {
                return 0;
            }
            return availableCacheSize.longValue();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * This operation will cost available cache size.
     * if the request size exceed the available size, it's should be allowed,
     * because maybe one entry size exceed the size and
     * the query must be finished, the available size will become invalid.
     *
     * @param size allocate size
     */
    public void allocate(long size) {
        try {
            lock.writeLock().lock();
            availableCacheSize.add(-size);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * This method used to release used cache size and add available cache size.
     * in normal case, the available size shouldn't exceed max cache size.
     *
     * @param size release size
     */
    public void release(long size) {
        try {
            lock.writeLock().lock();
            availableCacheSize.add(size);
            if (availableCacheSize.longValue() > maxCacheSize) {
                availableCacheSize.reset();
                availableCacheSize.add(maxCacheSize);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

}
