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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class OffsetsCache implements AutoCloseable {
    private static final int CACHE_TTL_SECONDS =
            Integer.getInteger("pulsar.jclouds.readhandleimpl.offsetsscache.ttl.seconds", 5 * 60);
    // limit the cache size to avoid OOM
    // 1 million entries consumes about 60MB of heap space
    private static final int CACHE_MAX_SIZE =
            Integer.getInteger("pulsar.jclouds.readhandleimpl.offsetsscache.max.size", 1_000_000);
    private final ScheduledExecutorService cacheEvictionExecutor;

    record Key(long ledgerId, long entryId) {

    }

    private final Cache<OffsetsCache.Key, Long> entryOffsetsCache;

    public OffsetsCache() {
        if (CACHE_MAX_SIZE > 0) {
            entryOffsetsCache = CacheBuilder
                    .newBuilder()
                    .expireAfterAccess(CACHE_TTL_SECONDS, TimeUnit.SECONDS)
                    .maximumSize(CACHE_MAX_SIZE)
                    .build();
            cacheEvictionExecutor =
                    Executors.newSingleThreadScheduledExecutor(
                            new ThreadFactoryBuilder().setNameFormat("jcloud-offsets-cache-eviction").build());
            int period = Math.max(CACHE_TTL_SECONDS / 2, 1);
            cacheEvictionExecutor.scheduleAtFixedRate(() -> {
                entryOffsetsCache.cleanUp();
            }, period, period, TimeUnit.SECONDS);
        } else {
            cacheEvictionExecutor = null;
            entryOffsetsCache = null;
        }
    }

    public void put(long ledgerId, long entryId, long currentPosition) {
        if (entryOffsetsCache != null) {
            entryOffsetsCache.put(new Key(ledgerId, entryId), currentPosition);
        }
    }

    public Long getIfPresent(long ledgerId, long entryId) {
        return entryOffsetsCache != null ? entryOffsetsCache.getIfPresent(new Key(ledgerId, entryId)) : null;
    }

    public void clear() {
        if (entryOffsetsCache != null) {
            entryOffsetsCache.invalidateAll();
        }
    }

    @Override
    public void close() {
        if (cacheEvictionExecutor != null) {
            cacheEvictionExecutor.shutdownNow();
        }
    }
}
