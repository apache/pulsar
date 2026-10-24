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
package org.apache.bookkeeper.mledger;

import org.apache.bookkeeper.common.annotation.InterfaceAudience.LimitedPrivate;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;

/**
 * The broker's settings for reading offloaded ledgers, which an offloader factory applies to every offloader that it
 * creates: the cache of offloaded data that their read handles share.
 *
 * @param cacheSizeBytes the most bytes of offloaded data that the cache keeps; 0 keeps none, and only reads of the
 *                       same data at the same time share it. The data that reads are using and the fetches in
 *                       progress come on top of it
 * @param cacheExpirySeconds how long the cache keeps data that no read has used
 * @param prefetchChunks how many chunks after the one that a sequential read moves on to are fetched in the
 *                       background; 0 fetches none
 * @param prefetchThreads the threads that prefetch offloaded data, which also bound how many chunks are prefetched at
 *                        a time; a read fetches the data that it needs on its own thread
 */
@LimitedPrivate
@Evolving
public record OffloadedReadCacheConfig(long cacheSizeBytes, int cacheExpirySeconds, int prefetchChunks,
                                       int prefetchThreads) {
    /** The defaults, for an offloader created without the broker's settings. */
    public static final OffloadedReadCacheConfig DEFAULT = new OffloadedReadCacheConfig(64L * 1024 * 1024, 60, 1, 4);

    public OffloadedReadCacheConfig {
        if (cacheSizeBytes < 0 || cacheExpirySeconds < 1 || prefetchChunks < 0 || prefetchThreads < 1) {
            throw new IllegalArgumentException("Invalid offloaded read cache settings: cacheSizeBytes="
                    + cacheSizeBytes + " cacheExpirySeconds=" + cacheExpirySeconds + " prefetchChunks="
                    + prefetchChunks + " prefetchThreads=" + prefetchThreads);
        }
    }
}
