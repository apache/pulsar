/*******************************************************************************
 * Copyright 2014 Trevor Robinson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.scurrilous.circe.impl;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Provides access to a singleton hash function cache, if an implementation is
 * available.
 */
public final class HashCacheLoader {

    private static final HashCache HASH_CACHE;

    static {
        final Iterator<HashCache> iterator = ServiceLoader.load(HashCache.class).iterator();
        HASH_CACHE = iterator.hasNext() ? iterator.next() : null;
    }

    /**
     * Returns whether a hash function cache is available.
     * 
     * @return true if a cache is available, false if {@link #getCache} will
     *         throw an exception
     */
    public static boolean hasCache() {
        return HASH_CACHE != null;
    }

    /**
     * Returns the single hash function cache.
     * 
     * @return the single hash cache
     * @throws UnsupportedOperationException if no hash cache is available
     */
    public static HashCache getCache() {
        if (HASH_CACHE == null)
            throw new UnsupportedOperationException();
        return HASH_CACHE;
    }

    private HashCacheLoader() {
    }
}
