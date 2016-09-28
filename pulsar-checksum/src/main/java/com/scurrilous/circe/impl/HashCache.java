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

import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.scurrilous.circe.Hash;
import com.scurrilous.circe.HashParameters;
import com.scurrilous.circe.HashSupport;

/**
 * Interface implemented by hash function caches.
 */
public interface HashCache {

    /**
     * Requests a cached hash function with the given parameters and required
     * support flags. If no matching function is cached, the given loader is
     * called to obtain one to cache.
     * 
     * @param params the hash algorithm parameters
     * @param required the required hash support flags
     * @param loader a cache loader that creates the function if not cached
     * @return a hash with the given parameters and support flags
     * @throws ExecutionException if the loader throws an exception
     */
    Hash get(HashParameters params, EnumSet<HashSupport> required, Callable<Hash> loader)
            throws ExecutionException;
}
