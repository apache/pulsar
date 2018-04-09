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

import java.util.List;

/**
 * Cache eviction policy abstraction interface.
 *
 */
public interface EntryCacheEvictionPolicy {
    /**
     * Perform the cache eviction of at least sizeToFree bytes on the supplied list of caches.
     *
     * @param caches
     *            the list of caches to consider
     * @param sizeToFree
     *            the minimum size in bytes to be freed
     */
    void doEviction(List<EntryCache> caches, long sizeToFree);
}
