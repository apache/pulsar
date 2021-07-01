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

/**
 * Cache size allocator.
 */
public interface CacheSizeAllocator {

    /**
     * Get available cache size.
     *
     * @return available cache size
     */
    public long getAvailableCacheSize();

    /**
     * Cost available cache.
     *
     * @param size allocate size
     */
    public void allocate(long size);

    /**
     * Release allocated cache size.
     *
     * @param size release size
     */
    public void release(long size);

}
