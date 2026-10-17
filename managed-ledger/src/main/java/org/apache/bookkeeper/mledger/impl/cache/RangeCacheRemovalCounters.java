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

import io.netty.util.Recycler;

/**
 * Mutable object to store the number of entries and the total size removed from the cache. The instances
 * are recycled to avoid creating new instances.
 */
class RangeCacheRemovalCounters {
    private final Recycler.Handle<RangeCacheRemovalCounters> recyclerHandle;
    private static final Recycler<RangeCacheRemovalCounters> RECYCLER = new Recycler<RangeCacheRemovalCounters>() {
        @Override
        protected RangeCacheRemovalCounters newObject(Handle<RangeCacheRemovalCounters> recyclerHandle) {
            return new RangeCacheRemovalCounters(recyclerHandle);
        }
    };
    int removedEntries;
    long removedSize;

    private RangeCacheRemovalCounters(Recycler.Handle<RangeCacheRemovalCounters> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    static <T> RangeCacheRemovalCounters create() {
        RangeCacheRemovalCounters results = RECYCLER.get();
        results.removedEntries = 0;
        results.removedSize = 0;
        return results;
    }

    void recycle() {
        removedEntries = 0;
        removedSize = 0;
        recyclerHandle.recycle(this);
    }

    public void entryRemoved(long size) {
        removedSize += size;
        removedEntries++;
    }
}
