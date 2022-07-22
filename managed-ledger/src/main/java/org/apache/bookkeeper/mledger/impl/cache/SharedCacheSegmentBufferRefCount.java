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
package org.apache.bookkeeper.mledger.impl.cache;

import io.netty.buffer.ByteBuf;
import io.netty.util.IllegalReferenceCountException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.common.util.collections.ConcurrentLongPairObjectHashMap;

class SharedCacheSegmentBufferRefCount implements SharedCacheSegment {

    private final AtomicInteger currentSize = new AtomicInteger();
    private final ConcurrentLongPairObjectHashMap<ByteBuf> index;
    private final int segmentSize;

    SharedCacheSegmentBufferRefCount(int segmentSize) {
        this.segmentSize = segmentSize;
        this.index = ConcurrentLongPairObjectHashMap.<ByteBuf>newBuilder()
                // We are going to often clear() the map, with the expectation that it's going to get filled again
                // immediately after. In these conditions it does not make sense to shrink it each time.
                .autoShrink(false)
                .concurrencyLevel(Runtime.getRuntime().availableProcessors() * 2)
                .build();
    }

    @Override
    public boolean insert(long ledgerId, long entryId, ByteBuf entry) {
        int newSize = currentSize.addAndGet(entry.readableBytes());

        if (newSize > segmentSize) {
            // The segment is full
            return false;
        } else {
            // Insert entry into read cache segment
            ByteBuf oldValue = index.putIfAbsent(ledgerId, entryId, entry.retain());
            if (oldValue != null) {
                entry.release();
                return false;
            } else {
                return true;
            }
        }
    }

    @Override
    public ByteBuf get(long ledgerId, long entryId) {
        ByteBuf entry = index.get(ledgerId, entryId);
        if (entry != null) {
            try {
                return entry.retain();
            } catch (IllegalReferenceCountException e) {
                // Entry was removed between the get() and the retain() calls
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public int getSize() {
        return currentSize.get();
    }

    @Override
    public void close() {
        clear();
    }

    @Override
    public void clear() {
        index.forEach((ledgerId, entryId, e) -> e.release());
        index.clear();
    }
}
