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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.collections.ConcurrentLongLongPairHashMap;

class SharedCacheSegmentBufferCopy implements AutoCloseable, SharedCacheSegment {

    private final ByteBuf cacheBuffer;
    private final AtomicInteger currentOffset = new AtomicInteger();
    private final ConcurrentLongLongPairHashMap index;
    private final int segmentSize;

    private static final int ALIGN_64_MASK = ~(64 - 1);

    SharedCacheSegmentBufferCopy(int segmentSize) {
        this.segmentSize = segmentSize;
        this.cacheBuffer = PulsarByteBufAllocator.DEFAULT.buffer(segmentSize, segmentSize);
        this.cacheBuffer.writerIndex(segmentSize - 1);
        this.index = ConcurrentLongLongPairHashMap.newBuilder()
                // We are going to often clear() the map, with the expectation that it's going to get filled again
                // immediately after. In these conditions it does not make sense to shrink it each time.
                .autoShrink(false)
                .concurrencyLevel(Runtime.getRuntime().availableProcessors() * 8)
                .build();
    }

    @Override
    public boolean insert(long ledgerId, long entryId, ByteBuf entry) {
        int entrySize = entry.readableBytes();
        int alignedSize = align64(entrySize);
        int offset = currentOffset.getAndAdd(alignedSize);

        if (offset + entrySize > segmentSize) {
            // The segment is full
            return false;
        } else {
            // Copy entry into read cache segment
            cacheBuffer.setBytes(offset, entry, entry.readerIndex(), entry.readableBytes());
            long value = offset << 32 | entrySize;
            index.put(ledgerId, entryId, value, 0);
            return true;
        }
    }

    @Override
    public ByteBuf get(long ledgerId, long entryId) {
        long value = index.getFirstValue(ledgerId, entryId);
        if (value >= 0) {
            int offset = (int) (value >> 32);
            int entryLen = (int) value;

            ByteBuf entry = PulsarByteBufAllocator.DEFAULT.buffer(entryLen, entryLen);
            entry.writeBytes(cacheBuffer, offset, entryLen);
            return entry;
        } else {
            return null;
        }
    }

    @Override
    public int getSize() {
        return currentOffset.get();
    }

    @Override
    public void close() {
        cacheBuffer.release();
    }

    private static int align64(int size) {
        return (size + 64 - 1) & ALIGN_64_MASK;
    }

    @Override
    public void clear() {
        index.clear();
        currentOffset.set(0);
    }
}
