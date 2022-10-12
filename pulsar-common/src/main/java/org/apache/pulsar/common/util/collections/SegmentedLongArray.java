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
package org.apache.pulsar.common.util.collections;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;

@NotThreadSafe
public class SegmentedLongArray implements AutoCloseable {

    private static final int SIZE_OF_LONG = 8;

    private static final int MAX_SEGMENT_SIZE = 2 * 1024 * 1024; // 2M longs -> 16 MB
    private final List<ByteBuf> buffers = new ArrayList<>();

    @Getter
    private final long initialCapacity;

    @Getter
    private long capacity;

    public SegmentedLongArray(long initialCapacity) {
        long remainingToAdd = initialCapacity;

        // Add first segment
        int sizeToAdd = (int) Math.min(remainingToAdd, MAX_SEGMENT_SIZE);
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(sizeToAdd * SIZE_OF_LONG);
        buffer.writerIndex(sizeToAdd * SIZE_OF_LONG);
        buffers.add(buffer);
        remainingToAdd -= sizeToAdd;

        // Add the remaining segments, all at full segment size, if necessary
        while (remainingToAdd > 0) {
            buffer = PooledByteBufAllocator.DEFAULT.directBuffer(MAX_SEGMENT_SIZE * SIZE_OF_LONG);
            buffer.writerIndex(MAX_SEGMENT_SIZE * SIZE_OF_LONG);
            buffers.add(buffer);
            remainingToAdd -= MAX_SEGMENT_SIZE;
        }

        this.initialCapacity = initialCapacity;
        this.capacity = this.initialCapacity;
    }

    public void writeLong(long offset, long value) {
        int bufferIdx = (int) (offset / MAX_SEGMENT_SIZE);
        int internalIdx = (int) (offset % MAX_SEGMENT_SIZE);
        buffers.get(bufferIdx).setLong(internalIdx * SIZE_OF_LONG, value);
    }

    public long readLong(long offset) {
        int bufferIdx = (int) (offset / MAX_SEGMENT_SIZE);
        int internalIdx = (int) (offset % MAX_SEGMENT_SIZE);
        return buffers.get(bufferIdx).getLong(internalIdx * SIZE_OF_LONG);
    }

    public void increaseCapacity() {
        if (capacity < MAX_SEGMENT_SIZE) {
            // Resize the current buffer to bigger capacity
            capacity += (capacity <= 256 ? capacity : capacity / 2);
            capacity = Math.min(capacity, MAX_SEGMENT_SIZE);
            buffers.get(0).capacity((int) this.capacity * SIZE_OF_LONG);
            buffers.get(0).writerIndex((int) this.capacity * SIZE_OF_LONG);
        } else {
            // Let's add 1 mode buffer to the list
            int bufferSize = MAX_SEGMENT_SIZE * SIZE_OF_LONG;
            ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(bufferSize, bufferSize);
            buffer.writerIndex(bufferSize);
            buffers.add(buffer);
            capacity += MAX_SEGMENT_SIZE;
        }
    }

    public void shrink(long newCapacity) {
        if (newCapacity >= capacity || newCapacity < initialCapacity) {
            return;
        }

        long sizeToReduce = capacity - newCapacity;
        while (sizeToReduce >= MAX_SEGMENT_SIZE && buffers.size() > 1) {
            ByteBuf b = buffers.remove(buffers.size() - 1);
            b.release();
            capacity -= MAX_SEGMENT_SIZE;
            sizeToReduce -= MAX_SEGMENT_SIZE;
        }

        if (buffers.size() == 1 && sizeToReduce > 0) {
            // We should also reduce the capacity of the first buffer
            capacity -= sizeToReduce;
            ByteBuf oldBuffer = buffers.get(0);
            ByteBuf newBuffer = PooledByteBufAllocator.DEFAULT.directBuffer((int) capacity * SIZE_OF_LONG);
            oldBuffer.getBytes(0, newBuffer, (int) capacity * SIZE_OF_LONG);
            oldBuffer.release();
            buffers.set(0, newBuffer);
        }
    }

    @Override
    public void close() {
        buffers.forEach(ByteBuf::release);
    }

    /**
     * The amount of memory used to back the array of longs.
     */
    public long bytesCapacity() {
        return capacity * SIZE_OF_LONG;
    }
}
