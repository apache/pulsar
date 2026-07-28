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
package org.apache.pulsar.common.util.collections;

import io.netty.buffer.ByteBuf;

/**
 * Factory for creating {@link LongBitmap} instances.
 */
public final class LongBitmaps {

    private LongBitmaps() {
        // utility class
    }

    /**
     * Creates a new empty thread-safe LongBitmap.
     *
     * @return a new LongBitmap instance
     */
    public static LongBitmap create() {
        return new ConcurrentRoaringBitmap();
    }

    /**
     * Deserializes a LongBitmap from a ByteBuf.
     *
     * <p>Advances the buffer's {@code readerIndex} by the number of bytes consumed. The
     * buffer may be heap-backed, direct, or a {@link io.netty.buffer.CompositeByteBuf} —
     * the implementation reads via {@link ByteBuf#nioBuffer(int, int)} without copying
     * when possible.
     *
     * <p>The serialized format is the standard 32-bit RoaringBitmap portable format, so
     * buffers produced by {@link LongBitmap#serialize()} round-trip exactly. Buffers in
     * other formats (e.g. {@code Roaring64Bitmap}) are rejected.
     *
     * @param buf the input buffer positioned at the start of the serialized bitmap
     * @return the deserialized LongBitmap
     * @throws RuntimeException if the buffer is malformed, truncated, or in an
     *         unrecognized format (wraps the underlying {@link java.io.IOException})
     */
    public static LongBitmap deserialize(ByteBuf buf) {
        return ConcurrentRoaringBitmap.deserialize(buf);
    }

    /**
     * Deserializes a LongBitmap from long[] format compatible with {@link java.util.BitSet}.
     *
     * <p>Creates a new LongBitmap populated with values from the long array format
     * produced by {@link LongBitmap#serializeToLongArray()}. This format is compatible
     * with {@link java.util.BitSet#valueOf(long[])}.
     *
     * @param data long array in BitSet format
     * @return a new LongBitmap containing the deserialized values
     */
    public static LongBitmap deserializeFromLongArray(long[] data) {
        LongBitmap bitmap = create();
        bitmap.deserializeFromLongArray(data);
        return bitmap;
    }
}
