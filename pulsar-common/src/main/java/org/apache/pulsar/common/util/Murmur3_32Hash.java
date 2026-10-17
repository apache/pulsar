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

/*
 * The original MurmurHash3 was written by Austin Appleby, and is placed in the
 * public domain. This source code, implemented by Licht Takeuchi, is based on
 * the orignal MurmurHash3 source code.
 */
package org.apache.pulsar.common.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.UnsignedBytes;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Implementation of the MurmurHash3 non-cryptographic hash function.
 */
@SuppressWarnings("checkstyle:TypeName")
public class Murmur3_32Hash implements Hash {
    private static final Murmur3_32Hash instance = new Murmur3_32Hash();

    private static final int CHUNK_SIZE = 4;
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;
    private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    @VisibleForTesting
    static final int NOT_ASCII = -1;
    // Reads a String's internal byte[] where java.lang is open to this class's module, as bin/pulsar opens it with
    // --add-opens java.base/java.lang=ALL-UNNAMED; null elsewhere, such as in most clients. HotSpot inlines invokeExact
    // on a static final MethodHandle into a plain field load.
    private static final MethodHandle STRING_VALUE_GETTER = findStringValueGetter();

    private final int seed;

    private Murmur3_32Hash() {
        seed = 0;
    }

    private static MethodHandle findStringValueGetter() {
        try {
            MethodHandle getter = MethodHandles.privateLookupIn(String.class, MethodHandles.lookup())
                    .findGetter(String.class, "value", byte[].class);
            // the JDK's compact strings keep an ASCII string's bytes as they are
            byte[] value = (byte[]) getter.invoke("a");
            return value.length == 1 && value[0] == 'a' ? getter : null;
        } catch (Throwable ignore) {
            // java.lang isn't open to this module, or String has no byte[] value
            return null;
        }
    }

    public static Hash getInstance() {
        return instance;
    }

    @Override
    public int makeHash(byte[] b) {
        return makeHash0(b) & Integer.MAX_VALUE;
    }

    /**
     * The raw, signed 32-bit MurmurHash3 of {@code b}, <i>without</i> the sign-bit masking that
     * {@link #makeHash(byte[])} applies. Useful when both 16-bit halves of the hash are needed as
     * independent, full-range fields — e.g. PIP-486 routes scalable-topic segments on the high half
     * and entry-buckets on the low half. ({@link #makeHash(byte[])} clears bit 31, which would make
     * the high half only span [0, 0x7FFF].)
     */
    public static int makeRawHash(byte[] b) {
        return instance.makeHash0(b);
    }

    /**
     * The same as {@code makeHash(value.getBytes(StandardCharsets.UTF_8))}. An ASCII string is hashed from its
     * internal bytes, which are its UTF-8 bytes, without encoding it into a new array.
     */
    public static int makeHashUtf8(String value) {
        byte[] latin1 = latin1Value(value);
        if (latin1 != null) {
            int hash = makeAsciiHash(latin1);
            if (hash != NOT_ASCII) {
                return hash;
            }
        }
        return instance.makeHash(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The internal byte[] of a compact string with the LATIN1 coder, or null.
     */
    @VisibleForTesting
    static byte[] latin1Value(String value) {
        if (STRING_VALUE_GETTER == null) {
            return null;
        }
        byte[] bytes;
        try {
            bytes = (byte[]) STRING_VALUE_GETTER.invokeExact(value);
        } catch (NullPointerException e) {
            throw e;
        } catch (Throwable ignore) {
            return null;
        }
        // a UTF16 string's array has two bytes for each char
        return bytes.length == value.length() ? bytes : null;
    }

    /**
     * {@link #makeHash(byte[])} of the bytes if they are all ASCII, else {@link #NOT_ASCII}.
     */
    @VisibleForTesting
    static int makeAsciiHash(byte[] bytes) {
        int len = bytes.length;
        int h1 = instance.seed;
        int i = 0;
        for (; i + CHUNK_SIZE <= len; i += CHUNK_SIZE) {
            int k1 = (int) INT_LE.get(bytes, i);
            if ((k1 & 0x80808080) != 0) {
                return NOT_ASCII;
            }
            h1 = instance.mixH1(h1, instance.mixK1(k1));
        }
        int k1 = 0;
        for (int shift = 0; i < len; i++, shift += 8) {
            k1 |= (bytes[i] & 0xff) << shift;
        }
        if ((k1 & 0x80808080) != 0) {
            return NOT_ASCII;
        }
        h1 ^= instance.mixK1(k1);
        h1 ^= len;
        return instance.fmix(h1) & Integer.MAX_VALUE;
    }

    private int makeHash0(byte[] bytes) {
        int len = bytes.length;
        int reminder = len % CHUNK_SIZE;
        int h1 = seed;

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        while (byteBuffer.remaining() >= CHUNK_SIZE) {
            int k1 = byteBuffer.getInt();

            k1 = mixK1(k1);
            h1 = mixH1(h1, k1);
        }

        int k1 = 0;
        for (int i = 0; i < reminder; i++) {
            k1 ^= UnsignedBytes.toInt(byteBuffer.get()) << (i * 8);
        }

        h1 ^= mixK1(k1);
        h1 ^= len;
        h1 = fmix(h1);

        return h1;
    }

    private int fmix(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;

        return h;
    }

    private int mixK1(int k1) {
        k1 *= C1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= C2;
        return k1;
    }

    private int mixH1(int h1, int k1) {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        return h1 * 5 + 0xe6546b64;
    }
}
