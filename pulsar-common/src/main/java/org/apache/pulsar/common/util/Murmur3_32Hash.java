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
import io.netty.buffer.ByteBuf;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

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
    // String.value(), a String's internal bytes, and String.isLatin1(), whether they are LATIN1, one byte for each
    // char, rather than UTF16, two bytes for each char in the platform's byte order. They need java.lang to be open to
    // this class's module, as bin/pulsar opens it with --add-opens java.base/java.lang=ALL-UNNAMED, and are null
    // elsewhere, such as in most clients. HotSpot inlines invokeExact on a static final MethodHandle.
    private static final MethodHandle STRING_VALUE;
    private static final MethodHandle STRING_IS_LATIN1;
    private static final VarHandle CHAR_NATIVE = MethodHandles.byteArrayViewVarHandle(char[].class,
            ByteOrder.nativeOrder());
    // The value of each char of the basic Base64 alphabet, -1 for the others
    private static final byte[] BASE64_VALUES = base64Values();
    // makeHashBase64's result for input that the fast path doesn't decode, which the JDK's decoder then decodes or
    // rejects; a masked hash is never negative
    private static final int NOT_DECODED = -1;

    static {
        MethodHandle value = null;
        MethodHandle isLatin1 = null;
        try {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(String.class, MethodHandles.lookup());
            value = lookup.findVirtual(String.class, "value", MethodType.methodType(byte[].class));
            isLatin1 = lookup.findVirtual(String.class, "isLatin1", MethodType.methodType(boolean.class));
            if (!readsChars(value, isLatin1, "a") || !readsChars(value, isLatin1, "\u20ac")) {
                value = null;
                isLatin1 = null;
            }
        } catch (Throwable ignore) {
            // java.lang isn't open to this module, or String has no such methods
            value = null;
            isLatin1 = null;
        }
        STRING_VALUE = value;
        STRING_IS_LATIN1 = isLatin1;
    }

    private final int seed;

    private Murmur3_32Hash() {
        seed = 0;
    }

    private static byte[] base64Values() {
        byte[] values = new byte[256];
        Arrays.fill(values, (byte) -1);
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        for (int i = 0; i < alphabet.length(); i++) {
            values[alphabet.charAt(i)] = (byte) i;
        }
        return values;
    }

    // Whether the handles read a single-char string's char back
    private static boolean readsChars(MethodHandle value, MethodHandle isLatin1, String string) throws Throwable {
        byte[] bytes = (byte[]) value.invoke(string);
        char c = string.charAt(0);
        return (boolean) isLatin1.invoke(string)
                ? bytes.length == 1 && (bytes[0] & 0xff) == c
                : bytes.length == 2 && (char) CHAR_NATIVE.get(bytes, 0) == c;
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
     * The same as {@code makeHash(value.getBytes(StandardCharsets.UTF_8))}, without encoding the string into a new
     * array: the hash reads the string's internal bytes, and encodes its non-ASCII chars to UTF-8 as it hashes them.
     */
    public static int makeHashUtf8(String value) {
        if (STRING_VALUE == null) {
            return instance.makeHash(value.getBytes(StandardCharsets.UTF_8));
        }
        byte[] bytes;
        boolean latin1;
        try {
            bytes = (byte[]) STRING_VALUE.invokeExact(value);
            latin1 = (boolean) STRING_IS_LATIN1.invokeExact(value);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalStateException(e);
        }
        return (latin1 ? hashLatin1(bytes) : hashUtf16(bytes)) & Integer.MAX_VALUE;
    }

    /**
     * The same as {@code makeHash(Base64.getDecoder().decode(value))}, without decoding the string into a new array:
     * the hash decodes the string's internal bytes as it hashes them. Input that isn't the plain Base64 of whole
     * bytes, with or without padding, is decoded by the JDK's decoder, which rejects it as before.
     */
    public static int makeHashBase64(String value) {
        if (STRING_VALUE != null) {
            byte[] bytes;
            boolean latin1;
            try {
                bytes = (byte[]) STRING_VALUE.invokeExact(value);
                latin1 = (boolean) STRING_IS_LATIN1.invokeExact(value);
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Throwable e) {
                throw new IllegalStateException(e);
            }
            if (latin1) {
                int hash = hashBase64(bytes);
                if (hash != NOT_DECODED) {
                    return hash;
                }
            }
        }
        return instance.makeHash(Base64.getDecoder().decode(value));
    }

    /**
     * {@link #makeHash(byte[])} of the bytes from the buffer's reader index to its writer index, which it doesn't
     * change.
     */
    public static int makeHash(ByteBuf buffer) {
        int start = buffer.readerIndex();
        int len = buffer.readableBytes();
        int chunksEnd = start + chunksLength(len);
        int h1 = instance.seed;
        for (int i = start; i < chunksEnd; i += CHUNK_SIZE) {
            h1 = instance.mixH1(h1, instance.mixK1(buffer.getIntLE(i)));
        }
        int k1 = 0;
        int remaining = len - (chunksEnd - start);
        if (remaining > 0) {
            k1 = buffer.getUnsignedByte(chunksEnd);
            if (remaining > 1) {
                k1 |= buffer.getUnsignedByte(chunksEnd + 1) << 8;
                if (remaining > 2) {
                    k1 |= buffer.getUnsignedByte(chunksEnd + 2) << 16;
                }
            }
        }
        return instance.finish(h1, k1, len) & Integer.MAX_VALUE;
    }

    /**
     * The masked hash of the bytes that the LATIN1 Base64 chars decode to, or {@link #NOT_DECODED} for chars outside
     * the basic alphabet, padding other than one or two {@code '='} that complete the last four chars, or a single
     * char left over, which the JDK's decoder rejects. Like it, the decoding ignores the unused bits of the last char.
     */
    @VisibleForTesting
    static int hashBase64(byte[] chars) {
        int n = chars.length;
        int padding = 0;
        if (n > 0 && chars[n - 1] == '=') {
            padding = n > 1 && chars[n - 2] == '=' ? 2 : 1;
            if (n % 4 != 0) {
                return NOT_DECODED;
            }
        }
        int dataChars = n - padding;
        int rest = dataChars % 4;
        if (rest == 1) {
            return NOT_DECODED;
        }
        int wholeChars = dataChars - rest;
        int h1 = instance.seed;
        // the decoded bytes that don't fill a chunk yet, the first in the lowest byte
        long pending = 0;
        int pendingBytes = 0;
        for (int i = 0; i < wholeChars; i += 4) {
            int a = BASE64_VALUES[chars[i] & 0xff];
            int b = BASE64_VALUES[chars[i + 1] & 0xff];
            int c = BASE64_VALUES[chars[i + 2] & 0xff];
            int d = BASE64_VALUES[chars[i + 3] & 0xff];
            if ((a | b | c | d) < 0) {
                return NOT_DECODED;
            }
            int bits = a << 18 | b << 12 | c << 6 | d;
            // the three bytes, the first in the lowest byte
            int decoded = bits >>> 16 | (bits >>> 8 & 0xff) << 8 | (bits & 0xff) << 16;
            pending |= (long) decoded << (pendingBytes << 3);
            pendingBytes += 3;
            if (pendingBytes >= CHUNK_SIZE) {
                h1 = instance.mixH1(h1, instance.mixK1((int) pending));
                pending >>>= 32;
                pendingBytes -= CHUNK_SIZE;
            }
        }
        int len = wholeChars / 4 * 3;
        if (rest > 0) {
            int a = BASE64_VALUES[chars[wholeChars] & 0xff];
            int b = BASE64_VALUES[chars[wholeChars + 1] & 0xff];
            int c = rest == 3 ? BASE64_VALUES[chars[wholeChars + 2] & 0xff] : 0;
            if ((a | b | c) < 0) {
                return NOT_DECODED;
            }
            int decoded = (a << 2 | b >>> 4) & 0xff;
            int decodedBytes = 1;
            if (rest == 3) {
                decoded |= ((b << 4 | c >>> 2) & 0xff) << 8;
                decodedBytes = 2;
            }
            pending |= (long) decoded << (pendingBytes << 3);
            pendingBytes += decodedBytes;
            len += decodedBytes;
            if (pendingBytes >= CHUNK_SIZE) {
                h1 = instance.mixH1(h1, instance.mixK1((int) pending));
                pending >>>= 32;
            }
        }
        return instance.finish(h1, (int) pending, len) & Integer.MAX_VALUE;
    }

    /**
     * Whether {@link #makeHashUtf8(String)} reads the strings' internal bytes.
     */
    @VisibleForTesting
    static boolean readsStringInternals() {
        return STRING_VALUE != null;
    }

    /**
     * The raw hash of a LATIN1 string's UTF-8 bytes. Its ASCII chars are its UTF-8 bytes, hashed four at a time
     * from its array until a chunk has a char from 0x80 to 0xff, which takes two UTF-8 bytes.
     */
    @VisibleForTesting
    static int hashLatin1(byte[] latin1) {
        int len = latin1.length;
        int chunksEnd = chunksLength(len);
        int h1 = instance.seed;
        for (int i = 0; i < chunksEnd; i += CHUNK_SIZE) {
            int k1 = (int) INT_LE.get(latin1, i);
            if ((k1 & 0x80808080) != 0) {
                return hashLatin1Encoding(latin1, i, h1);
            }
            h1 = instance.mixH1(h1, instance.mixK1(k1));
        }
        int k1 = remainingBytes(latin1, chunksEnd);
        if ((k1 & 0x80808080) != 0) {
            return hashLatin1Encoding(latin1, chunksEnd, h1);
        }
        return instance.finish(h1, k1, len);
    }

    // Hashes the chars from start on, encoding each to UTF-8, after the whole chunks before it hashed into h1
    private static int hashLatin1Encoding(byte[] latin1, int start, int h1) {
        // the encoded bytes that don't fill a chunk yet, the first in the lowest byte
        long pending = 0;
        int pendingBytes = 0;
        int len = start;
        for (int i = start; i < latin1.length; i++) {
            int c = latin1[i] & 0xff;
            int encoded;
            int encodedBytes;
            if (c < 0x80) {
                encoded = c;
                encodedBytes = 1;
            } else {
                encoded = (0xc0 | c >>> 6) | (0x80 | c & 0x3f) << 8;
                encodedBytes = 2;
            }
            pending |= (encoded & 0xffffffffL) << (pendingBytes << 3);
            pendingBytes += encodedBytes;
            len += encodedBytes;
            if (pendingBytes >= CHUNK_SIZE) {
                h1 = instance.mixH1(h1, instance.mixK1((int) pending));
                pending >>>= 32;
                pendingBytes -= CHUNK_SIZE;
            }
        }
        return instance.finish(h1, (int) pending, len);
    }

    /**
     * The raw hash of a UTF16 string's UTF-8 bytes, encoding each char as {@link String#getBytes} does: a surrogate
     * pair takes four bytes, and an unpaired surrogate is encoded as {@code '?'}.
     */
    @VisibleForTesting
    static int hashUtf16(byte[] utf16) {
        int chars = utf16.length >> 1;
        int h1 = instance.seed;
        long pending = 0;
        int pendingBytes = 0;
        int len = 0;
        for (int i = 0; i < chars; i++) {
            char c = (char) CHAR_NATIVE.get(utf16, i << 1);
            int encoded;
            int encodedBytes;
            if (c < 0x80) {
                encoded = c;
                encodedBytes = 1;
            } else if (c < 0x800) {
                encoded = (0xc0 | c >>> 6) | (0x80 | c & 0x3f) << 8;
                encodedBytes = 2;
            } else if (!Character.isSurrogate(c)) {
                encoded = (0xe0 | c >>> 12) | (0x80 | c >>> 6 & 0x3f) << 8 | (0x80 | c & 0x3f) << 16;
                encodedBytes = 3;
            } else {
                char low;
                if (Character.isHighSurrogate(c) && i + 1 < chars
                        && Character.isLowSurrogate(low = (char) CHAR_NATIVE.get(utf16, (i + 1) << 1))) {
                    int codePoint = Character.toCodePoint(c, low);
                    encoded = (0xf0 | codePoint >>> 18) | (0x80 | codePoint >>> 12 & 0x3f) << 8
                            | (0x80 | codePoint >>> 6 & 0x3f) << 16 | (0x80 | codePoint & 0x3f) << 24;
                    encodedBytes = 4;
                    i++;
                } else {
                    encoded = '?';
                    encodedBytes = 1;
                }
            }
            pending |= (encoded & 0xffffffffL) << (pendingBytes << 3);
            pendingBytes += encodedBytes;
            len += encodedBytes;
            if (pendingBytes >= CHUNK_SIZE) {
                h1 = instance.mixH1(h1, instance.mixK1((int) pending));
                pending >>>= 32;
                pendingBytes -= CHUNK_SIZE;
            }
        }
        return instance.finish(h1, (int) pending, len);
    }

    // The hash's last steps: the remaining bytes, from 0 to 3, the length and the finalization
    private int finish(int h1, int k1, int len) {
        h1 ^= mixK1(k1);
        h1 ^= len;
        return fmix(h1);
    }

    private int makeHash0(byte[] bytes) {
        int len = bytes.length;
        int chunksEnd = chunksLength(len);
        int h1 = seed;
        for (int i = 0; i < chunksEnd; i += CHUNK_SIZE) {
            h1 = mixH1(h1, mixK1((int) INT_LE.get(bytes, i)));
        }
        return finish(h1, remainingBytes(bytes, chunksEnd), len);
    }

    // The length of the whole chunks of len bytes. A loop up to it is a counted loop for the JIT, without a safepoint
    // poll or a bounds check in each iteration, and unrolled; a loop while i + CHUNK_SIZE <= len isn't, since the JIT
    // can't prove that the sum doesn't overflow
    private static int chunksLength(int len) {
        return len & -CHUNK_SIZE;
    }

    // The 0 to 3 bytes from chunksEnd to the array's end, the first in the lowest byte
    private static int remainingBytes(byte[] bytes, int chunksEnd) {
        int remaining = bytes.length - chunksEnd;
        int k1 = 0;
        if (remaining > 0) {
            k1 = bytes[chunksEnd] & 0xff;
            if (remaining > 1) {
                k1 |= (bytes[chunksEnd + 1] & 0xff) << 8;
                if (remaining > 2) {
                    k1 |= (bytes[chunksEnd + 2] & 0xff) << 16;
                }
            }
        }
        return k1;
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
