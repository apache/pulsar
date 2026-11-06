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
package org.apache.pulsar.common.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;
import org.testng.annotations.Test;

@SuppressWarnings("checkstyle:TypeName")
public class Murmur3_32HashTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void testMakeRawHashIsDeterministic() {
        assertEquals(Murmur3_32Hash.makeRawHash(b("deterministic-key")),
                Murmur3_32Hash.makeRawHash(b("deterministic-key")));
        assertEquals(Murmur3_32Hash.makeRawHash(b("")), Murmur3_32Hash.makeRawHash(b("")));
    }

    @Test
    public void testMakeRawHashIsUnmaskedMakeHash() {
        // makeHash() is makeRawHash() with the sign bit cleared. Verify that relationship holds, and
        // that the low 16 bits (used for PIP-486 entry-bucketing) are identical for both.
        for (int i = 0; i < 1000; i++) {
            byte[] key = b("key-" + i);
            int raw = Murmur3_32Hash.makeRawHash(key);
            int masked = Murmur3_32Hash.getInstance().makeHash(key);
            assertEquals(raw & Integer.MAX_VALUE, masked, "key-" + i);
            assertEquals(raw & 0xFFFF, masked & 0xFFFF, "low 16 bits differ for key-" + i);
        }
    }

    @Test
    public void testMakeRawHashHighHalfIsFullRange() {
        // The reason makeRawHash exists: the high 16 bits must be full-range. makeHash clears bit 31,
        // so its high half never reaches 0x8000; the raw hash's high half does.
        boolean rawReachesUpperHalf = false;
        boolean maskedReachesUpperHalf = false;
        for (int i = 0; i < 5000; i++) {
            byte[] key = b("key-" + i);
            if (((Murmur3_32Hash.makeRawHash(key) >>> 16) & 0xFFFF) >= 0x8000) {
                rawReachesUpperHalf = true;
            }
            if (((Murmur3_32Hash.getInstance().makeHash(key) >>> 16) & 0xFFFF) >= 0x8000) {
                maskedReachesUpperHalf = true;
            }
        }
        assertTrue(rawReachesUpperHalf, "raw hash high 16 bits should span the full range");
        assertFalse(maskedReachesUpperHalf,
                "makeHash high 16 bits should never reach 0x8000 (bit 31 is cleared)");
    }

    @Test
    public void testReadsTheStringInternals() {
        // the tests' JVMs open java.lang, as bin/pulsar does
        assertTrue(Murmur3_32Hash.readsStringInternals());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testUtf8StringHashOfNullThrows() {
        Murmur3_32Hash.makeHashUtf8(null);
    }

    @Test
    public void testLatin1HashMatchesByteArrayHash() {
        Hash hash = Murmur3_32Hash.getInstance();
        Random random = new Random(1);
        for (int length = 0; length < 70; length++) {
            char[] ascii = new char[length];
            for (int i = 0; i < length; i++) {
                ascii[i] = (char) random.nextInt(0x80);
            }
            String asciiString = new String(ascii);
            assertEquals(Murmur3_32Hash.makeHashUtf8(asciiString), hash.makeHash(b(asciiString)), "length " + length);
            for (int i = 0; i < length; i++) {
                // a char from 0x80 to 0xff, of two UTF-8 bytes, in a whole chunk or in the remainder
                char[] latin1 = ascii.clone();
                latin1[i] = (char) (0x80 | random.nextInt(0x80));
                String latin1String = new String(latin1);
                assertEquals(Murmur3_32Hash.makeHashUtf8(latin1String), hash.makeHash(b(latin1String)),
                        "length " + length + ", position " + i);
            }
        }
    }

    @Test
    public void testByteArrayHashMatchesMurmur3() {
        Random random = new Random(1);
        for (int length = 0; length < 70; length++) {
            byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            assertEquals(Murmur3_32Hash.makeRawHash(bytes), referenceHash(bytes), "length " + length);
        }
    }

    @Test
    public void testBase64HashMatchesTheDecodedBytesHash() {
        Hash hash = Murmur3_32Hash.getInstance();
        Random random = new Random(1);
        for (int length = 0; length < 70; length++) {
            byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            String padded = Base64.getEncoder().encodeToString(bytes);
            String unpadded = Base64.getEncoder().withoutPadding().encodeToString(bytes);
            assertEquals(Murmur3_32Hash.makeHashBase64(padded), hash.makeHash(bytes), padded);
            assertEquals(Murmur3_32Hash.makeHashBase64(unpadded), hash.makeHash(bytes), unpadded);
        }
        // the unused bits of the last char, which the JDK's decoder ignores
        for (String value : new String[]{"AB==", "AB", "ABC=", "ABC", "AAAAAB==", "AAAAABC"}) {
            assertEquals(Murmur3_32Hash.makeHashBase64(value), hash.makeHash(Base64.getDecoder().decode(value)),
                    value);
        }
    }

    @Test
    public void testBase64HashRejectsWhatTheDecoderRejects() {
        Hash hash = Murmur3_32Hash.getInstance();
        String[] values = {"A", "AAAAA", "A===", "AA=", "AA=A", "AAA=A", "=AAA", "AA-_", "AA AA", "AAé=", "AA\n=",
                "AAAA=", "AAAA==", "漢字", "AAAAA===", "AA==AAAA", "AAAA=AA="};
        for (String value : values) {
            byte[] decoded;
            try {
                decoded = Base64.getDecoder().decode(value);
            } catch (IllegalArgumentException e) {
                assertThrows(IllegalArgumentException.class, () -> Murmur3_32Hash.makeHashBase64(value));
                continue;
            }
            assertEquals(Murmur3_32Hash.makeHashBase64(value), hash.makeHash(decoded), value);
        }
    }

    @Test
    public void testByteBufHashMatchesByteArrayHash() {
        Hash hash = Murmur3_32Hash.getInstance();
        Random random = new Random(1);
        for (int length = 0; length < 70; length++) {
            byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            // the bytes after others, which the hash skips
            ByteBuf buffer = Unpooled.buffer(length + 3).writeBytes(new byte[]{1, 2, 3}).writeBytes(bytes);
            buffer.readerIndex(3);
            assertEquals(Murmur3_32Hash.makeHash(buffer), hash.makeHash(bytes), "length " + length);
            assertEquals(buffer.readerIndex(), 3);
        }
    }

    // The hash as the byte array implementation computed it with a ByteBuffer
    private static int referenceHash(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int h1 = 0;
        while (buffer.remaining() >= 4) {
            int k1 = buffer.getInt() * 0xcc9e2d51;
            h1 ^= Integer.rotateLeft(k1, 15) * 0x1b873593;
            h1 = Integer.rotateLeft(h1, 13) * 5 + 0xe6546b64;
        }
        int k1 = 0;
        for (int i = 0; buffer.hasRemaining(); i++) {
            k1 ^= (buffer.get() & 0xff) << (i * 8);
        }
        k1 *= 0xcc9e2d51;
        h1 ^= Integer.rotateLeft(k1, 15) * 0x1b873593;
        h1 ^= bytes.length;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        return h1 ^ h1 >>> 16;
    }

    @Test
    public void testUtf8StringHashMatchesByteArrayHash() {
        Hash hash = Murmur3_32Hash.getInstance();
        String[] fixedValues = {
                "", "a", "ab", "abc", "abcd", "abcde", "plain-ascii", "device-123456", "España", "漢字", "emoji-🚀",
                "\ud800", "\udc00", "a\ud800b", "abcdé", "abcdefgh漢", "\u007f\u0080\u07ff\u0800\uffff",
                "a🚀", "ab🚀", "abc🚀", "abcde🚀x", "\ud800\ud800", "\udc00🚀", "🚀\ud800"
        };
        for (String value : fixedValues) {
            assertEquals(Murmur3_32Hash.makeHashUtf8(value), hash.makeHash(b(value)), value);
        }

        Random random = new Random(1);
        for (int sample = 0; sample < 10_000; sample++) {
            int length = random.nextInt(40);
            StringBuilder value = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                value.append((char) random.nextInt(Character.MAX_VALUE + 1));
            }
            String string = value.toString();
            assertEquals(Murmur3_32Hash.makeHashUtf8(string), hash.makeHash(b(string)),
                    "sample " + sample);
        }

        // mostly ASCII, so that the non-ASCII characters come after a run of whole ASCII chunks and within one
        for (int sample = 0; sample < 10_000; sample++) {
            int length = random.nextInt(40);
            StringBuilder value = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                value.append(random.nextInt(8) == 0 ? (char) random.nextInt(Character.MAX_VALUE + 1)
                        : (char) random.nextInt(0x80));
            }
            String string = value.toString();
            assertEquals(Murmur3_32Hash.makeHashUtf8(string), hash.makeHash(b(string)),
                    "mostly ASCII sample " + sample);
        }
    }
}
