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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
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
    public void testReadsTheInternalBytesOfALatin1String() {
        // the tests' JVMs open java.lang, as bin/pulsar does
        assertEquals(Murmur3_32Hash.latin1Value("device-123456"), b("device-123456"));
        assertEquals(Murmur3_32Hash.latin1Value("käyttäjä"), "käyttäjä".getBytes(StandardCharsets.ISO_8859_1));
        assertNull(Murmur3_32Hash.latin1Value("漢字"));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testUtf8StringHashOfNullThrows() {
        Murmur3_32Hash.makeHashUtf8(null);
    }

    @Test
    public void testAsciiHashMatchesByteArrayHash() {
        Hash hash = Murmur3_32Hash.getInstance();
        Random random = new Random(1);
        for (int length = 0; length < 70; length++) {
            byte[] ascii = new byte[length];
            for (int i = 0; i < length; i++) {
                ascii[i] = (byte) random.nextInt(0x80);
            }
            assertEquals(Murmur3_32Hash.makeAsciiHash(ascii), hash.makeHash(ascii), "length " + length);
            for (int i = 0; i < length; i++) {
                // a non-ASCII byte in a whole chunk or in the remainder
                byte[] notAscii = ascii.clone();
                notAscii[i] = (byte) (0x80 | random.nextInt(0x80));
                assertEquals(Murmur3_32Hash.makeAsciiHash(notAscii), Murmur3_32Hash.NOT_ASCII, "length " + length);
            }
        }
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
