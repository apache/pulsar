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
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
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
}
