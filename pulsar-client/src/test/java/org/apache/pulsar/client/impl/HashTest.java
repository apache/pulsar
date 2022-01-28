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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;

public class HashTest {
    @Test
    public void javaStringHashTest() {
        Hash h = JavaStringHash.getInstance();

        // Calculating `hashCode()` makes overflow as unsigned int32.
        String key1 = "keykeykeykeykey1";

        // `hashCode()` is negative as signed int32.
        String key2 = "keykeykey2";

        // Same value as C++ client
        assertEquals(434058482, h.makeHash(key1));
        assertEquals(42978643, h.makeHash(key2));
    }

    @Test
    public void murmur3_32HashTest() {
        Hash h = Murmur3Hash32.getInstance();

        // Same value as C++ client
        assertEquals(2110152746, h.makeHash("k1"));
        assertEquals(1479966664, h.makeHash("k2"));
        assertEquals(462881061, h.makeHash("key1"));
        assertEquals(1936800180, h.makeHash("key2"));
        assertEquals(39696932, h.makeHash("key01"));
        assertEquals(751761803, h.makeHash("key02"));
    }
}
