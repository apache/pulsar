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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

public class Int2ObjectOpenHashMapTest {

    @Test
    public void testEmpty() {
        Int2ObjectOpenHashMap<String> map = new Int2ObjectOpenHashMap<>();
        assertTrue(map.isEmpty());
        assertNull(map.get(1));
    }

    @Test
    public void testPutGet() {
        Int2ObjectOpenHashMap<String> map = new Int2ObjectOpenHashMap<>();
        assertNull(map.put(1, "one"));
        assertNull(map.put(2, "two"));
        assertEquals(map.get(1), "one");
        assertEquals(map.get(2), "two");
        assertNull(map.get(3));
        assertEquals(map.size(), 2);
    }

    @Test
    public void testRemove() {
        Int2ObjectOpenHashMap<String> map = new Int2ObjectOpenHashMap<>();
        map.put(1, "one");
        map.put(2, "two");
        assertEquals(map.remove(1), "one");
        assertNull(map.get(1));
        assertEquals(map.size(), 1);
    }

    @Test
    public void testRemoveConditional() {
        Int2ObjectOpenHashMap<String> map = new Int2ObjectOpenHashMap<>();
        String val = "one";
        map.put(1, val);
        assertFalse(map.remove(1, "other")); // different ref
        assertTrue(map.remove(1, val));
        assertTrue(map.isEmpty());
    }

    @Test
    public void testClear() {
        Int2ObjectOpenHashMap<String> map = new Int2ObjectOpenHashMap<>();
        map.put(1, "one");
        map.put(2, "two");
        map.clear();
        assertTrue(map.isEmpty());
        assertNull(map.get(1));
    }

    @Test
    public void testRehash() {
        Int2ObjectOpenHashMap<Integer> map = new Int2ObjectOpenHashMap<>(4);
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        assertEquals(map.size(), 100);
        for (int i = 0; i < 100; i++) {
            assertEquals(map.get(i), Integer.valueOf(i));
        }
    }
}
