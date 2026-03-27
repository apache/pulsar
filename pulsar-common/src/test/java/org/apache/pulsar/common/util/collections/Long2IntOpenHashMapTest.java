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
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

public class Long2IntOpenHashMapTest {

    @Test
    public void testEmpty() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        assertTrue(map.isEmpty());
        assertEquals(map.get(0), 0);
        assertEquals(map.get(1), 0);
    }

    @Test
    public void testPutGet() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        assertEquals(map.put(1, 10), 0);
        assertEquals(map.put(2, 20), 0);
        assertFalse(map.isEmpty());
        assertEquals(map.get(1), 10);
        assertEquals(map.get(2), 20);
        assertEquals(map.get(3), 0); // default
    }

    @Test
    public void testPutReplace() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        map.put(1, 10);
        assertEquals(map.put(1, 100), 10);
        assertEquals(map.get(1), 100);
    }

    @Test
    public void testRemove() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        map.put(1, 10);
        map.put(2, 20);
        assertEquals(map.remove(1), 10);
        assertEquals(map.get(1), 0); // default after removal
        assertEquals(map.remove(99), 0); // not present
    }

    @Test
    public void testGetOrDefault() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        map.put(1, 10);
        assertEquals(map.getOrDefault(1, -1), 10);
        assertEquals(map.getOrDefault(2, -1), -1);
    }

    @Test
    public void testComputeIfAbsent() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        assertEquals(map.computeIfAbsent(1, k -> 10), 10);
        assertEquals(map.computeIfAbsent(1, k -> 99), 10);
    }

    @Test
    public void testClear() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        map.put(1, 10);
        map.put(2, 20);
        map.clear();
        assertTrue(map.isEmpty());
        assertEquals(map.get(1), 0);
    }

    @Test
    public void testRehash() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap(4);
        for (int i = 0; i < 100; i++) {
            map.put(i, i * 10);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals(map.get(i), i * 10);
        }
    }
}
