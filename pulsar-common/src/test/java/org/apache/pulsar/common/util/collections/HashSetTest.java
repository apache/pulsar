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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class HashSetTest {

    @Test
    public void testLongOpenHashSetBasic() {
        LongOpenHashSet set = new LongOpenHashSet();
        assertTrue(set.isEmpty());
        assertTrue(set.add(1));
        assertTrue(set.add(2));
        assertFalse(set.add(1)); // duplicate
        assertEquals(set.size(), 2);
        assertTrue(set.contains(1));
        assertTrue(set.contains(2));
        assertFalse(set.contains(3));
    }

    @Test
    public void testLongOpenHashSetIterable() {
        LongOpenHashSet set = new LongOpenHashSet();
        set.add(3);
        set.add(1);
        set.add(2);
        List<Long> values = new ArrayList<>();
        for (long v : set) {
            values.add(v);
        }
        Collections.sort(values);
        assertEquals(values, List.of(1L, 2L, 3L));
    }

    @Test
    public void testLongOpenHashSetForEach() {
        LongOpenHashSet set = new LongOpenHashSet();
        set.add(10);
        set.add(20);
        List<Long> values = new ArrayList<>();
        set.forEach((long v) -> values.add(v));
        Collections.sort(values);
        assertEquals(values, List.of(10L, 20L));
    }

    @Test
    public void testLongOpenHashSetRehash() {
        LongOpenHashSet set = new LongOpenHashSet(4);
        for (int i = 0; i < 100; i++) {
            set.add(i);
        }
        assertEquals(set.size(), 100);
        for (int i = 0; i < 100; i++) {
            assertTrue(set.contains(i));
        }
    }

    @Test
    public void testIntOpenHashSetBasic() {
        IntOpenHashSet set = new IntOpenHashSet();
        assertTrue(set.isEmpty());
        assertTrue(set.add(1));
        assertTrue(set.add(2));
        assertFalse(set.add(1)); // duplicate
        assertEquals(set.size(), 2);
        assertTrue(set.contains(1));
        assertTrue(set.contains(2));
        assertFalse(set.contains(3));
    }

    @Test
    public void testIntOpenHashSetRehash() {
        IntOpenHashSet set = new IntOpenHashSet(4);
        for (int i = 0; i < 100; i++) {
            set.add(i);
        }
        assertEquals(set.size(), 100);
        for (int i = 0; i < 100; i++) {
            assertTrue(set.contains(i));
        }
    }
}
