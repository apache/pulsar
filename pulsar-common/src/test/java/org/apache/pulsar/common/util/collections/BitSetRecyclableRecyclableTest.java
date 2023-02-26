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
package org.apache.pulsar.common.util.collections;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BitSetRecyclableRecyclableTest {

    @Test
    public void testRecycle() {
        BitSetRecyclable bitset1 = BitSetRecyclable.create();
        bitset1.set(3);
        bitset1.recycle();
        BitSetRecyclable bitset2 = BitSetRecyclable.create();
        BitSetRecyclable bitset3 = BitSetRecyclable.create();
        Assert.assertSame(bitset2, bitset1);
        Assert.assertFalse(bitset2.get(3));
        Assert.assertNotSame(bitset3, bitset1);
    }

    @Test
    public void testResetWords() {
        BitSetRecyclable bitset1 = BitSetRecyclable.create();
        BitSetRecyclable bitset2 = BitSetRecyclable.create();
        bitset1.set(256);
        bitset2.set(128);
        bitset1.resetWords(bitset2.toLongArray());
        Assert.assertTrue(bitset1.get(128));
        Assert.assertFalse(bitset1.get(256));
    }

    @Test
    public void testBitSetEmpty() {
        BitSetRecyclable bitSet = BitSetRecyclable.create();
        bitSet.set(0, 5);
        bitSet.clear(1);
        bitSet.clear(2);
        bitSet.clear(3);
        long[] array = bitSet.toLongArray();
        Assert.assertFalse(bitSet.isEmpty());
        Assert.assertFalse(BitSetRecyclable.create().resetWords(array).isEmpty());
        bitSet.clear(0);
        bitSet.clear(4);
        Assert.assertTrue(bitSet.isEmpty());
        long[] array1 = bitSet.toLongArray();
        Assert.assertTrue(BitSetRecyclable.create().resetWords(array1).isEmpty());
        bitSet.recycle();
    }
}
