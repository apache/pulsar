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

public class ConcurrentBitSetRecyclableTest {

    @Test
    public void testRecycle() {
        ConcurrentBitSetRecyclable bitset1 = ConcurrentBitSetRecyclable.create();
        bitset1.set(3);
        bitset1.recycle();
        ConcurrentBitSetRecyclable bitset2 = ConcurrentBitSetRecyclable.create();
        ConcurrentBitSetRecyclable bitset3 = ConcurrentBitSetRecyclable.create();
        Assert.assertSame(bitset2, bitset1);
        Assert.assertFalse(bitset2.get(3));
        Assert.assertNotSame(bitset3, bitset1);
    }
}
