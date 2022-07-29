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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import lombok.Cleanup;
import org.testng.annotations.Test;

public class SegmentedLongArrayTest {

    @Test
    public void testArray() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(4);
        assertEquals(a.getCapacity(), 4);
        assertEquals(a.bytesCapacity(), 4 * 8);
        assertEquals(a.getInitialCapacity(), 4);

        a.writeLong(0, 0);
        a.writeLong(1, 1);
        a.writeLong(2, 2);
        a.writeLong(3, Long.MAX_VALUE);

        try {
            a.writeLong(4, Long.MIN_VALUE);
            fail("should have failed");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        a.increaseCapacity();

        a.writeLong(4, Long.MIN_VALUE);

        assertEquals(a.getCapacity(), 8);
        assertEquals(a.bytesCapacity(), 8 * 8);
        assertEquals(a.getInitialCapacity(), 4);

        assertEquals(a.readLong(0), 0);
        assertEquals(a.readLong(1), 1);
        assertEquals(a.readLong(2), 2);
        assertEquals(a.readLong(3), Long.MAX_VALUE);
        assertEquals(a.readLong(4), Long.MIN_VALUE);

        a.shrink(5);
        assertEquals(a.getCapacity(), 5);
        assertEquals(a.bytesCapacity(), 5 * 8);
        assertEquals(a.getInitialCapacity(), 4);
    }

    @Test
    public void testLargeArray() {
        long initialCap = 3 * 1024 * 1024;

        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(initialCap);
        assertEquals(a.getCapacity(), initialCap);
        assertEquals(a.bytesCapacity(), initialCap * 8);
        assertEquals(a.getInitialCapacity(), initialCap);

        long baseOffset = initialCap - 100;

        a.writeLong(baseOffset, 0);
        a.writeLong(baseOffset + 1, 1);
        a.writeLong(baseOffset + 2, 2);
        a.writeLong(baseOffset + 3, Long.MAX_VALUE);
        a.writeLong(baseOffset + 4, Long.MIN_VALUE);

        a.increaseCapacity();

        assertEquals(a.getCapacity(), 5 * 1024 * 1024);
        assertEquals(a.bytesCapacity(), 5 * 1024 * 1024 * 8);
        assertEquals(a.getInitialCapacity(), initialCap);

        assertEquals(a.readLong(baseOffset), 0);
        assertEquals(a.readLong(baseOffset + 1), 1);
        assertEquals(a.readLong(baseOffset + 2), 2);
        assertEquals(a.readLong(baseOffset + 3), Long.MAX_VALUE);
        assertEquals(a.readLong(baseOffset + 4), Long.MIN_VALUE);

        a.shrink(initialCap);
        assertEquals(a.getCapacity(), initialCap);
        assertEquals(a.bytesCapacity(), initialCap * 8);
        assertEquals(a.getInitialCapacity(), initialCap);
    }
}
