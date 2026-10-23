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
package org.apache.pulsar.client.api;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RangeTest {

    @Test
    public void testOf() {
        Range range = Range.of(0, 3);
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(3, range.getEnd());
    }

    @Test
    public void testIntersect() {
        Range range1 = Range.of(0, 9);
        Range range2 = Range.of(0, 2);
        Range intersectRange = range1.intersect(range2);
        Assert.assertEquals(0, intersectRange.getStart());
        Assert.assertEquals(2, intersectRange.getEnd());

        range2 = Range.of(10, 20);
        intersectRange = range1.intersect(range2);
        Assert.assertNull(intersectRange);

        range2 = Range.of(-10, -1);
        intersectRange = range1.intersect(range2);
        Assert.assertNull(intersectRange);

        range2 = Range.of(-5, 5);
        intersectRange = range1.intersect(range2);
        Assert.assertEquals(0, intersectRange.getStart());
        Assert.assertEquals(5, intersectRange.getEnd());

        range2 = Range.of(5, 15);
        intersectRange = range1.intersect(range2);
        Assert.assertEquals(5, intersectRange.getStart());
        Assert.assertEquals(9, intersectRange.getEnd());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalid() {
        Range.of(0, -5);
    }

    @Test
    public void testCompareTo() {
        Range range1 = Range.of(0, 5);
        Range range2 = Range.of(0, 5);
        Range range3 = Range.of(0, 10);
        Range range4 = Range.of(5, 10);

        Assert.assertEquals(0, range1.compareTo(range2));
        Assert.assertTrue(range1.compareTo(range3) < 0);
        Assert.assertTrue(range3.compareTo(range1) > 0);
        Assert.assertTrue(range1.compareTo(range4) < 0);
        Assert.assertTrue(range4.compareTo(range1) > 0);
    }

    @Test
    public void testContains() {
        Range range = Range.of(0, 5);

        Assert.assertTrue(range.contains(0));
        Assert.assertTrue(range.contains(3));
        Assert.assertTrue(range.contains(5));
        Assert.assertFalse(range.contains(-1));
        Assert.assertFalse(range.contains(6));
    }

    @Test
    public void testContainsRange() {
        Range range = Range.of(5, 10);

        // Test ranges that are fully contained
        Assert.assertTrue(range.contains(Range.of(6, 8)));

        Assert.assertTrue(range.contains(Range.of(5, 10)));

        Assert.assertTrue(range.contains(Range.of(5, 5)));

        Assert.assertTrue(range.contains(Range.of(5, 8)));

        Assert.assertTrue(range.contains(Range.of(10, 10)));

        Assert.assertTrue(range.contains(Range.of(8, 10)));

        // Test ranges that are not fully contained
        Assert.assertFalse(range.contains(Range.of(1, 5)));

        Assert.assertFalse(range.contains(Range.of(1, 4)));

        Assert.assertFalse(range.contains(Range.of(1, 10)));

        Assert.assertFalse(range.contains(Range.of(1, 11)));

        Assert.assertFalse(range.contains(Range.of(10, 12)));

        Assert.assertFalse(range.contains(Range.of(11, 20)));
    }


    @Test
    public void testSize() {
        Range range = Range.of(0, 0);
        Assert.assertEquals(1, range.size());

        range = Range.of(0, 1);
        Assert.assertEquals(2, range.size());

        range = Range.of(0, 5);
        Assert.assertEquals(6, range.size());

        range = Range.of(3, 3);
        Assert.assertEquals(1, range.size());
    }
}
