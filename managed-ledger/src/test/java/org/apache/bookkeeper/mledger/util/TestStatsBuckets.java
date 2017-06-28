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
package org.apache.bookkeeper.mledger.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

public class TestStatsBuckets {

    @Test
    public void testInvalidConstructor() {
        try {
            new StatsBuckets();
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void testUnorderedBoundaries() {
        try {
            new StatsBuckets(2, 1);
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void test() {
        StatsBuckets stats = new StatsBuckets(10, 20, 30);

        assertEquals(stats.getAvg(), Double.NaN);
        assertEquals(stats.getSum(), 0);
        assertEquals(stats.getCount(), 0);
        assertEquals(stats.getBuckets(), new long[] { 0, 0, 0, 0 });

        stats.addValue(5);

        // Before refresh stats should not be updated
        assertEquals(stats.getAvg(), Double.NaN);
        assertEquals(stats.getSum(), 0);
        assertEquals(stats.getCount(), 0);
        assertEquals(stats.getBuckets(), new long[] { 0, 0, 0, 0 });

        stats.refresh();

        assertEquals(stats.getAvg(), 5.0);
        assertEquals(stats.getSum(), 5);
        assertEquals(stats.getCount(), 1);
        assertEquals(stats.getBuckets(), new long[] { 1, 0, 0, 0 });

        stats.addValue(15);

        // Before refresh stats should not be updated
        assertEquals(stats.getAvg(), 5.0);
        assertEquals(stats.getSum(), 5);
        assertEquals(stats.getCount(), 1);
        assertEquals(stats.getBuckets(), new long[] { 1, 0, 0, 0 });

        stats.refresh();

        assertEquals(stats.getAvg(), 15.0);
        assertEquals(stats.getSum(), 15);
        assertEquals(stats.getCount(), 1);
        assertEquals(stats.getBuckets(), new long[] { 0, 1, 0, 0 });

        stats.addValue(50);

        assertEquals(stats.getSum(), 15);
        assertEquals(stats.getCount(), 1);

        stats.addValue(10);
        stats.addValue(30);

        stats.refresh();

        assertEquals(stats.getAvg(), 30.0);
        assertEquals(stats.getSum(), 90);
        assertEquals(stats.getCount(), 3);
        assertEquals(stats.getBuckets(), new long[] { 1, 0, 1, 1 });
    }
}
