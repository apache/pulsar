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
import org.testng.annotations.Test;

public class CompareUtilTest {
    @Test
    public void testCompareDoubleWithResolution_bucketedComparison() {
        // Same truncated bucket when dividing by resolution
        assertEquals(CompareUtil.compareDoubleWithResolution(0.01, 0.49, 0.5), 0);
        assertEquals(CompareUtil.compareDoubleWithResolution(0.51, 0.99, 0.5), 0);

        // Different truncated buckets
        assertEquals(CompareUtil.compareDoubleWithResolution(0.51, 0.49, 0.5), 1);
        assertEquals(CompareUtil.compareDoubleWithResolution(0.49, 0.51, 0.5), -1);
        assertEquals(CompareUtil.compareDoubleWithResolution(1.01, 0.49, 0.5), 1);

        // Larger numbers
        assertEquals(CompareUtil.compareDoubleWithResolution(19.99, 10.01, 10.0), 0);
        assertEquals(CompareUtil.compareDoubleWithResolution(19.99, 20.01, 10.0), -1);
        assertEquals(CompareUtil.compareDoubleWithResolution(10.00, 9.99, 10.0), 1);
    }

    @Test
    public void testCompareLongWithResolution_exactComparison() {
        // resolution = 1 -> behave like Long.compare
        assertEquals(CompareUtil.compareLongWithResolution(1L, 2L, 1L), -1);
        assertEquals(CompareUtil.compareLongWithResolution(2L, 1L, 1L), 1);
        assertEquals(CompareUtil.compareLongWithResolution(2L, 2L, 1L), 0);
    }

    @Test
    public void testCompareLongWithResolution_bucketedComparison() {
        // Same bucket when divided by resolution
        assertEquals(CompareUtil.compareLongWithResolution(8L, 9L, 10L), 0);
        assertEquals(CompareUtil.compareLongWithResolution(10L, 19L, 10L), 0);

        // Different buckets
        assertEquals(CompareUtil.compareLongWithResolution(9L, 20L, 10L), -1);
        assertEquals(CompareUtil.compareLongWithResolution(21L, 10L, 10L), 1);

        // Larger resolution
        assertEquals(CompareUtil.compareLongWithResolution(100L, 175L, 100L), 0);
        assertEquals(CompareUtil.compareLongWithResolution(199L, 201L, 100L), -1);
    }

    @Test
    public void testCompareIntegerWithResolution_exactComparison() {
        // resolution = 1 -> behave like Integer.compare
        assertEquals(CompareUtil.compareIntegerWithResolution(1, 2, 1), -1);
        assertEquals(CompareUtil.compareIntegerWithResolution(2, 1, 1), 1);
        assertEquals(CompareUtil.compareIntegerWithResolution(2, 2, 1), 0);
    }

    @Test
    public void testCompareIntegerWithResolution_bucketedComparison() {
        // Same bucket
        assertEquals(CompareUtil.compareIntegerWithResolution(3, 4, 5), 0);
        assertEquals(CompareUtil.compareIntegerWithResolution(5, 9, 5), 0);

        // Different buckets
        assertEquals(CompareUtil.compareIntegerWithResolution(4, 10, 5), -1);
        assertEquals(CompareUtil.compareIntegerWithResolution(11, 5, 5), 1);

        // Larger resolution
        assertEquals(CompareUtil.compareIntegerWithResolution(51, 75, 50), 0);
        assertEquals(CompareUtil.compareIntegerWithResolution(49, 101, 50), -1);
    }
}