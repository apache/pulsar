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
package org.apache.pulsar;

import static org.testng.Assert.assertTrue;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

public class RoaringbitmapTest {

    @Test
    public void testRoaringBitmapContains() {
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        for (long i = 1; i <= 100_000; i++) {
            roaringBitmap.add(i, i + 1);
        }

        for (long i = 1; i <= 100_000; i++) {
            assertTrue(roaringBitmap.contains(i, i + 1));
        }

        RoaringBitmap roaringBitmap2 = new RoaringBitmap();
        for (long i = 1; i <= 1000_000; i++) {
            roaringBitmap2.add(i, i + 1);
        }

        for (long i = 1; i <= 1000_000; i++) {
            assertTrue(roaringBitmap2.contains(i, i + 1));
        }

        RoaringBitmap roaringBitmap3 = new RoaringBitmap();
        for (long i = 1; i <= 10_000_000; i++) {
            roaringBitmap3.add(i, i + 1);
        }

        for (long i = 1; i <= 10_000_000; i++) {
            assertTrue(roaringBitmap3.contains(i, i + 1));
        }
    }
}
