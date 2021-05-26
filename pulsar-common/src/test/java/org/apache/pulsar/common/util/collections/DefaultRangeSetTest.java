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

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

public class DefaultRangeSetTest {
    static final LongPairRangeSet.LongPairConsumer<LongPairRangeSet.LongPair> consumer =
            LongPairRangeSet.LongPair::new;

    @Test
    public void testBehavior() {
        LongPairRangeSet.DefaultRangeSet<LongPairRangeSet.LongPair> set =
                new LongPairRangeSet.DefaultRangeSet<>(consumer);
        ConcurrentOpenLongPairRangeSet<LongPairRangeSet.LongPair> rangeSet =
                new ConcurrentOpenLongPairRangeSet<>(consumer);

        assertNull(set.firstRange());
        assertNull(set.lastRange());
        assertNull(set.span());

        assertNull(rangeSet.firstRange());
        assertNull(rangeSet.lastRange());
        assertNull(rangeSet.span());

        try {
            set.contains(null);
            fail("should fail");
        } catch (NullPointerException ignore) {
        }
        try {
            rangeSet.contains(null);
            fail("should fail");
        } catch (NullPointerException ignore) {
        }

        rangeSet.addOpenClosed(9, 0, 10, 10);
        set.addOpenClosed(9, 0, 10, 10);
        assertTrue(rangeSet.contains(10,1));
        assertTrue(set.contains(10,1));

    }
}
