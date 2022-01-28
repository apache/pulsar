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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.testng.annotations.Test;

public class EntryBatchIndexesAcksTest {

    @Test
    void shouldResetStateBeforeReusing() {
        // given
        // a bitset with 95 bits set
        BitSetRecyclable bitSet = BitSetRecyclable.create();
        bitSet.set(0, 95);
        long[] nintyFiveBitsSet = bitSet.toLongArray();
        // and a EntryBatchIndexesAcks for the size of 10
        EntryBatchIndexesAcks acks = EntryBatchIndexesAcks.get(10);

        // when setting 2 indexes with 95/100 bits set in each (5 "acked" in each)
        acks.setIndexesAcks(8, Pair.of(100, nintyFiveBitsSet));
        acks.setIndexesAcks(9, Pair.of(100, nintyFiveBitsSet));

        // then the totalAckedIndexCount should be 10
        assertEquals(acks.getTotalAckedIndexCount(), 10);

        // when recycled and used again
        acks.recycle();
        acks = EntryBatchIndexesAcks.get(2);

        // then there should be no previous state and totalAckedIndexCount should be 0
        assertEquals(acks.getTotalAckedIndexCount(), 0);
    }

}