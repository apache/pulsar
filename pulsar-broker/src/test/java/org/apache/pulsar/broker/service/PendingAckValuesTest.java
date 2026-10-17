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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

public class PendingAckValuesTest {
    @Test
    public void pack_RoundTripsRemainingUnackedAndStickyKeyHash() {
        int[][] values = new int[][] {
                {0, 0},
                {1, -1},
                {Integer.MAX_VALUE, Integer.MIN_VALUE},
                {42, Integer.MAX_VALUE},
                {1_000_000, -123456789}
        };

        for (int[] value : values) {
            long packedValue = PendingAckValues.pack(value[0], value[1]);

            assertFalse(PendingAckValues.isNotFound(packedValue));
            assertEquals(PendingAckValues.remainingUnacked(packedValue), value[0]);
            assertEquals(PendingAckValues.stickyKeyHash(packedValue), value[1]);
        }
    }

    @Test
    public void packedNotFound_UsesReservedNegativeRemainingUnackedValue() {
        assertTrue(PendingAckValues.isNotFound(PendingAckValues.PACKED_NOT_FOUND));
        assertEquals(PendingAckValues.remainingUnacked(PendingAckValues.PACKED_NOT_FOUND),
                PendingAckValues.NOT_FOUND);
        assertEquals(PendingAckValues.stickyKeyHash(PendingAckValues.PACKED_NOT_FOUND), 0);
        assertFalse(PendingAckValues.isNotFound(PendingAckValues.pack(0, 0)));
    }

    @Test
    public void pack_RejectsNegativeRemainingUnacked() {
        assertThrows(IllegalArgumentException.class, () -> PendingAckValues.pack(-1, 0));
    }
}
