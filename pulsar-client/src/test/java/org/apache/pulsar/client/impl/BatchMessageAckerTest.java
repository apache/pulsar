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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.BitSet;

public class BatchMessageAckerTest {

    private static final int BATCH_SIZE = 10;

    private BatchMessageAcker acker;

    @BeforeMethod
    public void setup() {
        acker = BatchMessageAcker.newAcker(10);
    }

    @Test
    public void testAckers() {
        assertEquals(BATCH_SIZE, acker.getOutstandingAcks());
        assertEquals(BATCH_SIZE, acker.getBatchSize());

        assertFalse(acker.ackIndividual(4));
        for (int i = 0; i < BATCH_SIZE; i++) {
            if (4 == i) {
                assertFalse(acker.getBitSet().get(i));
            } else {
                assertTrue(acker.getBitSet().get(i));
            }
        }

        assertFalse(acker.ackCumulative(6));
        for (int i = 0; i < BATCH_SIZE; i++) {
            if (i <= 6) {
                assertFalse(acker.getBitSet().get(i));
            } else {
                assertTrue(acker.getBitSet().get(i));
            }
        }

        for (int i = BATCH_SIZE - 1; i >= 8; i--) {
            assertFalse(acker.ackIndividual(i));
            assertFalse(acker.getBitSet().get(i));
        }

        assertTrue(acker.ackIndividual(7));
        assertEquals(0, acker.getOutstandingAcks());
    }

    @Test
    public void testBitSetAcker() {
        BitSet bitSet = BitSet.valueOf(acker.getBitSet().toLongArray());
        BatchMessageAcker bitSetAcker = BatchMessageAcker.newAcker(bitSet);

        Assert.assertEquals(acker.getBitSet(), bitSetAcker.getBitSet());
        Assert.assertEquals(acker.getOutstandingAcks(), bitSetAcker.getOutstandingAcks());
    }

}
