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
package org.apache.bookkeeper.mledger.util;

import static org.apache.bookkeeper.mledger.util.PositionAckSetUtil.andAckSet;
import static org.apache.bookkeeper.mledger.util.PositionAckSetUtil.compareToWithAckSet;
import static org.apache.bookkeeper.mledger.util.PositionAckSetUtil.isAckSetOverlap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.BitSet;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.AckSetState;
import org.apache.bookkeeper.mledger.impl.AckSetStateUtil;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.testng.annotations.Test;

public class PositionAckSetUtilTest {

    @Test
    public void isAckSetRepeatedTest() {
        BitSet thisBitSet = new BitSet();
        BitSet otherBitSet = new BitSet();

        thisBitSet.set(0, 64);
        otherBitSet.set(0, 64);

        thisBitSet.clear(1, 5);
        otherBitSet.clear(1, 6);
        assertTrue(isAckSetOverlap(thisBitSet.toLongArray(), otherBitSet.toLongArray()));
        otherBitSet.set(1, 5);
        assertFalse(isAckSetOverlap(thisBitSet.toLongArray(), otherBitSet.toLongArray()));
    }

    @Test
    public void compareToWithAckSetForCumulativeAckTest() {
        Position positionOne = PositionFactory.create(1, 1);
        Position positionTwo = PositionFactory.create(1, 2);
        assertEquals(compareToWithAckSet(positionOne, positionTwo), -1);
        positionTwo = PositionFactory.create(2, 1);
        assertEquals(compareToWithAckSet(positionOne, positionTwo), -1);
        positionTwo = PositionFactory.create(0, 1);
        assertEquals(compareToWithAckSet(positionOne, positionTwo), 1);
        positionTwo = PositionFactory.create(1, 0);
        assertEquals(compareToWithAckSet(positionOne, positionTwo), 1);
        positionTwo = PositionFactory.create(1, 1);
        assertEquals(compareToWithAckSet(positionOne, positionTwo), 0);

        BitSet bitSetOne = new BitSet();
        BitSet bitSetTwo = new BitSet();
        bitSetOne.set(0, 63);
        bitSetTwo.set(0, 63);
        bitSetOne.clear(0, 10);
        bitSetTwo.clear(0, 10);
        positionOne = AckSetStateUtil.createPositionWithAckSet(1, 1, bitSetOne.toLongArray());
        positionTwo = AckSetStateUtil.createPositionWithAckSet(1, 1, bitSetTwo.toLongArray());
        assertEquals(compareToWithAckSet(positionOne, positionTwo), 0);

        bitSetOne.clear(10, 12);
        AckSetState positionOneAckSetState = AckSetStateUtil.getAckSetState(positionOne);
        positionOneAckSetState.setAckSet(bitSetOne.toLongArray());
        assertEquals(compareToWithAckSet(positionOne, positionTwo), 2);

        bitSetOne.set(8, 12);
        positionOneAckSetState.setAckSet(bitSetOne.toLongArray());
        assertEquals(compareToWithAckSet(positionOne, positionTwo), -2);
    }

    @Test
    public void andAckSetTest() {
        Position positionOne = AckSetStateUtil.createPositionWithAckSet(1, 1, new long[0]);
        Position positionTwo = AckSetStateUtil.createPositionWithAckSet(1, 2, new long[0]);
        BitSet bitSetOne = new BitSet();
        BitSet bitSetTwo = new BitSet();
        bitSetOne.set(0);
        bitSetOne.set(2);
        bitSetOne.set(4);
        bitSetOne.set(6);
        bitSetOne.set(8);
        AckSetState positionOneAckSetState = AckSetStateUtil.getAckSetState(positionOne);
        positionOneAckSetState.setAckSet(bitSetOne.toLongArray());
        AckSetState positionTwoAckSetState = AckSetStateUtil.getAckSetState(positionTwo);
        positionTwoAckSetState.setAckSet(bitSetTwo.toLongArray());
        andAckSet(positionOne, positionTwo);
        BitSetRecyclable bitSetRecyclable = BitSetRecyclable.valueOf(positionOneAckSetState.getAckSet());
        assertTrue(bitSetRecyclable.isEmpty());

        bitSetTwo.set(2);
        bitSetTwo.set(4);

        positionOneAckSetState.setAckSet(bitSetOne.toLongArray());
        positionTwoAckSetState.setAckSet(bitSetTwo.toLongArray());
        andAckSet(positionOne, positionTwo);

        bitSetRecyclable = BitSetRecyclable.valueOf(positionOneAckSetState.getAckSet());
        BitSetRecyclable bitSetRecyclableTwo = BitSetRecyclable.valueOf(bitSetTwo.toLongArray());
        assertEquals(bitSetRecyclable, bitSetRecyclableTwo);

    }
}
