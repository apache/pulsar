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
package org.apache.pulsar.broker.transaction.pendingack;

import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;

public class PendingAckInMemoryDeleteTest {

    @Test
    public void pendingAckInMemoryDeleteTest() throws Exception {
        PersistentSubscription persistentSubscription = mock(PersistentSubscription.class);
        PendingAckHandleImpl pendingAckHandle = new PendingAckHandleImpl(persistentSubscription);
        Map<PositionImpl, PositionImpl> individualAckPositions = new HashMap<>();
        Field f = pendingAckHandle.getClass().getDeclaredField("individualAckPositions");
        f.setAccessible(true);
        f.set(pendingAckHandle, individualAckPositions);
        PositionImpl position = new PositionImpl(1, 1);
        BitSet bitSet = new BitSet();
        bitSet.set(0, 128);
        for (int i = 0; i < 128; i++) {
            if (i % 2 == 0) {
                bitSet.clear(i);
            }
        }
        position.setAckSet(bitSet.toLongArray());
        individualAckPositions.put(position, position);
        Method method = pendingAckHandle.getClass().getDeclaredMethod("endIndividualAckTxnCommon", HashMap.class);

        HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn = new HashMap<>();
        pendingAckMessageForCurrentTxn.put(position, position);
        method.setAccessible(true);
        assertTrue(individualAckPositions.containsKey(position));
        method.invoke(pendingAckHandle, pendingAckMessageForCurrentTxn);
        assertTrue(individualAckPositions.isEmpty());

        position.setAckSet(bitSet.toLongArray());
        pendingAckMessageForCurrentTxn.remove(position);
        individualAckPositions.put(position, position);
        bitSet.set(0, 128);
        for (int i = 0; i < 64; i++) {
            if (i % 2 == 0) {
                bitSet.clear(i);
            }
        }
        PositionImpl position1 = new PositionImpl(1, 1);
        position1.setAckSet(bitSet.toLongArray());
        pendingAckMessageForCurrentTxn.put(position1, position1);
        assertTrue(individualAckPositions.containsKey(position));
        method.invoke(pendingAckHandle, pendingAckMessageForCurrentTxn);
        assertTrue(individualAckPositions.containsKey(position));

        bitSet.set(0, 128);
        for (int i = 64; i < 128; i++) {
            if (i % 2 == 0) {
                bitSet.clear(i);
            }
        }

        position1.setAckSet(bitSet.toLongArray());
        assertTrue(individualAckPositions.containsKey(position));
        method.invoke(pendingAckHandle, pendingAckMessageForCurrentTxn);
        assertTrue(individualAckPositions.isEmpty());
    }
}
