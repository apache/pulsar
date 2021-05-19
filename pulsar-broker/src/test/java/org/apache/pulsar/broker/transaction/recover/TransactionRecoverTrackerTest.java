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
package org.apache.pulsar.broker.transaction.recover;

import io.netty.util.HashedWheelTimer;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.broker.transaction.timeout.TransactionTimeoutTrackerFactoryImpl;
import org.apache.pulsar.broker.transaction.timeout.TransactionTimeoutTrackerImpl;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class TransactionRecoverTrackerTest {

    @Test
    public void openStatusRecoverTrackerTest() throws Exception {
        TransactionMetadataStoreService transactionMetadataStoreService = mock(TransactionMetadataStoreService.class);
        TransactionTimeoutTracker timeoutTracker = new TransactionTimeoutTrackerFactoryImpl(
                transactionMetadataStoreService, new HashedWheelTimer()).newTracker(TransactionCoordinatorID.get(1));
        TransactionRecoverTrackerImpl recoverTracker =
                new TransactionRecoverTrackerImpl(transactionMetadataStoreService, timeoutTracker, 1);

        recoverTracker.handleOpenStatusTransaction(1, 200);
        recoverTracker.handleOpenStatusTransaction(2, 300);

        Field field = TransactionRecoverTrackerImpl.class.getDeclaredField("openTransactions");
        field.setAccessible(true);
        Map<Long, Long> map = (Map<Long, Long>) field.get(recoverTracker);

        assertEquals(map.size(), 2);
        assertEquals(map.get(1L).longValue(), 200L);
        assertEquals(map.get(2L).longValue(), 300L);

        field = TransactionTimeoutTrackerImpl.class.getDeclaredField("priorityQueue");
        field.setAccessible(true);
        TripleLongPriorityQueue priorityQueue = (TripleLongPriorityQueue) field.get(timeoutTracker);
        assertEquals(priorityQueue.size(), 0);

        recoverTracker.appendOpenTransactionToTimeoutTracker();
        assertEquals(priorityQueue.size(), 2);
    }

    @Test
    public void updateStatusRecoverTest() throws Exception {
        TransactionRecoverTrackerImpl recoverTracker =
                new TransactionRecoverTrackerImpl(mock(TransactionMetadataStoreService.class),
                        mock(TransactionTimeoutTrackerImpl.class), 1);
        long committingSequenceId = 1L;
        long committedSequenceId = 2L;
        long abortingSequenceId = 3L;
        long abortedSequenceId = 4L;
        recoverTracker.handleOpenStatusTransaction(committingSequenceId, 100);
        recoverTracker.handleOpenStatusTransaction(committedSequenceId, 100);
        recoverTracker.handleOpenStatusTransaction(abortingSequenceId, 100);
        recoverTracker.handleOpenStatusTransaction(abortedSequenceId, 100);

        Field field = TransactionRecoverTrackerImpl.class.getDeclaredField("openTransactions");
        field.setAccessible(true);
        Map<Long, Long> openMap = (Map<Long, Long>) field.get(recoverTracker);
        assertEquals(4, openMap.size());

        recoverTracker.updateTransactionStatus(committingSequenceId, TxnStatus.COMMITTING);
        assertEquals(3, openMap.size());
        recoverTracker.updateTransactionStatus(committedSequenceId, TxnStatus.COMMITTING);
        assertEquals(2, openMap.size());
        recoverTracker.updateTransactionStatus(committedSequenceId, TxnStatus.COMMITTED);

        recoverTracker.updateTransactionStatus(abortingSequenceId, TxnStatus.ABORTING);
        assertEquals(1, openMap.size());
        recoverTracker.updateTransactionStatus(abortedSequenceId, TxnStatus.ABORTING);
        assertEquals(0, openMap.size());
        recoverTracker.updateTransactionStatus(abortedSequenceId, TxnStatus.ABORTED);

        field = TransactionRecoverTrackerImpl.class.getDeclaredField("committingTransactions");
        field.setAccessible(true);
        Set<Long> commitSet = (Set<Long>) field.get(recoverTracker);

        assertEquals(commitSet.size(), 1);
        assertTrue(commitSet.contains(committingSequenceId));
        assertFalse(commitSet.contains(committedSequenceId));

        field = TransactionRecoverTrackerImpl.class.getDeclaredField("abortingTransactions");
        field.setAccessible(true);
        Set<Long> abortSet = (Set<Long>) field.get(recoverTracker);

        assertEquals(1, abortSet.size());
        assertTrue(abortSet.contains(abortingSequenceId));
        assertFalse(abortSet.contains(abortedSequenceId));
    }
}
