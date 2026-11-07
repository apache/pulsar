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
package org.apache.pulsar.transaction.coordinator.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionLogReplayCallback;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.TransactionNotFoundException;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MLTransactionMetadataStoreTimeoutTest {
    private MLTransactionLogImpl transactionLog;
    private TransactionTimeoutTracker timeoutTracker;
    private MLTransactionMetadataStore store;

    @BeforeMethod
    public void setUp() throws Exception {
        transactionLog = mock(MLTransactionLogImpl.class);
        timeoutTracker = mock(TransactionTimeoutTracker.class);
        AtomicLong entryId = new AtomicLong();
        when(transactionLog.append(any())).thenAnswer(invocation ->
                CompletableFuture.completedFuture(PositionFactory.create(1, entryId.getAndIncrement())));
        when(transactionLog.deletePosition(anyList())).thenReturn(CompletableFuture.completedFuture(null));
        when(transactionLog.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        doAnswer(invocation -> {
            invocation.<TransactionLogReplayCallback>getArgument(0).replayComplete();
            return null;
        }).when(transactionLog).replayAsync(any());
        store = new MLTransactionMetadataStore(TransactionCoordinatorID.get(1), transactionLog, timeoutTracker,
                new MLTransactionSequenceIdGenerator(), 0);
        store.init(mock(TransactionRecoverTracker.class)).get(5, TimeUnit.SECONDS);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (store != null) {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @DataProvider(name = "terminalStatuses")
    public Object[][] terminalStatuses() {
        return new Object[][] {
                {TxnStatus.COMMITTING, TxnStatus.COMMITTED},
                {TxnStatus.ABORTING, TxnStatus.ABORTED}
        };
    }

    @Test(dataProvider = "terminalStatuses")
    public void testRemoveTimeoutAfterTerminalStatusPersisted(TxnStatus intermediateStatus, TxnStatus terminalStatus)
            throws Exception {
        long transactionTimeout = TimeUnit.HOURS.toMillis(1);
        TxnID txnID = store.newTransaction(transactionTimeout, null).get(5, TimeUnit.SECONDS);
        TxnMeta txnMeta = store.getTxnMeta(txnID).get(5, TimeUnit.SECONDS);
        verify(timeoutTracker).addTransaction(txnID.getLeastSigBits(), transactionTimeout);
        verify(timeoutTracker, never()).removeTransaction(anyLong());

        store.updateTxnStatus(txnID, intermediateStatus, TxnStatus.OPEN, false).get(5, TimeUnit.SECONDS);
        assertThat(txnMeta.status()).isEqualTo(intermediateStatus);
        verify(timeoutTracker, never()).removeTransaction(anyLong());

        CompletableFuture<Position> appendFuture = new CompletableFuture<>();
        CountDownLatch appendStarted = new CountDownLatch(1);
        when(transactionLog.append(any())).thenAnswer(invocation -> {
            appendStarted.countDown();
            return appendFuture;
        });
        doAnswer(invocation -> {
            assertThat(Thread.holdsLock(txnMeta)).as("timeout cancellation must not hold the transaction lock")
                    .isFalse();
            return null;
        }).when(timeoutTracker).removeTransaction(txnID.getLeastSigBits());

        CompletableFuture<Void> updateFuture =
                store.updateTxnStatus(txnID, terminalStatus, intermediateStatus, false);
        try {
            assertThat(appendStarted.await(5, TimeUnit.SECONDS)).as("terminal log append started").isTrue();
            assertThat(updateFuture).isNotDone();
            assertThat(txnMeta.status()).isEqualTo(intermediateStatus);
            verify(timeoutTracker, never()).removeTransaction(anyLong());

            appendFuture.complete(PositionFactory.create(1, 2));
            updateFuture.get(5, TimeUnit.SECONDS);
            verify(timeoutTracker).removeTransaction(txnID.getLeastSigBits());
            assertThatThrownBy(() -> store.getTxnMeta(txnID).get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TransactionNotFoundException.class);
            assertThat(store.getOnGoingTxnCount().longValue()).isZero();
        } finally {
            appendFuture.complete(PositionFactory.create(1, 2));
        }
    }

    @Test(dataProvider = "terminalStatuses")
    public void testFailedTerminalAppendKeepsTimeout(TxnStatus intermediateStatus, TxnStatus terminalStatus)
            throws Exception {
        TxnID txnID = store.newTransaction(TimeUnit.HOURS.toMillis(1), null).get(5, TimeUnit.SECONDS);
        store.updateTxnStatus(txnID, intermediateStatus, TxnStatus.OPEN, false).get(5, TimeUnit.SECONDS);
        RuntimeException appendFailure = new RuntimeException("terminal log append failed");
        when(transactionLog.append(any())).thenReturn(CompletableFuture.failedFuture(appendFailure));

        assertThatThrownBy(() -> store.updateTxnStatus(txnID, terminalStatus, intermediateStatus, false)
                .get(5, TimeUnit.SECONDS)).hasRootCause(appendFailure);

        verify(timeoutTracker, never()).removeTransaction(anyLong());
        assertThat(store.getTxnMeta(txnID).get(5, TimeUnit.SECONDS).status()).isEqualTo(intermediateStatus);
        assertThat(store.getOnGoingTxnCount().longValue()).isEqualTo(1);
    }

    @Test(dataProvider = "terminalStatuses")
    public void testFailedLogDeletionStillRemovesTimeout(TxnStatus intermediateStatus, TxnStatus terminalStatus)
            throws Exception {
        TxnID txnID = store.newTransaction(TimeUnit.HOURS.toMillis(1), null).get(5, TimeUnit.SECONDS);
        store.updateTxnStatus(txnID, intermediateStatus, TxnStatus.OPEN, false).get(5, TimeUnit.SECONDS);
        when(transactionLog.deletePosition(anyList()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("log deletion failed")));

        store.updateTxnStatus(txnID, terminalStatus, intermediateStatus, false).get(5, TimeUnit.SECONDS);

        verify(timeoutTracker).removeTransaction(txnID.getLeastSigBits());
        verify(transactionLog).deletePosition(anyList());
        assertThatThrownBy(() -> store.getTxnMeta(txnID).get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(TransactionNotFoundException.class);
        assertThat(store.getOnGoingTxnCount().longValue()).isZero();
    }
}
