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
package org.apache.pulsar.broker.transaction.timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionLogReplayCallback;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.TransactionNotFoundException;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionSequenceIdGenerator;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TransactionTimeoutRetentionTest {
    @DataProvider(name = "terminalStatuses")
    public Object[][] terminalStatuses() {
        return new Object[][] {
                {TxnStatus.COMMITTING, TxnStatus.COMMITTED},
                {TxnStatus.ABORTING, TxnStatus.ABORTED}
        };
    }

    @Test(dataProvider = "terminalStatuses")
    public void testCompletedTransactionDoesNotRetainTimeout(TxnStatus intermediate, TxnStatus terminal)
            throws Exception {
        Timer timer = mock(Timer.class);
        Timeout transactionTimeout = mock(Timeout.class);
        TransactionMetadataStoreService service = mock(TransactionMetadataStoreService.class);
        List<Long> delays = new ArrayList<>();
        when(timer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
            delays.add(invocation.<TimeUnit>getArgument(2).toMillis(invocation.getArgument(1)));
            return transactionTimeout;
        });
        TransactionTimeoutTrackerImpl realTracker = new TransactionTimeoutTrackerImpl(1, timer, 100, service);
        MLTransactionLogImpl log = mock(MLTransactionLogImpl.class);
        AtomicLong entryId = new AtomicLong();
        when(log.append(any())).thenAnswer(invocation ->
                CompletableFuture.completedFuture(PositionFactory.create(1, entryId.getAndIncrement())));
        when(log.deletePosition(anyList())).thenReturn(CompletableFuture.completedFuture(null));
        when(log.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        doAnswer(invocation -> {
            invocation.<TransactionLogReplayCallback>getArgument(0).replayComplete();
            return null;
        }).when(log).replayAsync(any());
        MLTransactionMetadataStore store = new MLTransactionMetadataStore(TransactionCoordinatorID.get(1), log,
                realTracker, new MLTransactionSequenceIdGenerator(), 1);
        try {
            store.init(mock(TransactionRecoverTracker.class)).get(5, TimeUnit.SECONDS);
            TxnID id = store.newTransaction(TimeUnit.HOURS.toMillis(1), null).get(5, TimeUnit.SECONDS);
            store.updateTxnStatus(id, intermediate, TxnStatus.OPEN, false).get(5, TimeUnit.SECONDS);
            store.updateTxnStatus(id, terminal, intermediate, false).get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> store.getTxnMeta(id).get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TransactionNotFoundException.class);
            assertThat(store.getMetadataStoreStats().getActives()).isZero();

            realTracker.run(transactionTimeout);
            assertThat(delays).as("completed transaction must not schedule another timeout").hasSize(1);
            verifyNoInteractions(service);
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }
}
