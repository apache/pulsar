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
package org.apache.pulsar.broker.transaction.buffer.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Collections;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.SystemTopicTxnBufferSnapshotService;
import org.apache.pulsar.broker.service.SystemTopicTxnBufferSnapshotService.ReferenceCountedWriter;
import org.apache.pulsar.broker.service.TransactionBufferSnapshotServiceFactory;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndex;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndexes;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndexesMetadata;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotSegment;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker", timeOut = 30000)
public class SnapshotSegmentAbortedTxnProcessorResourceTest {

    @Test
    public void testLedgerArrivingAfterOpenTimeoutIsClosed() throws Exception {
        try (RecoveryFixture fixture = new RecoveryFixture(true, false)) {
            CompletableFuture<Position> recovery = fixture.processor.recoverFromSnapshot();
            AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback = fixture.openRequested.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> recovery.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
            verify(fixture.readOnly, never()).asyncClose();

            callback.openReadOnlyManagedLedgerComplete(fixture.readOnly, null);

            verify(fixture.readOnly).asyncClose();
        }
    }

    @Test
    public void testEntryArrivingAfterReadTimeoutIsReleased() throws Exception {
        try (RecoveryFixture fixture = new RecoveryFixture(false, true)) {
            CompletableFuture<Position> recovery = fixture.processor.recoverFromSnapshot();
            AsyncCallbacks.ReadEntryCallback callback = fixture.readRequested.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> recovery.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
            verify(fixture.readOnly).asyncClose();
            Entry lateEntry = mock(Entry.class);

            callback.readEntryComplete(lateEntry, null);

            verify(lateEntry).release();
        }
    }

    @DataProvider(name = "closeFailureModes")
    public Object[][] closeFailureModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "closeFailureModes")
    public void testCloseFailurePreservesRecoveryFailure(boolean synchronous) throws Exception {
        try (RecoveryFixture fixture = new RecoveryFixture(false, false)) {
            IllegalStateException closeFailure = new IllegalStateException("close failed");
            if (synchronous) {
                when(fixture.readOnly.asyncClose()).thenThrow(closeFailure);
            } else {
                when(fixture.readOnly.asyncClose()).thenReturn(CompletableFuture.failedFuture(closeFailure));
            }
            assertThatThrownBy(() -> fixture.processor.recoverFromSnapshot().get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class).hasRootCauseMessage("missing segment");
            verify(fixture.readOnly).asyncClose();
        }
    }

    private static final class RecoveryFixture implements AutoCloseable {
        private final ListeningScheduledExecutorService executor =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());
        private final ReadOnlyManagedLedger readOnly = mock(ReadOnlyManagedLedger.class);
        private final CompletableFuture<AsyncCallbacks.OpenReadOnlyManagedLedgerCallback> openRequested =
                new CompletableFuture<>();
        private final CompletableFuture<AsyncCallbacks.ReadEntryCallback> readRequested = new CompletableFuture<>();
        private final SnapshotSegmentAbortedTxnProcessorImpl processor;

        @SuppressWarnings("unchecked")
        private RecoveryFixture(boolean delayOpen, boolean delayRead) throws Exception {
            String topicName = "persistent://public/default/test-snapshot-resource-lifecycle";
            PersistentTopic topic = mock(PersistentTopic.class);
            BrokerService brokerService = mock(BrokerService.class);
            PulsarService pulsar = mock(PulsarService.class);
            ServiceConfiguration configuration = mock(ServiceConfiguration.class);
            PulsarClientImpl client = mock(PulsarClientImpl.class);
            ClientConfigurationData clientConfiguration = new ClientConfigurationData();
            clientConfiguration.setOperationTimeoutMs(1);
            OrderedScheduler scheduler = mock(OrderedScheduler.class);
            TransactionBufferSnapshotServiceFactory services = mock(TransactionBufferSnapshotServiceFactory.class);
            when(topic.getName()).thenReturn(topicName);
            when(topic.getBrokerService()).thenReturn(brokerService);
            when(brokerService.getPulsar()).thenReturn(pulsar);
            when(pulsar.getConfiguration()).thenReturn(configuration);
            when(pulsar.getClient()).thenReturn(client);
            when(client.getConfiguration()).thenReturn(clientConfiguration);
            when(configuration.getTransactionBufferSnapshotSegmentSize()).thenReturn(1024);
            when(pulsar.getTransactionSnapshotRecoverExecutorProvider()).thenReturn(scheduler);
            when(scheduler.chooseThread(any(Object.class))).thenReturn(executor);
            when(pulsar.getTransactionBufferSnapshotServiceFactory()).thenReturn(services);

            SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshotSegment> segments =
                    mock(SystemTopicTxnBufferSnapshotService.class);
            SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshotIndexes> indexesService =
                    mock(SystemTopicTxnBufferSnapshotService.class);
            ReferenceCountedWriter<TransactionBufferSnapshotSegment> segmentWriter = mock(ReferenceCountedWriter.class);
            ReferenceCountedWriter<TransactionBufferSnapshotIndexes> indexWriter = mock(ReferenceCountedWriter.class);
            SystemTopicClient.Writer<TransactionBufferSnapshotSegment> segmentSystemWriter =
                    mock(SystemTopicClient.Writer.class);
            SystemTopicClient.Writer<TransactionBufferSnapshotIndexes> indexSystemWriter =
                    mock(SystemTopicClient.Writer.class);
            when(services.getTxnBufferSnapshotSegmentService()).thenReturn(segments);
            when(services.getTxnBufferSnapshotIndexService()).thenReturn(indexesService);
            when(segments.getReferenceWriter(any())).thenReturn(segmentWriter);
            when(indexesService.getReferenceWriter(any())).thenReturn(indexWriter);
            when(segmentWriter.getFuture()).thenReturn(CompletableFuture.completedFuture(segmentSystemWriter));
            when(indexWriter.getFuture()).thenReturn(CompletableFuture.completedFuture(indexSystemWriter));
            when(indexSystemWriter.writeAsync(anyString(), any()))
                    .thenReturn(CompletableFuture.completedFuture(MessageId.earliest));
            TransactionBufferSnapshotIndex index = new TransactionBufferSnapshotIndex(0, 1, 1, 2, 2);
            TransactionBufferSnapshotIndexesMetadata metadata =
                    new TransactionBufferSnapshotIndexesMetadata(3, 3, Collections.emptyList());
            TransactionBufferSnapshotIndexes indexes =
                    new TransactionBufferSnapshotIndexes(topicName, Collections.singletonList(index), metadata);
            TableView<TransactionBufferSnapshotIndexes> tableView = mock(TableView.class);
            when(indexesService.getTableView(executor)).thenReturn(tableView);
            when(tableView.readLatest(topicName)).thenReturn(indexes);

            ManagedLedgerImpl ledger = mock(ManagedLedgerImpl.class);
            TreeMap<Long, LedgerInfo> ledgerInfos = new TreeMap<>();
            ledgerInfos.put(index.getAbortedMarkLedgerID(),
                    new LedgerInfo().setLedgerId(index.getAbortedMarkLedgerID()));
            when(topic.getManagedLedger()).thenReturn(ledger);
            when(ledger.getConfig()).thenReturn(new ManagedLedgerConfig());
            when(ledger.getLedgersInfo()).thenReturn(ledgerInfos);
            ManagedLedgerFactory factory = mock(ManagedLedgerFactory.class);
            when(brokerService.getManagedLedgerFactoryForTopic(any()))
                    .thenReturn(CompletableFuture.completedFuture(factory));
            when(readOnly.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
            doAnswer(invocation -> {
                AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback = invocation.getArgument(1);
                openRequested.complete(callback);
                if (!delayOpen) {
                    callback.openReadOnlyManagedLedgerComplete(readOnly, invocation.getArgument(3));
                }
                return null;
            }).when(factory).asyncOpenReadOnlyManagedLedger(anyString(), any(), any(), isNull());
            doAnswer(invocation -> {
                AsyncCallbacks.ReadEntryCallback callback = invocation.getArgument(1);
                readRequested.complete(callback);
                if (!delayRead) {
                    callback.readEntryFailed(new ManagedLedgerException("missing segment"), invocation.getArgument(2));
                }
                return null;
            }).when(readOnly).asyncReadEntry(any(), any(), isNull());
            processor = new SnapshotSegmentAbortedTxnProcessorImpl(topic);
        }

        @Override
        public void close() throws Exception {
            try {
                processor.closeAsync().get(5, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }
}
