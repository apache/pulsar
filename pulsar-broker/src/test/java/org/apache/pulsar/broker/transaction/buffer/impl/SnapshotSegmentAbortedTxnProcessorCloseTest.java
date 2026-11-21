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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Collections;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
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
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SnapshotSegmentAbortedTxnProcessorCloseTest {

    @Test(timeOut = 10_000)
    public void testCloseWaitsForRecoveryIndexUpdate() throws Exception {
        CompletableFuture<MessageId> indexUpdateFuture = new CompletableFuture<>();
        CountDownLatch indexUpdateStarted = new CountDownLatch(1);
        try (var fixture = new RecoveryFixture(5_000)) {
            when(fixture.indexSystemWriter.writeAsync(anyString(), any())).thenAnswer(invocation -> {
                indexUpdateStarted.countDown();
                return indexUpdateFuture;
            });
            doAnswer(invocation -> {
                AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback = invocation.getArgument(1);
                callback.openReadOnlyManagedLedgerComplete(fixture.readOnlyManagedLedger, invocation.getArgument(3));
                return null;
            }).when(fixture.managedLedgerFactory).asyncOpenReadOnlyManagedLedger(anyString(), any(), any(), isNull());
            doAnswer(invocation -> {
                AsyncCallbacks.ReadEntryCallback callback = invocation.getArgument(1);
                callback.readEntryFailed(new ManagedLedgerException("missing segment"), invocation.getArgument(2));
                return null;
            }).when(fixture.readOnlyManagedLedger).asyncReadEntry(any(), any(), isNull());

            try {
                fixture.processor.recoverFromSnapshot().get(5, TimeUnit.SECONDS);
                assertTrue(indexUpdateStarted.await(5, TimeUnit.SECONDS));
                // A later recovery must not replace the in-flight update in the close barrier.
                fixture.processor.recoverFromSnapshot().get(5, TimeUnit.SECONDS);
                verify(fixture.readOnlyManagedLedger, times(2)).asyncClose();

                CompletableFuture<Void> closeFuture = fixture.processor.closeAsync();

                assertFalse(closeFuture.isDone(), "Closing must wait for the recovery index update");
                verify(fixture.indexWriter, never()).release();

                indexUpdateFuture.complete(MessageId.earliest);
                closeFuture.get(5, TimeUnit.SECONDS);
                verify(fixture.indexWriter).release();
            } finally {
                indexUpdateFuture.complete(MessageId.earliest);
            }
        }
    }

    @Test(timeOut = 10_000)
    public void testClosesReadOnlyManagedLedgerWhenOpenCompletesAfterTimeout() throws Exception {
        try (var fixture = new RecoveryFixture(0)) {
            assertThatThrownBy(() -> fixture.processor.recoverFromSnapshot().get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(TimeoutException.class);

            var callback = ArgumentCaptor.forClass(AsyncCallbacks.OpenReadOnlyManagedLedgerCallback.class);
            verify(fixture.managedLedgerFactory)
                    .asyncOpenReadOnlyManagedLedger(anyString(), callback.capture(), any(), isNull());
            verify(fixture.readOnlyManagedLedger, never()).asyncClose();

            callback.getValue().openReadOnlyManagedLedgerComplete(fixture.readOnlyManagedLedger, null);

            verify(fixture.readOnlyManagedLedger).asyncClose();
            verify(fixture.readOnlyManagedLedger, never()).asyncReadEntry(any(), any(), isNull());
        }
    }

    private static final class RecoveryFixture implements AutoCloseable {
        private final ListeningScheduledExecutorService recoveryExecutor =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());
        private final ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);
        private final ReadOnlyManagedLedger readOnlyManagedLedger = mock(ReadOnlyManagedLedger.class);
        private final ReferenceCountedWriter<TransactionBufferSnapshotIndexes> indexWriter;
        private final SystemTopicClient.Writer<TransactionBufferSnapshotIndexes> indexSystemWriter;
        private final SnapshotSegmentAbortedTxnProcessorImpl processor;

        @SuppressWarnings("unchecked")
        RecoveryFixture(long operationTimeoutMs) throws Exception {
            String topicName = "persistent://public/default/test-close-during-index-update";
            PersistentTopic topic = mock(PersistentTopic.class);
            BrokerService brokerService = mock(BrokerService.class);
            PulsarService pulsar = mock(PulsarService.class);
            ServiceConfiguration configuration = mock(ServiceConfiguration.class);
            PulsarClientImpl client = mock(PulsarClientImpl.class);
            ClientConfigurationData clientConfiguration = new ClientConfigurationData();
            clientConfiguration.setOperationTimeoutMs(operationTimeoutMs);
            OrderedScheduler recoveryScheduler = mock(OrderedScheduler.class);
            TransactionBufferSnapshotServiceFactory serviceFactory =
                    mock(TransactionBufferSnapshotServiceFactory.class);

            when(topic.getName()).thenReturn(topicName);
            when(topic.getBrokerService()).thenReturn(brokerService);
            when(brokerService.getPulsar()).thenReturn(pulsar);
            when(pulsar.getConfiguration()).thenReturn(configuration);
            when(pulsar.getClient()).thenReturn(client);
            when(client.getConfiguration()).thenReturn(clientConfiguration);
            when(configuration.getTransactionBufferSnapshotSegmentSize()).thenReturn(1024);
            when(pulsar.getTransactionSnapshotRecoverExecutorProvider()).thenReturn(recoveryScheduler);
            when(recoveryScheduler.chooseThread(any(Object.class))).thenReturn(recoveryExecutor);
            when(pulsar.getTransactionBufferSnapshotServiceFactory()).thenReturn(serviceFactory);

            SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshotSegment> segmentService =
                    mock(SystemTopicTxnBufferSnapshotService.class);
            SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshotIndexes> indexService =
                    mock(SystemTopicTxnBufferSnapshotService.class);
            ReferenceCountedWriter<TransactionBufferSnapshotSegment> segmentWriter =
                    mock(ReferenceCountedWriter.class);
            indexWriter = mock(ReferenceCountedWriter.class);
            SystemTopicClient.Writer<TransactionBufferSnapshotSegment> segmentSystemWriter =
                    mock(SystemTopicClient.Writer.class);
            indexSystemWriter = mock(SystemTopicClient.Writer.class);

            when(serviceFactory.getTxnBufferSnapshotSegmentService()).thenReturn(segmentService);
            when(serviceFactory.getTxnBufferSnapshotIndexService()).thenReturn(indexService);
            when(segmentService.getReferenceWriter(any())).thenReturn(segmentWriter);
            when(indexService.getReferenceWriter(any())).thenReturn(indexWriter);
            when(segmentWriter.getFuture()).thenReturn(CompletableFuture.completedFuture(segmentSystemWriter));
            when(indexWriter.getFuture()).thenReturn(CompletableFuture.completedFuture(indexSystemWriter));

            TransactionBufferSnapshotIndex invalidIndex = new TransactionBufferSnapshotIndex(0, 1, 1, 2, 2);
            TransactionBufferSnapshotIndexesMetadata metadata =
                    new TransactionBufferSnapshotIndexesMetadata(3, 3, Collections.emptyList());
            TransactionBufferSnapshotIndexes indexes =
                    new TransactionBufferSnapshotIndexes(topicName, Collections.singletonList(invalidIndex), metadata);
            TableView<TransactionBufferSnapshotIndexes> tableView = mock(TableView.class);
            when(indexService.getTableView(recoveryExecutor)).thenReturn(tableView);
            when(tableView.readLatest(topicName)).thenReturn(indexes);

            ManagedLedgerImpl managedLedger = mock(ManagedLedgerImpl.class);
            when(topic.getManagedLedger()).thenReturn(managedLedger);
            when(managedLedger.getConfig()).thenReturn(new ManagedLedgerConfig());
            when(managedLedger.getLedgersInfo()).thenReturn(new TreeMap<>());
            when(readOnlyManagedLedger.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
            when(brokerService.getManagedLedgerFactoryForTopic(any()))
                    .thenReturn(CompletableFuture.completedFuture(managedLedgerFactory));

            processor = new SnapshotSegmentAbortedTxnProcessorImpl(topic);
        }

        @Override
        public void close() throws Exception {
            try {
                processor.closeAsync().get(5, TimeUnit.SECONDS);
            } finally {
                recoveryExecutor.shutdownNow();
                assertTrue(recoveryExecutor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }
}
