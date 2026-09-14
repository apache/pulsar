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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SnapshotSegmentAbortedTxnProcessorCloseTest {

    @Test(timeOut = 10_000)
    @SuppressWarnings("unchecked")
    public void testCloseWaitsForRecoveryIndexUpdate() throws Exception {
        ListeningScheduledExecutorService recoveryExecutor =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());
        CompletableFuture<MessageId> indexUpdateFuture = new CompletableFuture<>();
        CountDownLatch indexUpdateStarted = new CountDownLatch(1);
        SnapshotSegmentAbortedTxnProcessorImpl processor = null;
        try {
            String topicName = "persistent://public/default/test-close-during-index-update";
            PersistentTopic topic = mock(PersistentTopic.class);
            BrokerService brokerService = mock(BrokerService.class);
            PulsarService pulsar = mock(PulsarService.class);
            ServiceConfiguration configuration = mock(ServiceConfiguration.class);
            PulsarClientImpl client = mock(PulsarClientImpl.class);
            ClientConfigurationData clientConfiguration = new ClientConfigurationData();
            clientConfiguration.setOperationTimeoutMs(5_000);
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
            ReferenceCountedWriter<TransactionBufferSnapshotIndexes> indexWriter =
                    mock(ReferenceCountedWriter.class);
            SystemTopicClient.Writer<TransactionBufferSnapshotSegment> segmentSystemWriter =
                    mock(SystemTopicClient.Writer.class);
            SystemTopicClient.Writer<TransactionBufferSnapshotIndexes> indexSystemWriter =
                    mock(SystemTopicClient.Writer.class);

            when(serviceFactory.getTxnBufferSnapshotSegmentService()).thenReturn(segmentService);
            when(serviceFactory.getTxnBufferSnapshotIndexService()).thenReturn(indexService);
            when(segmentService.getReferenceWriter(any())).thenReturn(segmentWriter);
            when(indexService.getReferenceWriter(any())).thenReturn(indexWriter);
            when(segmentWriter.getFuture()).thenReturn(CompletableFuture.completedFuture(segmentSystemWriter));
            when(indexWriter.getFuture()).thenReturn(CompletableFuture.completedFuture(indexSystemWriter));
            when(indexSystemWriter.writeAsync(anyString(), any())).thenAnswer(invocation -> {
                indexUpdateStarted.countDown();
                return indexUpdateFuture;
            });

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
            ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);
            ReadOnlyManagedLedger readOnlyManagedLedger = mock(ReadOnlyManagedLedger.class);
            when(brokerService.getManagedLedgerFactoryForTopic(any()))
                    .thenReturn(CompletableFuture.completedFuture(managedLedgerFactory));
            doAnswer(invocation -> {
                AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback = invocation.getArgument(1);
                callback.openReadOnlyManagedLedgerComplete(readOnlyManagedLedger, invocation.getArgument(3));
                return null;
            }).when(managedLedgerFactory).asyncOpenReadOnlyManagedLedger(anyString(), any(), any(), isNull());
            doAnswer(invocation -> {
                AsyncCallbacks.ReadEntryCallback callback = invocation.getArgument(1);
                callback.readEntryFailed(new ManagedLedgerException("missing segment"), invocation.getArgument(2));
                return null;
            }).when(readOnlyManagedLedger).asyncReadEntry(any(), any(), isNull());

            processor = new SnapshotSegmentAbortedTxnProcessorImpl(topic);
            processor.recoverFromSnapshot().get(5, TimeUnit.SECONDS);
            assertTrue(indexUpdateStarted.await(5, TimeUnit.SECONDS));
            // A later recovery must not replace the in-flight update in the close barrier.
            processor.recoverFromSnapshot().get(5, TimeUnit.SECONDS);

            CompletableFuture<Void> closeFuture = processor.closeAsync();

            assertFalse(closeFuture.isDone(), "Closing must wait for the recovery index update");
            verify(indexWriter, never()).release();

            indexUpdateFuture.complete(MessageId.earliest);
            closeFuture.get(5, TimeUnit.SECONDS);
            verify(indexWriter).release();
        } finally {
            indexUpdateFuture.complete(MessageId.earliest);
            if (processor != null) {
                processor.closeAsync().get(5, TimeUnit.SECONDS);
            }
            recoveryExecutor.shutdownNow();
            assertTrue(recoveryExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
