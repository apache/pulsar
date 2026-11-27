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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
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
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TxnIDData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.SegmentStats;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SnapshotSegmentAbortedTxnProcessorStatsTest {

    private static final int SEGMENTS = 1_000;
    private static final long ABORTED_MARKER_LEDGER_ID = 10;
    private static final long SEGMENT_LEDGER_ID = 20;

    @Test(timeOut = 30_000)
    @SuppressWarnings("unchecked")
    public void testSegmentStatsDuringRecovery() throws Exception {
        AtomicReference<Thread> recoveryThread = new AtomicReference<>();
        ListeningScheduledExecutorService recoveryExecutor =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "test-snapshot-recovery");
                    recoveryThread.set(thread);
                    return thread;
                }));
        ExecutorService statsExecutor = Executors.newSingleThreadExecutor();
        SnapshotSegmentAbortedTxnProcessorImpl processor = null;
        try {
            String topicName = "persistent://public/default/test-segment-stats-during-recovery";
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
            when(serviceFactory.getTxnBufferSnapshotSegmentService()).thenReturn(segmentService);
            when(serviceFactory.getTxnBufferSnapshotIndexService()).thenReturn(indexService);
            when(segmentService.getReferenceWriter(any())).thenReturn(segmentWriter);
            when(indexService.getReferenceWriter(any())).thenReturn(indexWriter);
            when(segmentWriter.getFuture())
                    .thenReturn(CompletableFuture.completedFuture(mock(SystemTopicClient.Writer.class)));
            when(indexWriter.getFuture())
                    .thenReturn(CompletableFuture.completedFuture(mock(SystemTopicClient.Writer.class)));

            // Each segment holds one aborted transaction, written after the abort marker at (10, i).
            Schema<TransactionBufferSnapshotSegment> segmentSchema =
                    Schema.AVRO(TransactionBufferSnapshotSegment.class);
            List<TransactionBufferSnapshotIndex> indexList = new ArrayList<>(SEGMENTS);
            List<byte[]> segmentEntries = new ArrayList<>(SEGMENTS);
            List<String> expectedPositions = new ArrayList<>(SEGMENTS);
            List<String> expectedLastTxnIds = new ArrayList<>(SEGMENTS);
            for (int i = 0; i < SEGMENTS; i++) {
                indexList.add(new TransactionBufferSnapshotIndex(i, ABORTED_MARKER_LEDGER_ID, i,
                        SEGMENT_LEDGER_ID, i));
                segmentEntries.add(encodeSegment(segmentSchema, topicName, i));
                expectedPositions.add(PositionFactory.create(ABORTED_MARKER_LEDGER_ID, i).toString());
                expectedLastTxnIds.add(new TxnID(1, i).toString());
            }
            TransactionBufferSnapshotIndexes indexes = new TransactionBufferSnapshotIndexes(topicName, indexList,
                    new TransactionBufferSnapshotIndexesMetadata(ABORTED_MARKER_LEDGER_ID, SEGMENTS,
                            Collections.emptyList()));
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

            // Recovery reads the first segment only once the stats thread is polling.
            CountDownLatch statsPollingStarted = new CountDownLatch(1);
            AtomicBoolean pollingStartedBeforeFirstRead = new AtomicBoolean();
            AtomicInteger segmentReads = new AtomicInteger();
            doAnswer(invocation -> {
                Position position = invocation.getArgument(0);
                if (position.getEntryId() == 0) {
                    pollingStartedBeforeFirstRead.set(statsPollingStarted.await(5, TimeUnit.SECONDS));
                }
                segmentReads.incrementAndGet();
                AsyncCallbacks.ReadEntryCallback callback = invocation.getArgument(1);
                callback.readEntryComplete(EntryImpl.create(position.getLedgerId(), position.getEntryId(),
                        segmentEntries.get((int) position.getEntryId())), invocation.getArgument(2));
                return null;
            }).when(readOnlyManagedLedger).asyncReadEntry(any(), any(), isNull());

            SnapshotSegmentAbortedTxnProcessorImpl recoveringProcessor =
                    new SnapshotSegmentAbortedTxnProcessorImpl(topic);
            processor = recoveringProcessor;
            AtomicBoolean recoveryFinished = new AtomicBoolean();
            AtomicInteger callsDuringSegmentReads = new AtomicInteger();
            // The first exception ends the polling and fails this future.
            Future<?> statsPolling = statsExecutor.submit(() -> {
                while (!recoveryFinished.get()) {
                    int readsBefore = segmentReads.get();
                    recoveringProcessor.generateSnapshotStats(true);
                    statsPollingStarted.countDown();
                    if (readsBefore > 0 && segmentReads.get() < SEGMENTS) {
                        callsDuringSegmentReads.incrementAndGet();
                    }
                    // Back-to-back polls could starve a recovery thread that waits for a lock, so let it in first.
                    Thread recovery = recoveryThread.get();
                    while (recovery != null && !recoveryFinished.get()
                            && (recovery.getState() == Thread.State.BLOCKED
                            || recovery.getState() == Thread.State.WAITING)) {
                        Thread.onSpinWait();
                    }
                }
            });
            try {
                recoveringProcessor.recoverFromSnapshot().get(10, TimeUnit.SECONDS);
            } finally {
                recoveryFinished.set(true);
            }

            assertThatCode(() -> statsPolling.get(5, TimeUnit.SECONDS))
                    .as("generateSnapshotStats calls while snapshot recovery runs")
                    .doesNotThrowAnyException();
            assertThat(pollingStartedBeforeFirstRead)
                    .as("stats polling started before recovery read the first segment")
                    .isTrue();
            assertThat(callsDuringSegmentReads.get())
                    .as("generateSnapshotStats calls made between the first and the last segment read")
                    .isPositive();
            TransactionBufferStats stats = recoveringProcessor.generateSnapshotStats(true);
            assertThat(stats.segmentsStats.segmentsSize).as("recovered segment indexes").isEqualTo(SEGMENTS);
            assertThat(stats.totalAbortedTransactions).as("recovered aborted transactions").isEqualTo(SEGMENTS);
            List<SegmentStats> segmentStats = stats.segmentsStats.segmentStats;
            assertThat(segmentStats.stream().map(segment -> segment.persistentPosition).toList())
                    .as("persistent positions of the recovered segments")
                    .containsExactlyElementsOf(expectedPositions);
            assertThat(segmentStats.stream().map(segment -> segment.lastTxnID).toList())
                    .as("last aborted transactions of the recovered segments")
                    .containsExactlyElementsOf(expectedLastTxnIds);
        } finally {
            statsExecutor.shutdownNow();
            if (processor != null) {
                processor.closeAsync().get(5, TimeUnit.SECONDS);
            }
            recoveryExecutor.shutdownNow();
            assertThat(statsExecutor.awaitTermination(5, TimeUnit.SECONDS)).as("stats executor stopped").isTrue();
            assertThat(recoveryExecutor.awaitTermination(5, TimeUnit.SECONDS))
                    .as("recovery executor stopped").isTrue();
        }
    }

    private static byte[] encodeSegment(Schema<TransactionBufferSnapshotSegment> schema, String topicName,
                                        int sequenceId) {
        TransactionBufferSnapshotSegment segment = new TransactionBufferSnapshotSegment(topicName, sequenceId,
                ABORTED_MARKER_LEDGER_ID, sequenceId, List.of(new TxnIDData(1, sequenceId)));
        MessageMetadata metadata = new MessageMetadata().setProducerName("test").setSequenceId(sequenceId)
                .setPublishTime(1L);
        ByteBuf payload = Unpooled.wrappedBuffer(schema.encode(segment));
        ByteBuf data = Commands.serializeMetadataAndPayload(Commands.ChecksumType.None, metadata, payload);
        try {
            return ByteBufUtil.getBytes(data);
        } finally {
            data.release();
            payload.release();
        }
    }
}
