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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
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
import org.awaitility.Awaitility;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SnapshotSegmentAbortedTxnProcessorStatsTest {

    private static final int SEGMENTS = 3;
    private static final long ABORTED_MARKER_LEDGER_ID = 10;
    private static final long SEGMENT_LEDGER_ID = 20;

    @Test(timeOut = 30_000)
    @SuppressWarnings("unchecked")
    public void testSegmentStatsWhileRecoveryAddsSegment() throws Exception {
        // Recovery uses this position as the key of the first segment, so a stats call can be paused while it lists
        // that segment.
        PausingPosition firstMarker = new PausingPosition(ABORTED_MARKER_LEDGER_ID, 0);
        AtomicReference<Thread> recoveryThread = new AtomicReference<>();
        ListeningScheduledExecutorService recoveryExecutor =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(() -> {
                        // A static mock applies to the thread that opens it, so only recovery gets the position.
                        try (MockedStatic<PositionFactory> positions =
                                     mockStatic(PositionFactory.class, CALLS_REAL_METHODS)) {
                            positions.when(() -> PositionFactory.create(ABORTED_MARKER_LEDGER_ID, 0))
                                    .thenReturn(firstMarker);
                            runnable.run();
                        }
                    }, "test-snapshot-recovery");
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

            // Recovery gets the last segment only once the stats call is paused while it lists the first one.
            CountDownLatch recoveryAtLastSegment = new CountDownLatch(1);
            AtomicBoolean statsPausedBeforeLastSegment = new AtomicBoolean();
            doAnswer(invocation -> {
                Position position = invocation.getArgument(0);
                if (position.getEntryId() == SEGMENTS - 1) {
                    recoveryAtLastSegment.countDown();
                    statsPausedBeforeLastSegment.set(firstMarker.awaitPaused());
                }
                AsyncCallbacks.ReadEntryCallback callback = invocation.getArgument(1);
                callback.readEntryComplete(EntryImpl.create(position.getLedgerId(), position.getEntryId(),
                        segmentEntries.get((int) position.getEntryId())), invocation.getArgument(2));
                return null;
            }).when(readOnlyManagedLedger).asyncReadEntry(any(), any(), isNull());

            SnapshotSegmentAbortedTxnProcessorImpl recoveringProcessor =
                    new SnapshotSegmentAbortedTxnProcessorImpl(topic);
            processor = recoveringProcessor;
            Thread statsThread = statsExecutor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
            CompletableFuture<Position> recovery = recoveringProcessor.recoverFromSnapshot();
            assertThat(recoveryAtLastSegment.await(10, TimeUnit.SECONDS))
                    .as("recovery reached the last segment")
                    .isTrue();

            firstMarker.pauseNextToString();
            Future<TransactionBufferStats> statsDuringRecovery =
                    statsExecutor.submit(() -> recoveringProcessor.generateSnapshotStats(true));
            try {
                // Recovery either adds the last segment and finishes, or it waits for the lock the stats call holds.
                // Only this wait reports a stall: the gates of the pausing position stay closed for longer.
                Awaitility.await("recovery finished or waiting for the paused stats call")
                        .atMost(10, TimeUnit.SECONDS)
                        .until(() -> recovery.isDone() || isBlockedBy(recoveryThread.get(), statsThread));
            } finally {
                firstMarker.release();
            }

            assertThatCode(() -> statsDuringRecovery.get(5, TimeUnit.SECONDS))
                    .as("generateSnapshotStats while recovery adds a segment")
                    .doesNotThrowAnyException();
            assertThat(statsPausedBeforeLastSegment)
                    .as("the stats call was listing the segments when recovery got the last segment")
                    .isTrue();
            assertThat(statsDuringRecovery.get(5, TimeUnit.SECONDS).segmentsStats.segmentStats)
                    .as("segments recovered before the stats call")
                    .hasSize(SEGMENTS - 1);

            recovery.get(10, TimeUnit.SECONDS);
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
            firstMarker.release();
            statsExecutor.shutdownNow();
            try {
                if (processor != null) {
                    processor.closeAsync().get(5, TimeUnit.SECONDS);
                }
            } finally {
                recoveryExecutor.shutdownNow();
            }
            assertThat(statsExecutor.awaitTermination(5, TimeUnit.SECONDS)).as("stats executor stopped").isTrue();
            assertThat(recoveryExecutor.awaitTermination(5, TimeUnit.SECONDS))
                    .as("recovery executor stopped").isTrue();
        }
    }

    private static boolean isBlockedBy(Thread thread, Thread lockOwner) {
        ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.threadId());
        return info != null && info.getThreadState() == Thread.State.BLOCKED
                && info.getLockOwnerId() == lockOwner.threadId();
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

    /** A position whose {@code toString} can be held, to pause the thread that formats it. */
    private static final class PausingPosition implements Position {
        // Longer than the wait in the test, so that a stall is reported there and not by one of these gates.
        private static final int GATE_TIMEOUT_SECONDS = 30;
        private final long ledgerId;
        private final long entryId;
        private final AtomicBoolean pauseNext = new AtomicBoolean();
        private final CountDownLatch paused = new CountDownLatch(1);
        private final CountDownLatch resumed = new CountDownLatch(1);

        private PausingPosition(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        void pauseNextToString() {
            pauseNext.set(true);
        }

        boolean awaitPaused() throws InterruptedException {
            return paused.await(GATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        /** Lets the paused {@code toString} continue, and any thread that still waits for the pause. */
        void release() {
            paused.countDown();
            resumed.countDown();
        }

        @Override
        public long getLedgerId() {
            return ledgerId;
        }

        @Override
        public long getEntryId() {
            return entryId;
        }

        @Override
        public String toString() {
            if (pauseNext.compareAndSet(true, false)) {
                paused.countDown();
                try {
                    if (!resumed.await(GATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("The paused toString was not released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            return ledgerId + ":" + entryId;
        }

        @Override
        public int hashCode() {
            return hashCodeForPosition();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Position && compareTo((Position) obj) == 0;
        }
    }
}
