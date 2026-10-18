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
package org.apache.bookkeeper.mledger.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore.OperationType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedLedgerOfflineBacklogTest extends MockedBookKeeperTestCase {

    @DataProvider(name = "accuracy")
    public Object[][] accuracy() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "accuracy", timeOut = 15000)
    public void testCursorMetadataCallbacksDoNotBlockTheirExecutor(boolean accurate) throws Exception {
        TopicName topicName = TopicName.get("persistent://tenant/namespace/offline-backlog");
        String managedLedgerName = topicName.getPersistenceNamingEncoding();
        createOfflineTopic(topicName);

        Thread metadataWorker = factory.getScheduledExecutor().chooseThread(managedLedgerName)
                .submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
        ManagedLedgerOfflineBacklog backlog =
                new ManagedLedgerOfflineBacklog(DigestType.CRC32, new byte[0], "broker", accurate);
        CompletableFuture<PersistentOfflineTopicStats> result = CompletableFuture.supplyAsync(() -> {
            try {
                return backlog.estimateUnloadedTopicBacklog(factory, topicName);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, cachedExecutor);
        try {
            PersistentOfflineTopicStats stats = result.get(5, TimeUnit.SECONDS);
            assertThat(stats.totalMessages).isEqualTo(3);
            assertThat(stats.messageBacklog).isEqualTo(3);
            assertThat(stats.cursorDetails).containsOnlyKeys("subscription");
            assertThat(stats.cursorDetails.get("subscription").cursorBacklog).isEqualTo(3);
            factory.getScheduledExecutor().chooseThread(managedLedgerName)
                    .submit(() -> null).get(5, TimeUnit.SECONDS);
        } finally {
            if (!result.isDone()) {
                // The unfixed code waits for a cursor callback queued behind itself on this worker.
                // Interrupt only on failure so the regression also terminates against that version.
                metadataWorker.interrupt();
                result.handle((stats, error) -> null).get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test(dataProvider = "accuracy", timeOut = 15000)
    public void testAsyncEstimateCanStartOnTheMetadataCallbackThread(boolean accurate) throws Exception {
        TopicName topicName = TopicName.get("persistent://tenant/namespace/offline-async");
        createOfflineTopic(topicName);
        CompletableFuture<PersistentOfflineTopicStats> result = new CompletableFuture<>();
        CompletableFuture<Void> heartbeat = new CompletableFuture<>();
        factory.getMetaStore().getCursors(topicName.getPersistenceNamingEncoding(), new MetaStoreCallback<>() {
            @Override
            public void operationComplete(List<String> cursors, Stat stat) {
                ManagedLedgerOfflineBacklog backlog =
                        new ManagedLedgerOfflineBacklog(DigestType.CRC32, new byte[0], "broker", accurate);
                backlog.estimateUnloadedTopicBacklogAsync(factory, topicName, cachedExecutor)
                        .whenComplete((stats, error) -> {
                            if (error == null) {
                                result.complete(stats);
                            } else {
                                result.completeExceptionally(error);
                            }
                        });
                factory.getScheduledExecutor().executeOrdered(topicName.getPersistenceNamingEncoding(),
                        () -> heartbeat.complete(null));
            }

            @Override
            public void operationFailed(ManagedLedgerException.MetaStoreException error) {
                result.completeExceptionally(error);
            }
        });
        heartbeat.get(5, TimeUnit.SECONDS);
        PersistentOfflineTopicStats stats = result.get(5, TimeUnit.SECONDS);
        assertThat(stats.totalMessages).isEqualTo(3);
        assertThat(stats.messageBacklog).isEqualTo(3);
        assertThat(stats.dataLedgerDetails).hasSize(accurate ? 1 : 0);
    }

    @Test(timeOut = 15000)
    public void testMixedMetadataAndLedgerCursorsAreAggregated() throws Exception {
        TopicName topicName = TopicName.get("persistent://tenant/namespace/offline-many-cursors");
        Position lastPosition = createOfflineTopic(topicName);
        writeCursorInfo(topicName, "subscription", -1, lastPosition.getLedgerId(), -1);
        writeCursorInfo(topicName, "metadata-cursor", -1, lastPosition.getLedgerId(), 0);
        CountDownLatch readsReady = new CountDownLatch(12);
        CompletableFuture<Void> releaseReads = new CompletableFuture<>();
        List<LedgerEntries> observedEntries = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 12; i++) {
            LedgerHandle cursorLedger = bkc.createLedger(BookKeeper.DigestType.CRC32, new byte[0]);
            cursorLedger.addEntry(new PositionInfo().setLedgerId(lastPosition.getLedgerId())
                    .setEntryId(i % 3 - 1).toByteArray());
            cursorLedger.close();
            writeCursorInfo(topicName, "ledger-cursor-" + i, cursorLedger.getId(), -1, -1);
        }
        bkc.setReadHandleInterceptor((ledgerId, first, last, entries) -> {
            LedgerEntries observed = spy(entries);
            observedEntries.add(observed);
            readsReady.countDown();
            return releaseReads.thenApplyAsync(ignored -> observed, cachedExecutor);
        });
        ManagedLedgerOfflineBacklog backlog =
                new ManagedLedgerOfflineBacklog(DigestType.CRC32, new byte[0], "broker", true);
        CompletableFuture<PersistentOfflineTopicStats> result =
                backlog.estimateUnloadedTopicBacklogAsync(factory, topicName, cachedExecutor);
        try {
            assertThat(readsReady.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(result.isDone()).isFalse();
        } finally {
            releaseReads.complete(null);
        }
        PersistentOfflineTopicStats stats = result.get(5, TimeUnit.SECONDS);
        assertThat(stats.totalMessages).isEqualTo(3);
        assertThat(stats.messageBacklog).isEqualTo(29);
        assertThat(stats.cursorDetails).hasSize(14);
        assertThat(observedEntries).hasSize(12);
        observedEntries.forEach(entries -> verify(entries).close());
        assertThat(stats.cursorDetails.get("subscription").cursorBacklog).isEqualTo(3);
        assertThat(stats.cursorDetails.get("metadata-cursor").cursorBacklog).isEqualTo(2);
        for (int i = 0; i < 12; i++) {
            assertThat(stats.cursorDetails.get("ledger-cursor-" + i).cursorBacklog).isEqualTo(3 - i % 3);
        }
    }

    @Test(timeOut = 15000)
    public void testCalculateCursorBacklogsPreservesExistingStats() throws Exception {
        TopicName topicName = TopicName.get("persistent://tenant/namespace/offline-existing-stats");
        Position lastPosition = createOfflineTopic(topicName);
        NavigableMap<Long, ManagedLedgerInfo.LedgerInfo> ledgers = new TreeMap<>();
        ledgers.put(lastPosition.getLedgerId(), new ManagedLedgerInfo.LedgerInfo()
                .setLedgerId(lastPosition.getLedgerId()).setEntries(3).setSize(21));
        PersistentOfflineTopicStats stats = new PersistentOfflineTopicStats(topicName.toString(), "broker");
        stats.messageBacklog = 5;
        stats.addCursorDetails("existing", 2, 99);

        factory.calculateCursorBacklogs(topicName, ledgers, stats, false, BookKeeper.DigestType.CRC32, new byte[0]);

        assertThat(stats.messageBacklog).isEqualTo(8);
        assertThat(stats.cursorDetails).containsOnlyKeys("existing", "subscription");
        assertThat(stats.cursorDetails.get("existing").cursorBacklog).isEqualTo(2);
        assertThat(stats.cursorDetails.get("existing").cursorLedgerId).isEqualTo(99);
        assertThat(stats.cursorDetails.get("subscription").cursorBacklog).isEqualTo(3);
    }

    @Test(timeOut = 15000)
    public void testTimeoutBeforeManagedLedgerMetadataDoesNotStartMoreReads() throws Exception {
        TopicName topicName = TopicName.get("persistent://tenant/namespace/offline-metadata-timeout");
        createOfflineTopic(topicName);
        MetaStore actualStore = factory.getMetaStore();
        MetaStore delayedStore = spy(actualStore);
        BookKeeper observedBookKeeper = spy(bkc);
        ManagedLedgerFactoryImpl estimatorFactory = spy(factory);
        doReturn(delayedStore).when(estimatorFactory).getMetaStore();
        doReturn(CompletableFuture.completedFuture(observedBookKeeper)).when(estimatorFactory).getBookKeeper();
        CompletableFuture<Runnable> delayedCompletion = new CompletableFuture<>();
        doAnswer(invocation -> {
            MetaStoreCallback<ManagedLedgerInfo> callback = invocation.getArgument(2);
            actualStore.getManagedLedgerInfo(topicName.getPersistenceNamingEncoding(), false,
                    new MetaStoreCallback<>() {
                        @Override
                        public void operationComplete(ManagedLedgerInfo info, Stat stat) {
                            delayedCompletion.complete(() -> callback.operationComplete(info, stat));
                        }

                        @Override
                        public void operationFailed(ManagedLedgerException.MetaStoreException error) {
                            delayedCompletion.complete(() -> callback.operationFailed(error));
                        }
                    });
            return null;
        }).when(delayedStore).getManagedLedgerInfo(eq(topicName.getPersistenceNamingEncoding()), eq(false), any());
        PersistentOfflineTopicStats stats = new PersistentOfflineTopicStats(topicName.toString(), "broker");
        stats.totalMessages = 17;
        stats.storageSize = 19;
        stats.messageBacklog = 23;
        long generatedAt = stats.statGeneratedAt.getTime();
        CompletableFuture<Void> result = estimatorFactory.estimateUnloadedTopicBacklogAsync(stats, topicName, false,
                List.of(BookKeeper.DigestType.CRC32, new byte[0]), cachedExecutor, 100, TimeUnit.MILLISECONDS);
        Runnable completeMetadata = delayedCompletion.get(5, TimeUnit.SECONDS);
        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(TimeoutException.class);

        factory.getScheduledExecutor().chooseThread(topicName.getPersistenceNamingEncoding())
                .submit(completeMetadata).get(5, TimeUnit.SECONDS);
        factory.getScheduledExecutor().chooseThread(topicName.getPersistenceNamingEncoding())
                .submit(() -> null).get(5, TimeUnit.SECONDS);

        verify(observedBookKeeper, never()).newOpenLedgerOp();
        verify(delayedStore, never()).getCursors(any(), any());
        assertStatsUnchanged(stats, generatedAt);
    }

    @Test(timeOut = 15000)
    public void testTimeoutDoesNotPublishOrMutatePartialStats() throws Exception {
        TopicName topicName = TopicName.get("persistent://tenant/namespace/offline-timeout");
        Position lastPosition = createOfflineTopic(topicName);
        LedgerHandle cursorLedger = bkc.createLedger(BookKeeper.DigestType.CRC32, new byte[0]);
        cursorLedger.addEntry(new PositionInfo().setLedgerId(lastPosition.getLedgerId())
                .setEntryId(-1).toByteArray());
        cursorLedger.close();
        writeCursorInfo(topicName, "subscription", cursorLedger.getId(), -1, -1);
        MetaStore actualStore = factory.getMetaStore();
        MetaStore delayedStore = spy(actualStore);
        CompletableFuture<ReadHandle> dataHandle = new CompletableFuture<>();
        ManagedLedgerFactoryImpl estimatorFactory = observeReadHandle(lastPosition.getLedgerId(), dataHandle);
        BookKeeper observedBookKeeper = estimatorFactory.getBookKeeper().get(5, TimeUnit.SECONDS);
        doReturn(delayedStore).when(estimatorFactory).getMetaStore();
        CompletableFuture<Runnable> delayedCompletion = new CompletableFuture<>();
        doAnswer(invocation -> {
            MetaStoreCallback<ManagedCursorInfo> callback = invocation.getArgument(2);
            actualStore.asyncGetCursorInfo(topicName.getPersistenceNamingEncoding(), "subscription",
                    new MetaStoreCallback<>() {
                        @Override
                        public void operationComplete(ManagedCursorInfo info, Stat stat) {
                            delayedCompletion.complete(() -> callback.operationComplete(info, stat));
                        }

                        @Override
                        public void operationFailed(ManagedLedgerException.MetaStoreException error) {
                            delayedCompletion.complete(() -> callback.operationFailed(error));
                        }
                    });
            return null;
        }).when(delayedStore).asyncGetCursorInfo(eq(topicName.getPersistenceNamingEncoding()),
                eq("subscription"), any());
        PersistentOfflineTopicStats stats = new PersistentOfflineTopicStats(topicName.toString(), "broker");
        stats.totalMessages = 17;
        stats.storageSize = 19;
        stats.messageBacklog = 23;
        long generatedAt = stats.statGeneratedAt.getTime();
        CompletableFuture<Void> result = estimatorFactory.estimateUnloadedTopicBacklogAsync(stats, topicName, false,
                List.of(BookKeeper.DigestType.CRC32, new byte[0]), cachedExecutor, 1, TimeUnit.SECONDS);
        Runnable completeCursor = delayedCompletion.get(5, TimeUnit.SECONDS);
        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(TimeoutException.class);
        assertStatsUnchanged(stats, generatedAt);
        factory.getScheduledExecutor().chooseThread(topicName.getPersistenceNamingEncoding())
                .submit(completeCursor).get(5, TimeUnit.SECONDS);
        factory.getScheduledExecutor().chooseThread(topicName.getPersistenceNamingEncoding())
                .submit(() -> null).get(5, TimeUnit.SECONDS);
        verify(observedBookKeeper).newOpenLedgerOp();
        verify(dataHandle.get(5, TimeUnit.SECONDS)).closeAsync();
        assertStatsUnchanged(stats, generatedAt);
    }

    @Test(timeOut = 15000)
    public void testTimeoutDuringCursorReadClosesLateResources() throws Exception {
        TopicName topicName = TopicName.get("persistent://tenant/namespace/offline-cursor-read-timeout");
        Position lastPosition = createOfflineTopic(topicName);
        LedgerHandle cursorLedger = bkc.createLedger(BookKeeper.DigestType.CRC32, new byte[0]);
        cursorLedger.addEntry(new PositionInfo().setLedgerId(lastPosition.getLedgerId())
                .setEntryId(-1).toByteArray());
        cursorLedger.close();
        writeCursorInfo(topicName, "subscription", cursorLedger.getId(), -1, -1);
        CompletableFuture<ReadHandle> observedHandle = new CompletableFuture<>();
        ManagedLedgerFactoryImpl estimatorFactory = observeReadHandle(cursorLedger.getId(), observedHandle);
        CompletableFuture<LedgerEntries> observedEntries = new CompletableFuture<>();
        CompletableFuture<Void> releaseRead = new CompletableFuture<>();
        bkc.setReadHandleInterceptor((ledgerId, first, last, entries) -> {
            if (ledgerId != cursorLedger.getId()) {
                return CompletableFuture.completedFuture(entries);
            }
            LedgerEntries observed = spy(entries);
            observedEntries.complete(observed);
            return releaseRead.thenApply(__ -> observed);
        });
        PersistentOfflineTopicStats stats = new PersistentOfflineTopicStats(topicName.toString(), "broker");
        stats.totalMessages = 17;
        stats.storageSize = 19;
        stats.messageBacklog = 23;
        long generatedAt = stats.statGeneratedAt.getTime();
        CompletableFuture<Void> result = estimatorFactory.estimateUnloadedTopicBacklogAsync(stats, topicName, false,
                List.of(BookKeeper.DigestType.CRC32, new byte[0]), cachedExecutor, 1, TimeUnit.SECONDS);
        LedgerEntries entries = observedEntries.get(5, TimeUnit.SECONDS);
        ReadHandle handle = observedHandle.get(5, TimeUnit.SECONDS);
        try {
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(TimeoutException.class);
            verify(entries, never()).close();
            verify(handle, never()).closeAsync();
            assertStatsUnchanged(stats, generatedAt);
        } finally {
            releaseRead.complete(null);
        }

        verify(entries, timeout(5000)).close();
        verify(handle, timeout(5000)).closeAsync();
        assertStatsUnchanged(stats, generatedAt);
    }

    @DataProvider(name = "cursorLedgerFailures")
    public Object[][] cursorLedgerFailures() {
        return new Object[][] {{"open"}, {"read"}, {"parse"}};
    }

    @Test(dataProvider = "cursorLedgerFailures", timeOut = 15000)
    public void testCursorLedgerFailuresCompleteTheFuture(String failure) throws Exception {
        TopicName topicName = TopicName.get("persistent://tenant/namespace/offline-failure");
        Position lastPosition = createOfflineTopic(topicName);
        LedgerHandle cursorLedger = bkc.createLedger(BookKeeper.DigestType.CRC32, new byte[0]);
        cursorLedger.addEntry("parse".equals(failure) ? new byte[] {(byte) 0xff}
                : new PositionInfo().setLedgerId(lastPosition.getLedgerId()).setEntryId(-1).toByteArray());
        cursorLedger.close();
        writeCursorInfo(topicName, "subscription", cursorLedger.getId(), -1, -1);
        CompletableFuture<ReadHandle> observedHandle = new CompletableFuture<>();
        CompletableFuture<LedgerEntries> observedEntries = new CompletableFuture<>();
        ManagedLedgerFactoryImpl estimatorFactory = factory;
        if ("open".equals(failure)) {
            bkc.deleteLedger(cursorLedger.getId());
        } else {
            estimatorFactory = observeReadHandle(cursorLedger.getId(), observedHandle);
            bkc.setReadHandleInterceptor((ledgerId, first, last, entries) -> {
                LedgerEntries observed = spy(entries);
                observedEntries.complete(observed);
                if ("read".equals(failure)) {
                    observed.close();
                    return CompletableFuture.failedFuture(BKException.create(BKException.Code.ReadException));
                }
                return CompletableFuture.completedFuture(observed);
            });
        }
        ManagedLedgerOfflineBacklog backlog =
                new ManagedLedgerOfflineBacklog(DigestType.CRC32, new byte[0], "broker", true);
        CompletableFuture<PersistentOfflineTopicStats> result =
                backlog.estimateUnloadedTopicBacklogAsync(estimatorFactory, topicName, cachedExecutor);
        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class).cause().isNotInstanceOf(TimeoutException.class);
        if (!"open".equals(failure)) {
            verify(observedHandle.get(5, TimeUnit.SECONDS)).closeAsync();
            verify(observedEntries.get(5, TimeUnit.SECONDS)).close();
        }
        factory.getScheduledExecutor().chooseThread(topicName.getPersistenceNamingEncoding())
                .submit(() -> null).get(5, TimeUnit.SECONDS);
    }

    @Test(timeOut = 15000)
    public void testCursorFailureWaitsForOtherCursorCleanup() throws Exception {
        TopicName topicName = TopicName.get("persistent://tenant/namespace/offline-wait-for-cursors");
        Position lastPosition = createOfflineTopic(topicName);
        LedgerHandle slowCursorLedger = bkc.createLedger(BookKeeper.DigestType.CRC32, new byte[0]);
        slowCursorLedger.addEntry(new PositionInfo().setLedgerId(lastPosition.getLedgerId())
                .setEntryId(-1).toByteArray());
        slowCursorLedger.close();
        writeCursorInfo(topicName, "slow-subscription", slowCursorLedger.getId(), -1, -1);

        MetaStore actualStore = factory.getMetaStore();
        MetaStore delayedStore = spy(actualStore);
        CompletableFuture<MetaStoreCallback<ManagedCursorInfo>> failureCallback = new CompletableFuture<>();
        doAnswer(invocation -> {
            String cursorName = invocation.getArgument(1);
            MetaStoreCallback<ManagedCursorInfo> callback = invocation.getArgument(2);
            if ("subscription".equals(cursorName)) {
                failureCallback.complete(callback);
            } else {
                actualStore.asyncGetCursorInfo(invocation.getArgument(0), cursorName, callback);
            }
            return null;
        }).when(delayedStore).asyncGetCursorInfo(eq(topicName.getPersistenceNamingEncoding()), any(), any());

        CompletableFuture<ReadHandle> observedHandle = new CompletableFuture<>();
        ManagedLedgerFactoryImpl estimatorFactory = observeReadHandle(slowCursorLedger.getId(), observedHandle);
        doReturn(delayedStore).when(estimatorFactory).getMetaStore();
        CompletableFuture<LedgerEntries> observedEntries = new CompletableFuture<>();
        CompletableFuture<Void> releaseSlowRead = new CompletableFuture<>();
        bkc.setReadHandleInterceptor((ledgerId, first, last, entries) -> {
            if (ledgerId != slowCursorLedger.getId()) {
                return CompletableFuture.completedFuture(entries);
            }
            LedgerEntries observed = spy(entries);
            observedEntries.complete(observed);
            return releaseSlowRead.thenApply(__ -> observed);
        });

        ManagedLedgerOfflineBacklog backlog =
                new ManagedLedgerOfflineBacklog(DigestType.CRC32, new byte[0], "broker", false);
        CompletableFuture<PersistentOfflineTopicStats> result =
                backlog.estimateUnloadedTopicBacklogAsync(estimatorFactory, topicName, cachedExecutor);
        MetaStoreCallback<ManagedCursorInfo> callback = failureCallback.get(5, TimeUnit.SECONDS);
        LedgerEntries entries = observedEntries.get(5, TimeUnit.SECONDS);
        ReadHandle handle = observedHandle.get(5, TimeUnit.SECONDS);
        callback.operationFailed(new ManagedLedgerException.MetaStoreException("injected cursor failure"));
        try {
            assertThat(result).isNotDone();
            verify(handle, never()).closeAsync();
        } finally {
            releaseSlowRead.complete(null);
        }

        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(ManagedLedgerException.MetaStoreException.class);
        verify(entries).close();
        verify(handle).closeAsync();
    }

    @Test(dataProvider = "accuracy", timeOut = 15000)
    public void testUnknownCursorLacUsesTheBlockingExecutorOnlyForAccurateStats(boolean accurate) throws Exception {
        TopicName topicName = TopicName.get("persistent://tenant/namespace/offline-unknown-lac");
        createOfflineTopic(topicName);
        LedgerHandle cursorLedger = bkc.createLedger(BookKeeper.DigestType.CRC32, new byte[0]);
        cursorLedger.close();
        writeCursorInfo(topicName, "subscription", cursorLedger.getId(), -1, -1);
        CompletableFuture<ReadHandle> observedHandle = new CompletableFuture<>();
        ManagedLedgerFactoryImpl estimatorFactory = observeReadHandle(cursorLedger.getId(), observedHandle);
        AtomicInteger blockingTasks = new AtomicInteger();
        Executor blockingExecutor = task -> {
            blockingTasks.incrementAndGet();
            cachedExecutor.execute(task);
        };
        ManagedLedgerOfflineBacklog backlog =
                new ManagedLedgerOfflineBacklog(DigestType.CRC32, new byte[0], "broker", accurate);
        CompletableFuture<PersistentOfflineTopicStats> result =
                backlog.estimateUnloadedTopicBacklogAsync(estimatorFactory, topicName, blockingExecutor);
        if (accurate) {
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasRootCauseInstanceOf(ManagedLedgerException.class);
            assertThat(blockingTasks.get()).isEqualTo(1);
        } else {
            PersistentOfflineTopicStats stats = result.get(5, TimeUnit.SECONDS);
            assertThat(stats.totalMessages).isEqualTo(3);
            assertThat(stats.messageBacklog).isZero();
            assertThat(stats.cursorDetails.get("subscription").cursorBacklog).isEqualTo(-1);
            assertThat(blockingTasks.get()).isZero();
        }
        verify(observedHandle.get(5, TimeUnit.SECONDS)).closeAsync();
        factory.getScheduledExecutor().chooseThread(topicName.getPersistenceNamingEncoding())
                .submit(() -> null).get(5, TimeUnit.SECONDS);
    }

    @Test(dataProvider = "accuracy", timeOut = 15000)
    public void testMissingManagedLedgerReturnsZeroStats(boolean accurate) throws Exception {
        TopicName topicName = TopicName.get("persistent://tenant/namespace/offline-missing");
        ManagedLedgerOfflineBacklog backlog =
                new ManagedLedgerOfflineBacklog(DigestType.CRC32, new byte[0], "broker", accurate);

        PersistentOfflineTopicStats stats = backlog
                .estimateUnloadedTopicBacklogAsync(factory, topicName, cachedExecutor).get(5, TimeUnit.SECONDS);

        assertThat(stats.storageSize).isZero();
        assertThat(stats.totalMessages).isZero();
        assertThat(stats.messageBacklog).isZero();
        assertThat(stats.dataLedgerDetails).isEmpty();
        assertThat(stats.cursorDetails).isEmpty();
        assertThat(metadataStore.exists("/managed-ledgers/" + topicName.getPersistenceNamingEncoding())
                .get(5, TimeUnit.SECONDS)).isFalse();
    }

    @Test(dataProvider = "accuracy", timeOut = 15000)
    public void testEmptyLedgerMetadataAndCursorList(boolean accurate) throws Exception {
        TopicName emptyTopic = TopicName.get("persistent://tenant/namespace/offline-empty-ledgers");
        metadataStore.put("/managed-ledgers/" + emptyTopic.getPersistenceNamingEncoding(),
                new ManagedLedgerInfo().toByteArray(), Optional.empty()).get(5, TimeUnit.SECONDS);
        ManagedLedgerOfflineBacklog backlog =
                new ManagedLedgerOfflineBacklog(DigestType.CRC32, new byte[0], "broker", accurate);
        PersistentOfflineTopicStats emptyStats = backlog
                .estimateUnloadedTopicBacklogAsync(factory, emptyTopic, cachedExecutor).get(5, TimeUnit.SECONDS);
        assertThat(emptyStats.totalMessages).isZero();
        assertThat(emptyStats.messageBacklog).isZero();
        assertThat(emptyStats.cursorDetails).isEmpty();

        TopicName noCursorsTopic = TopicName.get("persistent://tenant/namespace/offline-empty-cursors");
        createOfflineTopic(noCursorsTopic);
        metadataStore.delete("/managed-ledgers/" + noCursorsTopic.getPersistenceNamingEncoding() + "/subscription",
                Optional.empty()).get(5, TimeUnit.SECONDS);
        PersistentOfflineTopicStats noCursorStats = backlog
                .estimateUnloadedTopicBacklogAsync(factory, noCursorsTopic, cachedExecutor).get(5, TimeUnit.SECONDS);
        assertThat(noCursorStats.totalMessages).isEqualTo(3);
        assertThat(noCursorStats.messageBacklog).isZero();
        assertThat(noCursorStats.cursorDetails).isEmpty();
    }

    @DataProvider(name = "metadataFailures")
    public Object[][] metadataFailures() {
        return new Object[][] {{OperationType.GET, false}, {OperationType.GET_CHILDREN, false},
                {OperationType.GET, true}};
    }

    @Test(dataProvider = "metadataFailures", timeOut = 15000)
    public void testMetadataFailuresCompleteTheFuture(OperationType operation, boolean cursorInfo) throws Exception {
        TopicName topicName = TopicName.get("persistent://tenant/namespace/offline-metadata-failure");
        createOfflineTopic(topicName);
        String failurePath = "/managed-ledgers/" + topicName.getPersistenceNamingEncoding()
                + (cursorInfo ? "/subscription" : "");
        metadataStore.failConditional(new MetadataStoreException("injected metadata failure"),
                (actualOperation, path) -> actualOperation == operation && path.equals(failurePath));
        ManagedLedgerOfflineBacklog backlog =
                new ManagedLedgerOfflineBacklog(DigestType.CRC32, new byte[0], "broker", true);
        CompletableFuture<PersistentOfflineTopicStats> result =
                backlog.estimateUnloadedTopicBacklogAsync(factory, topicName, cachedExecutor);
        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class).hasRootCauseInstanceOf(MetadataStoreException.class);
    }

    @SuppressWarnings("unchecked")
    private ManagedLedgerFactoryImpl observeReadHandle(long ledgerId, CompletableFuture<ReadHandle> observedHandle) {
        BookKeeper observedBookKeeper = spy(bkc);
        doAnswer(invocation -> {
            OpenBuilder builder = spy(bkc.newOpenLedgerOp());
            doAnswer(executeInvocation -> {
                CompletableFuture<ReadHandle> opened =
                        (CompletableFuture<ReadHandle>) executeInvocation.callRealMethod();
                return opened.thenApply(handle -> {
                    if (handle.getId() != ledgerId) {
                        return handle;
                    }
                    ReadHandle observed = spy(handle);
                    observedHandle.complete(observed);
                    return observed;
                });
            }).when(builder).execute();
            return builder;
        }).when(observedBookKeeper).newOpenLedgerOp();
        ManagedLedgerFactoryImpl estimatorFactory = spy(factory);
        doReturn(CompletableFuture.completedFuture(observedBookKeeper)).when(estimatorFactory).getBookKeeper();
        return estimatorFactory;
    }

    private Position createOfflineTopic(TopicName topicName) throws Exception {
        ManagedLedger ledger = factory.open(topicName.getPersistenceNamingEncoding(),
                new ManagedLedgerConfig().setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                        .setDigestType(DigestType.CRC32));
        ledger.openCursor("subscription");
        Position lastPosition = null;
        for (int i = 0; i < 3; i++) {
            lastPosition = ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }
        ledger.close();
        return lastPosition;
    }

    private void writeCursorInfo(TopicName topicName, String cursorName, long cursorLedgerId,
                                 long markDeleteLedgerId, long markDeleteEntryId) throws Exception {
        ManagedCursorInfo info = new ManagedCursorInfo().setCursorsLedgerId(cursorLedgerId)
                .setMarkDeleteLedgerId(markDeleteLedgerId).setMarkDeleteEntryId(markDeleteEntryId);
        metadataStore.put("/managed-ledgers/" + topicName.getPersistenceNamingEncoding() + "/" + cursorName,
                info.toByteArray(), Optional.empty()).get(5, TimeUnit.SECONDS);
    }

    private void assertStatsUnchanged(PersistentOfflineTopicStats stats, long generatedAt) {
        assertThat(stats.totalMessages).isEqualTo(17);
        assertThat(stats.storageSize).isEqualTo(19);
        assertThat(stats.messageBacklog).isEqualTo(23);
        assertThat(stats.cursorDetails).isEmpty();
        assertThat(stats.dataLedgerDetails).isEmpty();
        assertThat(stats.statGeneratedAt.getTime()).isEqualTo(generatedAt);
    }
}
