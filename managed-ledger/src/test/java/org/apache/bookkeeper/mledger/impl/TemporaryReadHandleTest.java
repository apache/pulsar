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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(timeOut = 30000)
public class TemporaryReadHandleTest extends MockedBookKeeperTestCase {
    private final List<LedgerHandle> openedHandles = new CopyOnWriteArrayList<>();
    private Consumer<LedgerHandle> configureHandle;

    @Override
    protected void setUpTestCase() throws Exception {
        openedHandles.clear();
        configureHandle = handle -> { };
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        bkc = spy(bkc);
        doAnswer(invocation -> trackOpens((OpenBuilder) invocation.callRealMethod()))
                .when(bkc).newOpenLedgerOp();
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
    }

    @SuppressWarnings("unchecked")
    private OpenBuilder trackOpens(OpenBuilder builder) {
        OpenBuilder trackedBuilder = spy(builder);
        doAnswer(invocation -> ((CompletableFuture<ReadHandle>) invocation.callRealMethod()).thenApply(handle -> {
            LedgerHandle trackedHandle = spy((LedgerHandle) handle);
            configureHandle.accept(trackedHandle);
            openedHandles.add(trackedHandle);
            return trackedHandle;
        })).when(trackedBuilder).execute();
        return trackedBuilder;
    }

    private ManagedLedgerConfig retainedConfig() {
        return defaultConfig().setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1);
    }

    private void assertClosed(LedgerHandle handle) {
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(handle).closeAsync());
    }

    @Test
    public void testInitializationClosesTemporaryHandle() throws Exception {
        ManagedLedgerConfig config = retainedConfig();
        ManagedLedger ledger = factory.open("initialization-close", config);
        ledger.addEntry(new byte[] {1});
        ledger.close();

        ManagedLedger reopened = factory.open("initialization-close", config);
        assertThat(openedHandles).hasSize(1);
        assertClosed(openedHandles.get(0));
        reopened.close();
    }

    @DataProvider(name = "completionResults")
    public Object[][] completionResults() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "completionResults")
    public void testInitializationWaitsForInterceptorBeforeClosing(boolean fail) throws Exception {
        ManagedLedgerConfig config = retainedConfig();
        ManagedLedger ledger = factory.open("interceptor-close", config);
        ledger.addEntry(new byte[] {1});
        ledger.close();

        CompletableFuture<Void> interceptorResult = new CompletableFuture<>();
        CompletableFuture<Void> interceptorStarted = new CompletableFuture<>();
        ManagedLedgerInterceptor interceptor = mock(ManagedLedgerInterceptor.class);
        when(interceptor.onManagedLedgerLastLedgerInitialize(eq("interceptor-close"), any()))
                .thenAnswer(invocation -> {
                    interceptorStarted.complete(null);
                    return interceptorResult;
                });
        config.setManagedLedgerInterceptor(interceptor);
        CompletableFuture<ManagedLedger> opening = openAsync("interceptor-close", config);
        interceptorStarted.get(5, TimeUnit.SECONDS);
        LedgerHandle handle = openedHandles.get(0);
        verify(handle, never()).closeAsync();

        if (fail) {
            interceptorResult.completeExceptionally(new IllegalStateException("interceptor failed"));
            assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS)).hasRootCauseMessage("interceptor failed");
        } else {
            interceptorResult.complete(null);
            opening.get(5, TimeUnit.SECONDS);
        }
        assertClosed(handle);
    }

    @Test
    public void testInitializationClosesHandleWhenInterceptorThrows() throws Exception {
        ManagedLedgerConfig config = retainedConfig();
        ManagedLedger ledger = factory.open("interceptor-throws", config);
        ledger.addEntry(new byte[] {1});
        ledger.close();
        ManagedLedgerInterceptor interceptor = mock(ManagedLedgerInterceptor.class);
        when(interceptor.onManagedLedgerLastLedgerInitialize(eq(ledger.getName()), any()))
                .thenThrow(new IllegalStateException("interceptor failed"));
        config.setManagedLedgerInterceptor(interceptor);

        assertThatThrownBy(() -> openAsync(ledger.getName(), config).get(5, TimeUnit.SECONDS))
                .hasRootCauseMessage("interceptor failed");
        assertThat(openedHandles).hasSize(1);
        assertClosed(openedHandles.get(0));
    }

    @Test(dataProvider = "completionResults")
    public void testTemporaryHandleCloseFailureDoesNotFailInitialization(boolean synchronous) throws Exception {
        ManagedLedgerConfig config = retainedConfig();
        ManagedLedger ledger = factory.open("temporary-close-failure", config);
        ledger.addEntry(new byte[] {1});
        ledger.close();
        configureHandle = handle -> {
            if (synchronous) {
                doThrow(new IllegalStateException("close failed")).when(handle).closeAsync();
            } else {
                doReturn(CompletableFuture.failedFuture(new BKException.BKReadException()))
                        .when(handle).closeAsync();
            }
        };

        ManagedLedger reopened = factory.open(ledger.getName(), config);
        assertThat(openedHandles).hasSize(1);
        assertClosed(openedHandles.get(0));
        reopened.close();
    }

    @Test
    public void testTerminatedLedgerRetainsItsReadHandle() throws Exception {
        ManagedLedgerConfig config = retainedConfig();
        ManagedLedger ledger = factory.open("terminated-handle", config);
        ledger.addEntry(new byte[] {1});
        ledger.terminate();
        ledger.close();

        ManagedLedgerImpl reopened = (ManagedLedgerImpl) factory.open("terminated-handle", config);
        assertThat(openedHandles).hasSize(1);
        LedgerHandle handle = openedHandles.get(0);
        assertThat(reopened.currentLedger).isSameAs(handle);
        verify(handle, never()).closeAsync();
        verify(handle, never()).asyncClose(any(), any());
        reopened.close();
        verify(handle).asyncClose(any(), any());
    }

    @Test(dataProvider = "completionResults")
    public void testReadOnlyInitializationClosesHandleAfterLacRead(boolean fail) throws Exception {
        ManagedLedgerConfig config = retainedConfig();
        ManagedLedger ledger = factory.open("readonly-initialization", config);
        Position position = ledger.addEntry(new byte[] {1});
        CompletableFuture<Long> lac = new CompletableFuture<>();
        configureHandle = handle -> doReturn(lac).when(handle).readLastAddConfirmedAsync();
        ReadOnlyManagedLedgerImpl readOnly = new ReadOnlyManagedLedgerImpl(factory, bkc, factory.getMetaStore(),
                config, executor, ledger.getName());
        CompletableFuture<Void> opening = readOnly.initialize();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> openedHandles.size() == 1);
        LedgerHandle handle = openedHandles.get(0);
        verify(handle, never()).closeAsync();
        if (fail) {
            lac.completeExceptionally(new BKException.BKReadException());
            assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(BKException.BKReadException.class);
        } else {
            lac.complete(position.getEntryId());
            opening.get(5, TimeUnit.SECONDS);
        }
        assertClosed(handle);
        readOnly.close();
    }

    @Test
    public void testReadOnlyInitializationClosesHandleWhenLacReadThrows() throws Exception {
        ManagedLedgerConfig config = retainedConfig();
        ManagedLedger ledger = factory.open("readonly-lac-throws", config);
        ledger.addEntry(new byte[] {1});
        configureHandle = handle -> doThrow(new IllegalStateException("LAC read failed"))
                .when(handle).readLastAddConfirmedAsync();
        ReadOnlyManagedLedgerImpl readOnly = new ReadOnlyManagedLedgerImpl(factory, bkc, factory.getMetaStore(),
                config, executor, ledger.getName());

        assertThatThrownBy(() -> readOnly.initialize().get(5, TimeUnit.SECONDS))
                .hasRootCauseMessage("LAC read failed");
        assertThat(openedHandles).hasSize(1);
        assertClosed(openedHandles.get(0));
        readOnly.close();
    }

    @Test
    public void testLacRecheckClosesTemporaryHandle() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("lac-recheck", retainedConfig());
        ledger.addEntry(new byte[] {1});
        LedgerHandle writer = ledger.currentLedger;
        ledger.addEntryFailedDueToConcurrentlyModified(writer, BKException.Code.MetadataVersionException);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> openedHandles.size() == 1);
        assertThat(openedHandles.get(0)).isNotSameAs(writer);
        assertClosed(openedHandles.get(0));
    }

    @Test
    public void testOfflineLedgerStatisticsCloseTemporaryHandle() throws Exception {
        TopicName topic = TopicName.get("persistent://public/default/offline-handles");
        ManagedLedgerConfig config = retainedConfig();
        ManagedLedger ledger = factory.open(topic.getPersistenceNamingEncoding(), config);
        ledger.addEntry(new byte[] {1});
        ledger.close();

        factory.estimateUnloadedTopicBacklog(new PersistentOfflineTopicStats(topic.toString(), "broker"), topic,
                true, List.of(BookKeeper.DigestType.fromApiDigestType(config.getDigestType()), config.getPassword()));
        assertThat(openedHandles).hasSize(1);
        assertClosed(openedHandles.get(0));
    }

    @DataProvider(name = "cursorRecoveryResults")
    public Object[][] cursorRecoveryResults() {
        return new Object[][] {{"empty"}, {"read-failure"}, {"rollback"}, {"parse-failure"}, {"success"}};
    }

    @Test(dataProvider = "cursorRecoveryResults")
    public void testCursorRecoveryClosesOnlyUnclaimedHandle(String outcome) throws Exception {
        ManagedLedgerConfig config = retainedConfig();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cursor-recovery-close", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position position = ledger.addEntry(new byte[] {1});
        LedgerHandle cursorLedger = bkc.createLedger(3, 2, 2,
                BookKeeper.DigestType.fromApiDigestType(config.getDigestType()), config.getPassword());
        if (!outcome.equals("empty")) {
            byte[] data = outcome.equals("parse-failure") ? new byte[] {(byte) 0xff}
                    : new PositionInfo().setLedgerId(position.getLedgerId()).setEntryId(position.getEntryId())
                            .toByteArray();
            cursorLedger.addEntry(data);
        }
        cursorLedger.close();
        if (outcome.equals("read-failure") || outcome.equals("rollback")) {
            configureHandle = handle -> doAnswer(invocation -> {
                AsyncCallback.ReadCallback callback = invocation.getArgument(2);
                int rc = outcome.equals("rollback")
                        ? BKException.Code.ReadException : BKException.Code.TimeoutException;
                callback.readComplete(rc, handle, null, invocation.getArgument(3));
                return null;
            }).when(handle).asyncReadEntries(anyLong(), anyLong(), any(), any());
        }
        ManagedCursorInfo info = new ManagedCursorInfo().setCursorsLedgerId(cursorLedger.getId())
                .setMarkDeleteLedgerId(position.getLedgerId()).setMarkDeleteEntryId(-1);
        CompletableFuture<Void> recovered = new CompletableFuture<>();
        cursor.recoverFromLedger(info, new ManagedCursorImpl.VoidCallback() {
            @Override
            public void operationComplete() {
                recovered.complete(null);
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                recovered.completeExceptionally(exception);
            }
        });
        if (outcome.endsWith("failure")) {
            assertThatThrownBy(() -> recovered.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class);
        } else {
            recovered.get(5, TimeUnit.SECONDS);
        }
        assertThat(openedHandles).hasSize(1);
        LedgerHandle handle = openedHandles.get(0);
        if (outcome.equals("success")) {
            assertThat(cursor.getCursorLedger()).isEqualTo(handle.getId());
            verify(handle, never()).closeAsync();
            cursor.close();
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(bkc.getLedgers()).doesNotContain(handle.getId()));
        } else {
            assertClosed(handle);
        }
    }

    @DataProvider(name = "offlineCursorResults")
    public Object[][] offlineCursorResults() {
        return new Object[][] {{"success"}, {"read-failure"}, {"parse-failure"}, {"empty"}};
    }

    @Test(dataProvider = "offlineCursorResults")
    public void testOfflineCursorStatisticsCloseTemporaryHandle(String outcome) throws Exception {
        TopicName topic = TopicName.get("persistent://public/default/offline-cursor-handles");
        ManagedLedgerConfig config = retainedConfig().setMaxUnackedRangesToPersistInMetadataStore(0);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(topic.getPersistenceNamingEncoding(), config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        ledger.addEntry(new byte[] {1});
        Position second = ledger.addEntry(new byte[] {2});
        cursor.delete(second);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> cursor.getStats().getPersistLedgerSucceed() > 0);
        long cursorLedgerId = cursor.getCursorLedger();
        assertThat(cursorLedgerId).isNotEqualTo(-1);
        // The backlog estimator blocks inside getCursors' callback. Do not run that callback on the
        // same ordered executor that must deliver asyncGetCursorInfo's result.
        MetaStore metaStore = spy(factory.getMetaStore());
        doAnswer(invocation -> {
            MetaStore.MetaStoreCallback<List<String>> callback = invocation.getArgument(1);
            cachedExecutor.execute(() -> callback.operationComplete(List.of(cursor.getName()), null));
            return null;
        }).when(metaStore).getCursors(eq(ledger.getName()), any());
        factory = spy(factory);
        doReturn(metaStore).when(factory).getMetaStore();
        configureHandle = handle -> {
            if (handle.getId() == cursorLedgerId) {
                if (outcome.equals("empty")) {
                    doReturn(LedgerHandle.INVALID_ENTRY_ID).when(handle).getLastAddConfirmed();
                } else if (!outcome.equals("success")) {
                    doAnswer(invocation -> {
                        AsyncCallback.ReadCallback callback = invocation.getArgument(2);
                        if (outcome.equals("read-failure")) {
                            callback.readComplete(BKException.Code.ReadException, handle, null,
                                    invocation.getArgument(3));
                        } else {
                            LedgerEntry entry = mock(LedgerEntry.class);
                            when(entry.getEntry()).thenReturn(new byte[] {(byte) 0xff});
                            callback.readComplete(BKException.Code.OK, handle, Collections.enumeration(List.of(entry)),
                                    invocation.getArgument(3));
                        }
                        return null;
                    }).when(handle).asyncReadEntries(anyLong(), anyLong(), any(), any());
                }
            }
        };
        factory.estimateUnloadedTopicBacklog(new PersistentOfflineTopicStats(topic.toString(), "broker"), topic,
                !outcome.equals("empty"),
                List.of(BookKeeper.DigestType.fromApiDigestType(config.getDigestType()), config.getPassword()));
        LedgerHandle handle = openedHandles.stream().filter(h -> h.getId() == cursorLedgerId).findFirst().orElseThrow();
        assertClosed(handle);
    }

    private CompletableFuture<ManagedLedger> openAsync(String name, ManagedLedgerConfig config) {
        CompletableFuture<ManagedLedger> result = new CompletableFuture<>();
        factory.asyncOpen(name, config, new AsyncCallbacks.OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                result.complete(ledger);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null, null);
        return result;
    }
}
