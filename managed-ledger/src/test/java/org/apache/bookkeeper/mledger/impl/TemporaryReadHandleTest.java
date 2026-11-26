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
import static org.mockito.ArgumentMatchers.anyBoolean;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.MessageRange;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.NestedPositionInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(timeOut = 30000)
public class TemporaryReadHandleTest extends MockedBookKeeperTestCase {
    private final List<ReadHandle> openedHandles = new CopyOnWriteArrayList<>();
    private Consumer<ReadHandle> configureHandle;

    @Override
    protected void setUpTestCase() throws Exception {
        openedHandles.clear();
        configureHandle = handle -> { };
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        PulsarMockBookKeeper realBkc = bkc;
        bkc = spy(bkc);
        doAnswer(invocation -> trackOpens((OpenBuilder) invocation.callRealMethod()))
                .when(bkc).newOpenLedgerOp();
        // Read-write managed ledgers and cursors open their ledgers through the callback API on this branch.
        doAnswer(invocation -> {
            AsyncCallback.OpenCallback callback = invocation.getArgument(3);
            AsyncCallback.OpenCallback trackingCallback = (rc, handle, ctx) -> {
                if (rc != BKException.Code.OK) {
                    callback.openComplete(rc, handle, ctx);
                    return;
                }
                LedgerHandle trackedHandle = spy(handle);
                configureHandle.accept(trackedHandle);
                openedHandles.add(trackedHandle);
                callback.openComplete(rc, trackedHandle, ctx);
            };
            realBkc.asyncOpenLedger(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2),
                    trackingCallback, invocation.getArgument(4), invocation.getArgument(5));
            return null;
        }).when(bkc).asyncOpenLedger(anyLong(), any(), any(), any(), any(), anyBoolean());
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
    }

    @SuppressWarnings("unchecked")
    private OpenBuilder trackOpens(OpenBuilder builder) {
        OpenBuilder trackedBuilder = spy(builder);
        doAnswer(invocation -> ((CompletableFuture<ReadHandle>) invocation.callRealMethod()).thenApply(handle -> {
            ReadHandle trackedHandle = spy(handle);
            configureHandle.accept(trackedHandle);
            openedHandles.add(trackedHandle);
            return trackedHandle;
        })).when(trackedBuilder).execute();
        return trackedBuilder;
    }

    private ManagedLedgerConfig retainedConfig() {
        return new ManagedLedgerConfig().setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1);
    }

    private void assertClosed(ReadHandle handle) {
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(handle).closeAsync());
    }

    // Managed ledgers and cursors open LedgerHandles; only the read-only builder path returns a plain ReadHandle.
    private static Consumer<ReadHandle> asLedgerHandle(Consumer<LedgerHandle> configure) {
        return handle -> configure.accept((LedgerHandle) handle);
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
        LedgerHandle handle = (LedgerHandle) openedHandles.get(0);
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
        ReadHandle handle = openedHandles.get(0);
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

    @DataProvider(name = "synchronousLacFailures")
    public Object[][] synchronousLacFailures() {
        return new Object[][] {
                {new IllegalStateException("LAC read failed"), false},
                {new IllegalArgumentException("invalid LAC read"), false},
                {new ArrayIndexOutOfBoundsException("empty ledger"), true}
        };
    }

    @Test(dataProvider = "synchronousLacFailures")
    public void testReadOnlyInitializationClosesHandleWhenLacReadThrows(RuntimeException failure,
                                                                      boolean emptyLedger) throws Exception {
        ManagedLedgerConfig config = retainedConfig();
        ManagedLedger ledger = factory.open("readonly-lac-throws", config);
        ledger.addEntry(new byte[] {1});
        configureHandle = handle -> doThrow(failure).when(handle).readLastAddConfirmedAsync();
        ReadOnlyManagedLedgerImpl readOnly = new ReadOnlyManagedLedgerImpl(factory, bkc, factory.getMetaStore(),
                config, executor, ledger.getName());

        try {
            CompletableFuture<Void> opening = readOnly.initialize();
            if (emptyLedger) {
                opening.get(5, TimeUnit.SECONDS);
                assertThat(readOnly.getLedgersInfo().lastEntry().getValue().getEntries()).isZero();
                assertThat(readOnly.getLastConfirmedEntry()).isEqualTo(PositionFactory.EARLIEST);
            } else {
                assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS)).rootCause().isSameAs(failure);
            }
        } finally {
            readOnly.close();
        }
        assertThat(openedHandles).hasSize(1);
        assertClosed(openedHandles.get(0));
        assertThat(readOnly.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
    }

    @DataProvider(name = "cursorRecoveryResults")
    public Object[][] cursorRecoveryResults() {
        return new Object[][] {{"empty"}, {"read-failure"}, {"rollback"}, {"parse-failure"},
                {"invalid-range-failure"}, {"cursor-state-failure"}, {"success"}, {"callback-throws"}};
    }

    @Test(dataProvider = "cursorRecoveryResults")
    public void testCursorRecoveryClosesOnlyUnclaimedHandle(String outcome) throws Exception {
        ManagedLedgerConfig config = retainedConfig();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cursor-recovery-close", config);
        ManagedCursorImpl cursor;
        if (outcome.equals("cursor-state-failure")) {
            ManagedLedgerImpl failingLedger = spy(ledger);
            cursor = new ManagedCursorImpl(bkc, failingLedger, "cursor");
            doThrow(new IllegalStateException("cursor position update failed"))
                    .when(failingLedger).onCursorReadPositionUpdated(eq(cursor), any(Position.class));
        } else {
            cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        }
        Position position = ledger.addEntry(new byte[] {1});
        LedgerHandle cursorLedger = bkc.createLedger(3, 2, 2,
                BookKeeper.DigestType.fromApiDigestType(config.getDigestType()), config.getPassword());
        PositionInfo.Builder snapshot = PositionInfo.newBuilder().setLedgerId(position.getLedgerId())
                .setEntryId(position.getEntryId());
        if (outcome.equals("invalid-range-failure")) {
            // This snapshot parses successfully, but restoring the reversed range fails in subMap().
            snapshot.addIndividualDeletedMessages(MessageRange.newBuilder()
                    .setLowerEndpoint(NestedPositionInfo.newBuilder().setLedgerId(2).setEntryId(-1))
                    .setUpperEndpoint(NestedPositionInfo.newBuilder().setLedgerId(1).setEntryId(0)));
        }
        byte[] data = outcome.equals("parse-failure") ? new byte[] {(byte) 0xff} : snapshot.build().toByteArray();
        if (!outcome.equals("empty")) {
            cursorLedger.addEntry(data);
        }
        cursorLedger.close();
        CompletableFuture<Void> readCallbackFinished = new CompletableFuture<>();
        if (outcome.equals("read-failure") || outcome.equals("rollback")) {
            configureHandle = asLedgerHandle(handle -> doAnswer(invocation -> {
                AsyncCallback.ReadCallback callback = invocation.getArgument(2);
                int rc = outcome.equals("rollback")
                        ? BKException.Code.ReadException : BKException.Code.TimeoutException;
                callback.readComplete(rc, handle, null, invocation.getArgument(3));
                return null;
            }).when(handle).asyncReadEntries(anyLong(), anyLong(), any(), any()));
        } else if (outcome.equals("callback-throws")) {
            configureHandle = asLedgerHandle(handle -> doAnswer(invocation -> {
                AsyncCallback.ReadCallback callback = invocation.getArgument(2);
                LedgerEntry entry = mock(LedgerEntry.class);
                when(entry.getEntry()).thenReturn(data);
                when(entry.getLength()).thenReturn((long) data.length);
                try {
                    callback.readComplete(BKException.Code.OK, handle, Collections.enumeration(List.of(entry)),
                            invocation.getArgument(3));
                } finally {
                    readCallbackFinished.complete(null);
                }
                return null;
            }).when(handle).asyncReadEntries(anyLong(), anyLong(), any(), any()));
        }
        ManagedCursorInfo info = ManagedCursorInfo.newBuilder().setCursorsLedgerId(cursorLedger.getId())
                .setMarkDeleteLedgerId(position.getLedgerId()).setMarkDeleteEntryId(-1).build();
        CompletableFuture<Void> recovered = new CompletableFuture<>();
        AtomicInteger completedCallbacks = new AtomicInteger();
        AtomicInteger failedCallbacks = new AtomicInteger();
        cursor.recoverFromLedger(info, new ManagedCursorImpl.VoidCallback() {
            @Override
            public void operationComplete() {
                completedCallbacks.incrementAndGet();
                recovered.complete(null);
                if (outcome.equals("callback-throws")) {
                    throw new IllegalStateException("recovery callback failed");
                }
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                failedCallbacks.incrementAndGet();
                recovered.completeExceptionally(exception);
            }
        });
        boolean failure = outcome.endsWith("failure");
        if (failure) {
            var assertion = assertThatThrownBy(() -> recovered.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class);
            if (outcome.equals("invalid-range-failure")) {
                assertion.hasRootCauseInstanceOf(IllegalArgumentException.class);
            } else if (outcome.equals("cursor-state-failure")) {
                assertion.hasRootCauseMessage("cursor position update failed");
            }
        } else {
            recovered.get(5, TimeUnit.SECONDS);
        }
        if (outcome.equals("callback-throws")) {
            readCallbackFinished.get(5, TimeUnit.SECONDS);
        }
        assertThat(completedCallbacks.get()).isEqualTo(failure ? 0 : 1);
        assertThat(failedCallbacks.get()).isEqualTo(failure ? 1 : 0);
        assertThat(openedHandles).hasSize(1);
        LedgerHandle handle = (LedgerHandle) openedHandles.get(0);
        if (outcome.equals("success") || outcome.equals("callback-throws")) {
            assertThat(cursor.getCursorLedger()).isEqualTo(handle.getId());
            verify(handle, never()).closeAsync();
            cursor.close();
            assertClosed(handle);
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(bkc.getLedgers()).doesNotContain(handle.getId()));
        } else {
            assertThat(cursor.getCursorLedger()).isNotEqualTo(handle.getId());
            assertClosed(handle);
        }
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
