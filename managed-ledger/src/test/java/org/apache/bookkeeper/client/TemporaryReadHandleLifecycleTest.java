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
package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(timeOut = 30000)
public class TemporaryReadHandleLifecycleTest extends MockedBookKeeperTestCase {
    private final Set<LedgerMetadataListener> activeListeners =
            Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
    private final List<LedgerHandle> openedHandles = new CopyOnWriteArrayList<>();
    private LedgerManager ledgerManager;
    private volatile long delayedReadLedgerId = -1;
    private volatile boolean failDelayedRead;
    private CompletableFuture<Runnable> delayedRead;

    @Override
    protected void setUpTestCase() throws Exception {
        activeListeners.clear();
        openedHandles.clear();
        delayedReadLedgerId = -1;
        failDelayedRead = false;
        delayedRead = new CompletableFuture<>();
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        ledgerManager = mock(LedgerManager.class);
        doAnswer(invocation -> activeListeners.add(invocation.getArgument(1)))
                .when(ledgerManager).registerLedgerMetadataListener(anyLong(), any());
        doAnswer(invocation -> activeListeners.remove(invocation.getArgument(1)))
                .when(ledgerManager).unregisterLedgerMetadataListener(anyLong(), any());
        ClientContext context = spy(bkc.getClientCtx());
        doReturn(ledgerManager).when(context).getLedgerManager();
        bkc = spy(bkc);
        doAnswer(invocation -> trackOpens((OpenBuilder) invocation.callRealMethod(), context))
                .when(bkc).newOpenLedgerOp();
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
    }

    @SuppressWarnings("unchecked")
    private OpenBuilder trackOpens(OpenBuilder builder, ClientContext context) {
        OpenBuilder trackedBuilder = spy(builder);
        AtomicBoolean keepUpdateMetadata = new AtomicBoolean();
        doAnswer(invocation -> {
            keepUpdateMetadata.set(invocation.getArgument(0));
            return invocation.callRealMethod();
        }).when(trackedBuilder).withKeepUpdateMetadata(anyBoolean());
        doAnswer(invocation -> ((CompletableFuture<ReadHandle>) invocation.callRealMethod()).thenApply(handle -> {
            try {
                // Keep BookKeeper's real registration and close paths; only the metadata manager is mocked.
                LedgerHandle readHandle = new ReadOnlyLedgerHandle(context, handle.getId(),
                        new Versioned<>(handle.getLedgerMetadata(), new LongVersion(0)),
                        BookKeeper.DigestType.fromApiDigestType(handle.getLedgerMetadata().getDigestType()),
                        handle.getLedgerMetadata().getPassword(), keepUpdateMetadata.get()) {
                    @Override
                    public void asyncReadEntries(long firstEntry, long lastEntry, AsyncCallback.ReadCallback callback,
                                                 Object ctx) {
                        // Use the in-memory ledger's entries while retaining the real handle lifecycle.
                        Runnable read = () -> {
                            if (getId() == delayedReadLedgerId && failDelayedRead) {
                                callback.readComplete(BKException.Code.TimeoutException, this, null, ctx);
                            } else {
                                ((LedgerHandle) handle).asyncReadEntries(firstEntry, lastEntry,
                                        (rc, originalHandle, entries, readCtx) ->
                                                callback.readComplete(rc, this, entries, readCtx),
                                        ctx);
                            }
                        };
                        if (getId() != delayedReadLedgerId || !delayedRead.complete(read)) {
                            read.run();
                        }
                    }
                };
                openedHandles.add(readHandle);
                return readHandle;
            } catch (GeneralSecurityException e) {
                throw new CompletionException(e);
            }
        })).when(trackedBuilder).execute();
        return trackedBuilder;
    }

    @DataProvider(name = "terminatedLedgers")
    public Object[][] terminatedLedgers() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "terminatedLedgers")
    public void testInitializationReleasesListenerAtHandleOwnershipEnd(boolean terminated) throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1);
        ManagedLedger ledger = factory.open("initialization-listener", config);
        ledger.addEntry(new byte[] {1});
        if (terminated) {
            ledger.terminate();
        }
        ledger.close();

        ManagedLedger reopened = factory.open(ledger.getName(), config);
        assertThat(openedHandles).hasSize(1);
        LedgerHandle handle = openedHandles.get(0);
        LedgerMetadataListener listener = (LedgerMetadataListener) handle;
        verify(ledgerManager).registerLedgerMetadataListener(eq(handle.getId()), same(listener));
        if (terminated) {
            assertThat(activeListeners).containsExactly(listener);
            verify(ledgerManager, never()).unregisterLedgerMetadataListener(eq(handle.getId()), same(listener));
        } else {
            assertListenerReleased(handle, listener);
        }
        reopened.close();
        assertListenerReleased(handle, listener);
    }

    @DataProvider(name = "cursorOwnershipEnds")
    public Object[][] cursorOwnershipEnds() {
        return new Object[][] {{"close", false}, {"close", true}, {"switch", false}, {"switch", true},
                {"delete", false}, {"delete", true}};
    }

    @Test(dataProvider = "cursorOwnershipEnds")
    public void testCursorReleasesListenerBeforeLedgerDeletion(String action, boolean failDelete)
            throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1)
                .setMaxUnackedRangesToPersistInMetadataStore(0);
        ManagedLedger ledger = factory.open("cursor-listener", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position first = ledger.addEntry(new byte[] {1});
        Position second = ledger.addEntry(new byte[] {2});
        cursor.delete(second);
        long cursorLedgerId = cursor.getCursorLedger();
        assertThat(cursorLedgerId).isNotEqualTo(-1);
        ledger.close();

        ManagedLedger reopened = factory.open(ledger.getName(), config);
        ManagedCursorImpl recovered = (ManagedCursorImpl) reopened.openCursor("cursor");
        LedgerHandle handle = openedHandles.stream().filter(h -> h.getId() == cursorLedgerId).findFirst().orElseThrow();
        LedgerMetadataListener listener = (LedgerMetadataListener) handle;
        try {
            assertThat(activeListeners).containsExactly(listener);
            verify(ledgerManager).registerLedgerMetadataListener(eq(cursorLedgerId), same(listener));
            verify(ledgerManager, never()).unregisterLedgerMetadataListener(eq(cursorLedgerId), same(listener));
            CompletableFuture<Void> deleteStarted = new CompletableFuture<>();
            doAnswer(invocation -> {
                if (failDelete) {
                    AsyncCallback.DeleteCallback callback = invocation.getArgument(1);
                    callback.deleteComplete(BKException.Code.UnexpectedConditionException, invocation.getArgument(2));
                }
                // Otherwise leave deletion pending: handle cleanup must not wait for its callback.
                deleteStarted.complete(null);
                return null;
            }).when(bkc).asyncDeleteLedger(eq(cursorLedgerId), any(), any());

            if (action.equals("switch")) {
                recovered.markDelete(first);
                assertThat(recovered.getCursorLedger()).isNotEqualTo(cursorLedgerId).isNotEqualTo(-1);
                Position third = reopened.addEntry(new byte[] {3});
                recovered.markDelete(third);
                assertThat(recovered.getMarkDeletedPosition()).isEqualTo(third);
            } else if (action.equals("delete")) {
                reopened.deleteCursor("cursor");
            } else {
                recovered.close();
            }
            deleteStarted.get(5, TimeUnit.SECONDS);
            assertListenerReleased(handle, listener);
            assertThat(bkc.getLedgers()).contains(cursorLedgerId);
        } finally {
            try {
                reopened.close();
            } finally {
                handle.close();
            }
        }
    }

    @DataProvider(name = "closeMetadataWriteFailures")
    public Object[][] closeMetadataWriteFailures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "closeMetadataWriteFailures")
    public void testCursorCloseReleasesRecoveredListenerOnMetadataFailure(boolean failWrite) throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1)
                .setMaxUnackedRangesToPersistInMetadataStore(0);
        ManagedLedger ledger = factory.open("cursor-close-metadata-failure-listener", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        ledger.addEntry(new byte[] {1});
        Position second = ledger.addEntry(new byte[] {2});
        cursor.delete(second);
        long cursorLedgerId = cursor.getCursorLedger();
        assertThat(cursorLedgerId).isNotEqualTo(-1);
        ledger.close();

        ManagedLedger reopened = factory.open(ledger.getName(), config);
        ManagedCursorImpl recovered = (ManagedCursorImpl) reopened.openCursor("cursor");
        LedgerHandle handle = openedHandles.stream().filter(h -> h.getId() == cursorLedgerId).findFirst().orElseThrow();
        LedgerMetadataListener listener = (LedgerMetadataListener) handle;
        AtomicInteger injectedFailures = new AtomicInteger();
        try {
            assertThat(activeListeners).containsExactly(listener);
            verify(ledgerManager).registerLedgerMetadataListener(eq(cursorLedgerId), same(listener));
            if (failWrite) {
                metadataStore.failConditional(new MetadataStoreException("injected close metadata failure"),
                        (operation, path) -> {
                            if (operation == FaultInjectionMetadataStore.OperationType.PUT
                                    && path.equals("/managed-ledgers/" + ledger.getName() + "/cursor")) {
                                injectedFailures.incrementAndGet();
                                return true;
                            }
                            return false;
                        });
                assertThatThrownBy(recovered::close).isInstanceOf(ManagedLedgerException.class);
                assertThat(injectedFailures).hasValue(1);
                assertThat(recovered.getState()).isEqualTo("Closing");
            } else {
                recovered.close();
                assertThat(injectedFailures).hasValue(0);
                assertThat(recovered.getState()).isEqualTo("Closed");
            }
            assertListenerReleased(handle, listener);
            // Retry the public close APIs after the one injected failure has been consumed.
            recovered.close();
            reopened.close();
            if (failWrite) {
                assertThat(injectedFailures).hasValue(1);
                assertThat(recovered.getState()).isEqualTo("Closing");
                assertThat(bkc.getLedgers()).contains(cursorLedgerId);
                verify(bkc, never()).asyncDeleteLedger(eq(cursorLedgerId), any(), any());
            }
            assertListenerReleased(handle, listener);
            if (failWrite) {
                // Closing the local read handle must preserve the persisted position for the next owner.
                ManagedLedger recoveredAgain = factory.open(ledger.getName(), config);
                try {
                    ManagedCursor nextCursor = recoveredAgain.openCursor("cursor");
                    assertThat(nextCursor.getMarkDeletedPosition()).isEqualTo(recovered.getMarkDeletedPosition());
                    assertThat(nextCursor.isMessageDeleted(second)).isTrue();
                } finally {
                    recoveredAgain.close();
                }
                assertThat(injectedFailures).hasValue(1);
            }
        } finally {
            try {
                reopened.close();
            } finally {
                handle.close();
            }
        }
    }

    @DataProvider(name = "cursorRecoveryCloseOrdering")
    public Object[][] cursorRecoveryCloseOrdering() {
        return new Object[][] {{false, false, false}, {true, false, false}, {false, true, false},
                {true, true, false}, {true, false, true}};
    }

    @Test(dataProvider = "cursorRecoveryCloseOrdering")
    public void testLazyRecoveryReleasesListenerAfterManagedLedgerClose(boolean closeBeforeRecovery, boolean failRead,
                                                                       boolean failLedgerClose) throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1)
                .setMaxUnackedRangesToPersistInMetadataStore(0);
        ManagedLedger ledger = factory.open("lazy-cursor-close-listener", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        ledger.addEntry(new byte[] {1});
        Position second = ledger.addEntry(new byte[] {2});
        cursor.delete(second);
        long cursorLedgerId = cursor.getCursorLedger();
        assertThat(cursorLedgerId).isNotEqualTo(-1);
        ledger.close();

        delayedReadLedgerId = cursorLedgerId;
        failDelayedRead = failRead;
        ManagedLedgerImpl reopened = (ManagedLedgerImpl) factory.open(ledger.getName(),
                config.setLazyCursorRecovery(true));
        Runnable resumeRead = delayedRead.get(5, TimeUnit.SECONDS);
        LedgerHandle handle = openedHandles.stream().filter(h -> h.getId() == cursorLedgerId).findFirst().orElseThrow();
        LedgerMetadataListener listener = (LedgerMetadataListener) handle;
        CompletableFuture<ManagedCursor> recovery = new CompletableFuture<>();
        // Observe the pending recovery through the public API, without accessing the internal cursor map.
        reopened.asyncOpenCursor("cursor", new AsyncCallbacks.OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                recovery.complete(cursor);
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                recovery.completeExceptionally(exception);
            }
        }, null);
        ManagedCursor recovered = null;
        boolean resumed = false;
        try {
            assertThat(recovery).isNotDone();
            assertThat(activeListeners).containsExactly(listener);
            verify(ledgerManager).registerLedgerMetadataListener(eq(cursorLedgerId), same(listener));
            Futures.CloseFuture closing = new Futures.CloseFuture();
            Futures.CloseFuture repeatedClose = new Futures.CloseFuture();
            if (closeBeforeRecovery) {
                if (failLedgerClose) {
                    bkc.failNow(BKException.Code.UnexpectedConditionException);
                }
                reopened.asyncClose(closing, null);
                reopened.asyncClose(repeatedClose, null);
                assertThat(reopened.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
                assertThat(closing).isNotDone();
                assertThat(repeatedClose).isNotDone();
                assertThat(recovery).isNotDone();
            }
            resumeRead.run();
            resumed = true;
            if (failRead) {
                assertThatThrownBy(() -> recovery.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(ManagedLedgerException.class);
            } else if (closeBeforeRecovery) {
                assertThatThrownBy(() -> recovery.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(ManagedLedgerAlreadyClosedException.class);
            } else {
                recovered = recovery.get(5, TimeUnit.SECONDS);
            }
            if (!closeBeforeRecovery) {
                reopened.asyncClose(closing, null);
            }
            if (failLedgerClose) {
                Throwable closeError = closing.handle((ignore, error) -> error).get(5, TimeUnit.SECONDS);
                assertThat(closeError).isInstanceOf(ManagedLedgerException.class)
                        .hasMessage(BKException.getMessage(BKException.Code.UnexpectedConditionException)
                                + " error code: " + BKException.Code.UnexpectedConditionException);
                Throwable repeatedCloseError = repeatedClose.handle((ignore, error) -> error).get(5, TimeUnit.SECONDS);
                assertThat(repeatedCloseError).isSameAs(closeError);
            } else {
                closing.get(5, TimeUnit.SECONDS);
                if (closeBeforeRecovery) {
                    repeatedClose.get(5, TimeUnit.SECONDS);
                }
            }
            reopened.close();
            assertThat(reopened.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
            assertListenerReleased(handle, listener);
            if (recovered != null) {
                assertThat(recovered.isClosed()).isTrue();
            }
        } finally {
            if (!resumed) {
                resumeRead.run();
            }
            try {
                if (recovered == null && !recovery.isCompletedExceptionally()) {
                    recovered = recovery.get(5, TimeUnit.SECONDS);
                }
                if (recovered != null) {
                    recovered.close();
                }
                reopened.close();
            } finally {
                handle.close();
            }
        }
    }

    private void assertListenerReleased(LedgerHandle handle, LedgerMetadataListener listener) {
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(activeListeners).isEmpty();
            verify(ledgerManager).unregisterLedgerMetadataListener(eq(handle.getId()), same(listener));
        });
    }
}
