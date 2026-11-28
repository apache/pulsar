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
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.MetaStore;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.mledger.util.ManagedLedgerUtils;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.metadata.api.GetResult;
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
    private CompletableFuture<Runnable> delayedRead;

    @Override
    protected void setUpTestCase() throws Exception {
        activeListeners.clear();
        openedHandles.clear();
        delayedReadLedgerId = -1;
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
        metadataStore = spy(metadataStore);
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
                        Runnable read = () -> ((LedgerHandle) handle).asyncReadEntries(firstEntry, lastEntry,
                                (rc, originalHandle, entries, readCtx) ->
                                        callback.readComplete(rc, this, entries, readCtx),
                                ctx);
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

    @DataProvider(name = "booleans")
    public Object[][] booleans() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "booleans")
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

    @Test
    public void testCursorCloseReleasesRecoveredListenerOnMetadataFailure() throws Exception {
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
            assertListenerReleased(handle, listener);
            // Retry the public close APIs after the one injected failure has been consumed.
            recovered.close();
            reopened.close();
            assertThat(injectedFailures).hasValue(1);
            assertThat(recovered.getState()).isEqualTo("Closing");
            assertThat(bkc.getLedgers()).contains(cursorLedgerId);
            verify(bkc, never()).asyncDeleteLedger(eq(cursorLedgerId), any(), any());
            assertListenerReleased(handle, listener);
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
        } finally {
            try {
                reopened.close();
            } finally {
                handle.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDeleteMetadataFailureAfterUnloadReleasesRecoveredListener() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1)
                .setMaxUnackedRangesToPersistInMetadataStore(0);
        ManagedLedger ledger = factory.open("cursor-delete-metadata-failure-listener", config);
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
        String cursorPath = "/managed-ledgers/" + ledger.getName() + "/cursor";
        GetResult metadataBeforeDelete = metadataStore.get(cursorPath).get(5, TimeUnit.SECONDS).orElseThrow();
        CompletableFuture<Void> releaseDelete = new CompletableFuture<>();
        metadataStore.failConditional(new MetadataStoreException("injected cursor metadata delete failure"),
                (operation, path) -> operation == FaultInjectionMetadataStore.OperationType.DELETE
                        && path.equals(cursorPath));
        doAnswer(invocation -> {
            CompletableFuture<Void> failedDelete = (CompletableFuture<Void>) invocation.callRealMethod();
            return releaseDelete.thenCompose(ignored -> failedDelete);
        }).doCallRealMethod().when(metadataStore).delete(eq(cursorPath), any());
        CompletableFuture<Void> deletion = new CompletableFuture<>();
        try {
            assertThat(activeListeners).containsExactly(listener);
            reopened.asyncDeleteCursor("cursor", new AsyncCallbacks.DeleteCursorCallback() {
                @Override
                public void deleteCursorComplete(Object ctx) {
                    deletion.complete(null);
                }

                @Override
                public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                    deletion.completeExceptionally(exception);
                }
            }, null);
            assertThat(deletion).isNotDone();
            Futures.CloseFuture closing = new Futures.CloseFuture();
            reopened.asyncClose(closing, null);
            closing.get(5, TimeUnit.SECONDS);
            releaseDelete.complete(null);
            assertThatThrownBy(() -> deletion.get(5, TimeUnit.SECONDS))
                    .hasRootCauseMessage("injected cursor metadata delete failure");
            assertThat(recovered.getState()).isEqualTo("DeletingFailed");
            assertListenerReleased(handle, listener);
            assertCursorMetadataUnchanged(cursorPath, metadataBeforeDelete, cursorLedgerId);
        } finally {
            releaseDelete.complete(null);
            try {
                reopened.close();
            } finally {
                handle.close();
            }
        }
    }

    @Test
    public void testEagerRecoveryAfterInitializationFailureReleasesListener() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1)
                .setMaxUnackedRangesToPersistInMetadataStore(0);
        ManagedLedger ledger = factory.open("eager-recovery-initialization-failure-listener", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("slow-cursor");
        ledger.openCursor("failing-cursor");
        ledger.addEntry(new byte[] {1});
        Position second = ledger.addEntry(new byte[] {2});
        cursor.delete(second);
        long cursorLedgerId = cursor.getCursorLedger();
        assertThat(cursorLedgerId).isNotEqualTo(-1);
        ledger.close();

        String cursorPath = "/managed-ledgers/" + ledger.getName() + "/slow-cursor";
        GetResult metadataBeforeRecovery = metadataStore.get(cursorPath).get(5, TimeUnit.SECONDS).orElseThrow();
        CompletableFuture<Optional<GetResult>> failingMetadata = new CompletableFuture<>();
        doReturn(failingMetadata).doCallRealMethod().when(metadataStore)
                .get(eq("/managed-ledgers/" + ledger.getName() + "/failing-cursor"));
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        CompletableFuture<ManagedLedgerImpl> initializing = new CompletableFuture<>();
        Futures.CloseFuture firstClose = new Futures.CloseFuture();
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                    ManagedLedgerConfig ledgerConfig, Supplier<CompletableFuture<Boolean>> ownershipChecker) {
                ManagedLedgerImpl created = new ManagedLedgerImpl(this, bk, store, ledgerConfig,
                        scheduledExecutor, name, ownershipChecker) {
                    @Override
                    public synchronized void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
                        super.asyncClose(new AsyncCallbacks.CloseCallback() {
                            @Override
                            public void closeComplete(Object context) {
                                firstClose.closeComplete(context);
                                callback.closeComplete(context);
                            }

                            @Override
                            public void closeFailed(ManagedLedgerException exception, Object context) {
                                firstClose.closeFailed(exception, context);
                                callback.closeFailed(exception, context);
                            }
                        }, ctx);
                    }
                };
                initializing.complete(created);
                return created;
            }
        };
        delayedReadLedgerId = cursorLedgerId;
        CompletableFuture<ManagedLedger> opening = new CompletableFuture<>();
        factory.asyncOpen(ledger.getName(), config.setLazyCursorRecovery(false),
                new AsyncCallbacks.OpenLedgerCallback() {
                    @Override
                    public void openLedgerComplete(ManagedLedger opened, Object ctx) {
                        opening.complete(opened);
                    }

                    @Override
                    public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        opening.completeExceptionally(exception);
                    }
                }, null, null);
        Runnable resumeRead = delayedRead.get(5, TimeUnit.SECONDS);
        LedgerHandle handle = openedHandles.stream().filter(h -> h.getId() == cursorLedgerId).findFirst().orElseThrow();
        LedgerMetadataListener listener = (LedgerMetadataListener) handle;
        boolean resumed = false;
        try {
            assertThat(activeListeners).containsExactly(listener);
            assertThat(opening).isNotDone();
            failingMetadata.completeExceptionally(new MetadataStoreException("injected eager cursor metadata failure"));
            assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS))
                    .hasRootCauseMessage("injected eager cursor metadata failure");
            firstClose.get(5, TimeUnit.SECONDS);
            ManagedLedgerImpl failedLedger = initializing.get(5, TimeUnit.SECONDS);
            assertThat(failedLedger.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
            verify(ledgerManager, never()).unregisterLedgerMetadataListener(eq(cursorLedgerId), same(listener));

            resumeRead.run();
            resumed = true;
            assertListenerReleased(handle, listener);
            assertThat(failedLedger.getCursors()).isEmpty();
            assertThat(failedLedger.getActiveCursors()).isEmpty();
            assertCursorMetadataUnchanged(cursorPath, metadataBeforeRecovery, cursorLedgerId);
        } finally {
            failingMetadata.completeExceptionally(new MetadataStoreException("test cleanup"));
            if (!resumed) {
                resumeRead.run();
            }
            handle.close();
        }
    }

    @Test
    public void testLateRecoveryReleasesListenerWithoutChangingNextOwnersCursor() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1)
                .setMaxUnackedRangesToPersistInMetadataStore(0);
        ManagedLedger ledger = factory.open("late-recovery-listener", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position first = ledger.addEntry(new byte[] {1});
        Position second = ledger.addEntry(new byte[] {2});
        cursor.delete(second);
        long cursorLedgerId = cursor.getCursorLedger();
        assertThat(cursorLedgerId).isNotEqualTo(-1);
        ledger.close();

        delayedReadLedgerId = cursorLedgerId;
        ManagedLedgerImpl previousOwner = (ManagedLedgerImpl) factory.open(ledger.getName(),
                config.setLazyCursorRecovery(true));
        Runnable resumeRead = delayedRead.get(5, TimeUnit.SECONDS);
        LedgerHandle previousHandle = openedHandles.stream().filter(h -> h.getId() == cursorLedgerId)
                .findFirst().orElseThrow();
        LedgerMetadataListener previousListener = (LedgerMetadataListener) previousHandle;
        CompletableFuture<ManagedCursor> recovery = new CompletableFuture<>();
        AtomicInteger recoveryCallbacks = new AtomicInteger();
        previousOwner.asyncOpenCursor("cursor", new AsyncCallbacks.OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor recovered, Object ctx) {
                recoveryCallbacks.incrementAndGet();
                recovery.complete(recovered);
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                recoveryCallbacks.incrementAndGet();
                recovery.completeExceptionally(exception);
            }
        }, null);
        String ledgerPath = "/managed-ledgers/" + ledger.getName();
        CompletableFuture<Void> releaseWrite = new CompletableFuture<>();
        CompletableFuture<Void> writeStarted = new CompletableFuture<>();
        doAnswer(invocation -> {
            byte[] value = invocation.getArgument(1);
            Optional<Long> expectedVersion = invocation.getArgument(2);
            writeStarted.complete(null);
            return releaseWrite.thenCompose(ignored -> metadataStore.put(ledgerPath, value, expectedVersion));
        }).doCallRealMethod().when(metadataStore).put(eq(ledgerPath), any(byte[].class), any());
        Futures.CloseFuture closing = new Futures.CloseFuture();
        ManagedLedger nextOwner = null;
        boolean resumed = false;
        try {
            assertThat(activeListeners).containsExactly(previousListener);
            CompletableFuture<Void> update = previousOwner.asyncAddLedgerProperty(first.getLedgerId(), "key", "value");
            writeStarted.get(5, TimeUnit.SECONDS);
            previousOwner.asyncClose(closing, null);
            closing.get(5, TimeUnit.SECONDS);
            assertThat(recovery).as("managed-ledger close must not wait for the recovery read").isNotDone();
            verify(ledgerManager, never()).unregisterLedgerMetadataListener(eq(cursorLedgerId), same(previousListener));

            nextOwner = factory.open(ledger.getName(), config);
            ManagedCursor nextCursor = nextOwner.openCursor("cursor");
            LedgerHandle nextHandle = openedHandles.stream()
                    .filter(h -> h.getId() == cursorLedgerId && h != previousHandle).findFirst().orElseThrow();
            LedgerMetadataListener nextListener = (LedgerMetadataListener) nextHandle;
            assertThat(activeListeners).containsExactlyInAnyOrder(previousListener, nextListener);
            String cursorPath = ledgerPath + "/cursor";
            GetResult metadataBeforeRecovery = metadataStore.get(cursorPath).get(5, TimeUnit.SECONDS).orElseThrow();

            // The new owner's initialization advances the version, so the held write now fails with BadVersion.
            releaseWrite.complete(null);
            assertThatThrownBy(() -> update.get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(MetadataStoreException.BadVersionException.class);

            resumeRead.run();
            resumed = true;
            assertThatThrownBy(() -> recovery.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerAlreadyClosedException.class);
            assertThat(recoveryCallbacks).hasValue(1);
            assertThat(previousOwner.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(activeListeners).containsExactly(nextListener);
                verify(ledgerManager).unregisterLedgerMetadataListener(eq(cursorLedgerId), same(previousListener));
            });
            verify(ledgerManager).registerLedgerMetadataListener(eq(cursorLedgerId), same(previousListener));
            verify(ledgerManager, never()).unregisterLedgerMetadataListener(eq(cursorLedgerId), same(nextListener));
            assertThat(previousOwner.getCursors()).isEmpty();
            assertThat(previousOwner.getActiveCursors()).isEmpty();
            assertCursorMetadataUnchanged(cursorPath, metadataBeforeRecovery, cursorLedgerId);
            assertThat(nextCursor.isMessageDeleted(second)).isTrue();
            assertThat(nextCursor.getNumberOfEntriesInBacklog(true)).isEqualTo(1);
            nextCursor.markDelete(first);
            assertThat(nextCursor.getNumberOfEntriesInBacklog(true)).isZero();
        } finally {
            releaseWrite.cancel(false);
            if (!resumed) {
                resumeRead.run();
            }
            try {
                if (nextOwner != null) {
                    nextOwner.close();
                }
                if (!closing.isDone()) {
                    previousOwner.close();
                }
            } finally {
                previousHandle.close();
            }
        }
    }

    @Test(dataProvider = "booleans")
    public void testFencedUnloadReleasesRecoveredCursorListener(boolean recoverBeforeFencing) throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1)
                .setMaxUnackedRangesToPersistInMetadataStore(0);
        ManagedLedger ledger = factory.open("fenced-unload-listener", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position first = ledger.addEntry(new byte[] {1});
        Position second = ledger.addEntry(new byte[] {2});
        cursor.delete(second);
        long cursorLedgerId = cursor.getCursorLedger();
        assertThat(cursorLedgerId).isNotEqualTo(-1);
        ledger.close();

        delayedReadLedgerId = cursorLedgerId;
        ManagedLedgerImpl previousOwner = (ManagedLedgerImpl) factory.open(ledger.getName(),
                config.setLazyCursorRecovery(true));
        CompletableFuture<Void> resumeRead = new CompletableFuture<>();
        resumeRead.thenRun(delayedRead.get(5, TimeUnit.SECONDS));
        LedgerHandle handle = openedHandles.stream().filter(h -> h.getId() == cursorLedgerId).findFirst().orElseThrow();
        LedgerMetadataListener listener = (LedgerMetadataListener) handle;
        try {
            CompletableFuture<ManagedCursor> opening = ManagedLedgerUtils.openCursor(previousOwner, "cursor");
            assertThat(opening).isNotDone();
            if (recoverBeforeFencing) {
                resumeRead.complete(null);
                assertThat(opening.get(5, TimeUnit.SECONDS).isMessageDeleted(second)).isTrue();
            }
            assertThat(activeListeners).containsExactly(listener);
            String ledgerPath = "/managed-ledgers/" + ledger.getName();
            String cursorPath = ledgerPath + "/cursor";
            GetResult cursorMetadata = metadataStore.get(cursorPath).get(5, TimeUnit.SECONDS).orElseThrow();

            // Advance the managed ledger's metadata version so the old owner's next update fences it.
            GetResult ledgerMetadata = metadataStore.get(ledgerPath).get(5, TimeUnit.SECONDS).orElseThrow();
            metadataStore.put(ledgerPath, ledgerMetadata.getValue(), Optional.of(ledgerMetadata.getStat().getVersion()))
                    .get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> previousOwner.asyncAddLedgerProperty(first.getLedgerId(), "key", "value")
                    .get(5, TimeUnit.SECONDS)).hasRootCauseInstanceOf(MetadataStoreException.BadVersionException.class);
            assertThat(previousOwner.getState()).isEqualTo(ManagedLedgerImpl.State.Fenced);
            assertThatThrownBy(previousOwner::close).isInstanceOf(ManagedLedgerFencedException.class);

            if (!recoverBeforeFencing) {
                resumeRead.complete(null);
                assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(ManagedLedgerFencedException.class);
                assertThat(previousOwner.getCursors()).isEmpty();
                assertThat(previousOwner.getActiveCursors()).isEmpty();
            }
            assertListenerReleased(handle, listener);
            assertCursorMetadataUnchanged(cursorPath, cursorMetadata, cursorLedgerId);
            ManagedLedger nextOwner = factory.open(ledger.getName(), config);
            try {
                ManagedCursor nextCursor = nextOwner.openCursor("cursor");
                assertThat(nextCursor.isMessageDeleted(second)).isTrue();
                assertThat(nextCursor.getNumberOfEntriesInBacklog(true)).isEqualTo(1);
            } finally {
                nextOwner.close();
            }
        } finally {
            resumeRead.complete(null);
            handle.close();
        }
    }

    @Test(dataProvider = "booleans")
    public void testLateRecoveryReleasesListenerWhileWriterCloseIsPending(boolean terminateWhileClosing)
            throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1)
                .setMaxUnackedRangesToPersistInMetadataStore(0);
        ManagedLedger ledger = factory.open("late-recovery-during-writer-close", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        ledger.addEntry(new byte[] {1});
        cursor.delete(ledger.addEntry(new byte[] {2}));
        long cursorLedgerId = cursor.getCursorLedger();
        assertThat(cursorLedgerId).isNotEqualTo(-1);
        ledger.close();

        delayedReadLedgerId = cursorLedgerId;
        ManagedLedgerImpl previousOwner = (ManagedLedgerImpl) factory.open(ledger.getName(),
                config.setLazyCursorRecovery(true));
        CompletableFuture<Void> resumeRead = new CompletableFuture<>();
        resumeRead.thenRun(delayedRead.get(5, TimeUnit.SECONDS));
        LedgerHandle handle = openedHandles.stream().filter(h -> h.getId() == cursorLedgerId).findFirst().orElseThrow();
        LedgerMetadataListener listener = (LedgerMetadataListener) handle;
        String cursorPath = "/managed-ledgers/" + ledger.getName() + "/cursor";
        GetResult cursorMetadata = metadataStore.get(cursorPath).get(5, TimeUnit.SECONDS).orElseThrow();
        CompletableFuture<Void> releaseWriterClose = bkc.promiseAfter(0);
        try {
            CompletableFuture<ManagedCursor> opening = ManagedLedgerUtils.openCursor(previousOwner, "cursor");
            assertThat(opening).isNotDone();
            assertThat(activeListeners).containsExactly(listener);
            Futures.CloseFuture closing = new Futures.CloseFuture();
            previousOwner.asyncClose(closing, null);
            assertThat(closing).isNotDone();

            if (terminateWhileClosing) {
                CompletableFuture<Position> terminating = new CompletableFuture<>();
                previousOwner.asyncTerminate(new AsyncCallbacks.TerminateCallback() {
                    @Override
                    public void terminateComplete(Position position, Object ctx) {
                        terminating.complete(position);
                    }

                    @Override
                    public void terminateFailed(ManagedLedgerException exception, Object ctx) {
                        terminating.completeExceptionally(exception);
                    }
                }, null);
                assertThatThrownBy(() -> terminating.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(ManagedLedgerAlreadyClosedException.class);
                assertThat(closing).isNotDone();
            }

            resumeRead.complete(null);
            assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerAlreadyClosedException.class);
            assertThatThrownBy(() -> ManagedLedgerUtils.openCursor(previousOwner, "cursor").get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class);
            assertThat(previousOwner.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
            assertListenerReleased(handle, listener);
            assertThat(closing).isNotDone();
            assertCursorMetadataUnchanged(cursorPath, cursorMetadata, cursorLedgerId);

            releaseWriterClose.complete(null);
            closing.get(5, TimeUnit.SECONDS);
            assertCursorMetadataUnchanged(cursorPath, cursorMetadata, cursorLedgerId);
        } finally {
            releaseWriterClose.complete(null);
            resumeRead.complete(null);
            handle.close();
        }
    }

    @Test
    public void testLateRecoveryReleasesListenerAfterFailedDeleteAndUnload() throws Exception {
        factory = spy(factory);
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1)
                .setMaxUnackedRangesToPersistInMetadataStore(0);
        ManagedLedger ledger = factory.open("late-recovery-after-failed-delete", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        ledger.addEntry(new byte[] {1});
        cursor.delete(ledger.addEntry(new byte[] {2}));
        long cursorLedgerId = cursor.getCursorLedger();
        assertThat(cursorLedgerId).isNotEqualTo(-1);
        ledger.close();

        delayedReadLedgerId = cursorLedgerId;
        ManagedLedgerImpl previousOwner = (ManagedLedgerImpl) factory.open(ledger.getName(),
                config.setLazyCursorRecovery(true));
        CompletableFuture<Void> resumeRead = new CompletableFuture<>();
        resumeRead.thenRun(delayedRead.get(5, TimeUnit.SECONDS));
        LedgerHandle handle = openedHandles.stream().filter(h -> h.getId() == cursorLedgerId).findFirst().orElseThrow();
        LedgerMetadataListener listener = (LedgerMetadataListener) handle;
        String cursorPath = "/managed-ledgers/" + ledger.getName() + "/cursor";
        GetResult cursorMetadata = metadataStore.get(cursorPath).get(5, TimeUnit.SECONDS).orElseThrow();
        try {
            CompletableFuture<ManagedCursor> opening = ManagedLedgerUtils.openCursor(previousOwner, "cursor");
            assertThat(opening).isNotDone();
            assertThat(activeListeners).containsExactly(listener);

            doReturn(false).when(factory).isMetadataServiceAvailable();
            try {
                assertThatThrownBy(previousOwner::delete).hasMessageContaining("Metadata service is not available");
            } finally {
                doCallRealMethod().when(factory).isMetadataServiceAvailable();
            }
            assertThat(previousOwner.getState()).isEqualTo(ManagedLedgerImpl.State.FencedForDeletion);
            assertThatThrownBy(previousOwner::close).isInstanceOf(ManagedLedgerFencedException.class);

            resumeRead.complete(null);
            assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerFencedException.class);
            assertThat(previousOwner.getActiveCursors()).isEmpty();
            assertListenerReleased(handle, listener);
            assertCursorMetadataUnchanged(cursorPath, cursorMetadata, cursorLedgerId);

            previousOwner.delete();
            assertThat(metadataStore.get(cursorPath).get(5, TimeUnit.SECONDS)).isEmpty();
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(bkc.getLedgers()).doesNotContain(cursorLedgerId));
        } finally {
            resumeRead.complete(null);
            handle.close();
        }
    }

    @Test
    public void testWriterCloseFailureReleasesRecoveredListenerWithoutGracefulCursorClose() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1)
                .setMaxUnackedRangesToPersistInMetadataStore(0);
        ManagedLedger ledger = factory.open("writer-close-failure-listener", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        ledger.addEntry(new byte[] {1});
        Position second = ledger.addEntry(new byte[] {2});
        cursor.delete(second);
        long cursorLedgerId = cursor.getCursorLedger();
        assertThat(cursorLedgerId).isNotEqualTo(-1);
        ledger.close();

        ManagedLedger reopened = factory.open(ledger.getName(), config);
        ManagedCursor recovered = reopened.openCursor("cursor");
        LedgerHandle handle = openedHandles.stream().filter(h -> h.getId() == cursorLedgerId).findFirst().orElseThrow();
        LedgerMetadataListener listener = (LedgerMetadataListener) handle;
        String cursorPath = "/managed-ledgers/" + ledger.getName() + "/cursor";
        GetResult metadataBeforeClose = metadataStore.get(cursorPath).get(5, TimeUnit.SECONDS).orElseThrow();
        try {
            assertThat(activeListeners).containsExactly(listener);
            verify(ledgerManager).registerLedgerMetadataListener(eq(cursorLedgerId), same(listener));
            bkc.failNow(BKException.Code.UnexpectedConditionException);
            Futures.CloseFuture closing = new Futures.CloseFuture();
            reopened.asyncClose(closing, null);
            Throwable closeError = closing.handle((ignore, error) -> error).get(5, TimeUnit.SECONDS);
            assertThat(closeError).isInstanceOf(ManagedLedgerException.class)
                    .hasMessage(BKException.getMessage(BKException.Code.UnexpectedConditionException)
                            + " error code: " + BKException.Code.UnexpectedConditionException);
            assertListenerReleased(handle, listener);
            assertCursorMetadataUnchanged(cursorPath, metadataBeforeClose, cursorLedgerId);
            assertThat(recovered.isMessageDeleted(second)).isTrue();
        } finally {
            try {
                reopened.close();
            } finally {
                handle.close();
            }
        }
    }

    private void assertCursorMetadataUnchanged(String path, GetResult before, long cursorLedgerId) throws Exception {
        GetResult after = metadataStore.get(path).get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(after.getValue()).isEqualTo(before.getValue());
        assertThat(after.getStat().getVersion()).isEqualTo(before.getStat().getVersion());
        assertThat(bkc.getLedgers()).contains(cursorLedgerId);
        verify(bkc, never()).asyncDeleteLedger(eq(cursorLedgerId), any(), any());
    }

    private void assertListenerReleased(LedgerHandle handle, LedgerMetadataListener listener) {
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(activeListeners).isEmpty();
            verify(ledgerManager).unregisterLedgerMetadataListener(eq(handle.getId()), same(listener));
        });
    }
}
