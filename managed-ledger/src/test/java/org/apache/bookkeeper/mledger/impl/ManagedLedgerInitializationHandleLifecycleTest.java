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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(timeOut = 30000)
public class ManagedLedgerInitializationHandleLifecycleTest extends MockedBookKeeperTestCase {
    private final List<ReadHandle> openedHandles = new CopyOnWriteArrayList<>();
    private CompletableFuture<Void> openGate;
    private CompletableFuture<Void> interceptorGate;

    @Override
    protected void setUpTestCase() throws Exception {
        openedHandles.clear();
        openGate = CompletableFuture.completedFuture(null);
        interceptorGate = CompletableFuture.completedFuture(null);
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        bkc = spy(bkc);
        doAnswer(invocation -> trackOpen((OpenBuilder) invocation.callRealMethod()))
                .when(bkc).newOpenLedgerOp();
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
    }

    @Override
    protected void cleanUpTestCase() throws Exception {
        openGate.complete(null);
        interceptorGate.complete(null);
        for (ReadHandle handle : openedHandles) {
            handle.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings("unchecked")
    private OpenBuilder trackOpen(OpenBuilder original) {
        OpenBuilder builder = spy(original);
        doAnswer(invocation -> ((CompletableFuture<ReadHandle>) invocation.callRealMethod()).thenCompose(handle -> {
            ReadHandle tracked = spy(handle);
            openedHandles.add(tracked);
            return openGate.thenApply(ignored -> tracked);
        })).when(builder).execute();
        return builder;
    }

    private ManagedLedgerConfig storedLedger(String name, boolean terminated) throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS);
        ManagedLedger writer = factory.open(name, config);
        writer.addEntry(new byte[] {1});
        if (terminated) {
            writer.terminate();
        }
        writer.close();
        return config;
    }

    private CompletableFuture<ManagedLedger> reopen(String name, ManagedLedgerConfig config) {
        CompletableFuture<ManagedLedger> opened = new CompletableFuture<>();
        factory.asyncOpen(name, config, new AsyncCallbacks.OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                opened.complete(ledger);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException error, Object ctx) {
                opened.completeExceptionally(error);
            }
        }, null, null);
        return opened;
    }

    @Test
    public void testOrdinaryInitializationReleasesItsTemporaryHandle() throws Exception {
        String name = "ordinary-initialization-handle";
        ManagedLedgerConfig config = storedLedger(name, false);

        ManagedLedgerImpl reopened = (ManagedLedgerImpl) reopen(name, config).get(5, TimeUnit.SECONDS);

        assertThat(openedHandles).hasSize(1);
        ReadHandle temporaryHandle = openedHandles.get(0);
        assertThat(reopened.currentLedger).isNotSameAs(temporaryHandle);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(temporaryHandle).closeAsync());
        reopened.addEntry(new byte[] {2});
    }

    @Test
    public void testTerminatedInitializationTransfersHandleToManagedLedger() throws Exception {
        String name = "terminated-initialization-handle";
        ManagedLedgerConfig config = storedLedger(name, true);

        ManagedLedgerImpl reopened = (ManagedLedgerImpl) reopen(name, config).get(5, TimeUnit.SECONDS);

        assertThat(openedHandles).hasSize(1);
        LedgerHandle retained = (LedgerHandle) openedHandles.get(0);
        assertThat(reopened.currentLedger).isSameAs(retained);
        verify(retained, never()).closeAsync();
        assertThat(reopened.getState()).isEqualTo(ManagedLedgerImpl.State.Terminated);
        reopened.close();
        verify(retained).asyncClose(any(), any());
    }

    @DataProvider(name = "interceptorFailureModes")
    public Object[][] interceptorFailureModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "interceptorFailureModes")
    public void testInterceptorFailureReleasesTemporaryHandle(boolean synchronous) throws Exception {
        String name = "failed-interceptor-initialization-handle";
        ManagedLedgerConfig config = storedLedger(name, false);
        ManagedLedgerInterceptor interceptor = mock(ManagedLedgerInterceptor.class);
        IllegalStateException failure = new IllegalStateException("interceptor initialization failed");
        if (synchronous) {
            doThrow(failure).when(interceptor).onManagedLedgerLastLedgerInitialize(eq(name), any());
        } else {
            when(interceptor.onManagedLedgerLastLedgerInitialize(eq(name), any()))
                    .thenReturn(CompletableFuture.failedFuture(failure));
        }
        config.setManagedLedgerInterceptor(interceptor);

        assertThatThrownBy(() -> reopen(name, config).get(5, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(IllegalStateException.class).hasRootCauseMessage(failure.getMessage());

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(openedHandles.get(0)).closeAsync());
    }

    @Test
    public void testShutdownReleasesHandleWhileInterceptorRemainsPending() throws Exception {
        String name = "pending-interceptor-initialization-handle";
        ManagedLedgerConfig config = storedLedger(name, false);
        ManagedLedgerInterceptor interceptor = mock(ManagedLedgerInterceptor.class);
        interceptorGate = new CompletableFuture<>();
        when(interceptor.onManagedLedgerLastLedgerInitialize(eq(name), any())).thenReturn(interceptorGate);
        config.setManagedLedgerInterceptor(interceptor);
        CompletableFuture<ManagedLedger> initialization = reopen(name, config);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                verify(interceptor).onManagedLedgerLastLedgerInitialize(eq(name), any()));
        ReadHandle temporaryHandle = openedHandles.get(0);

        factory.shutdownAsync().get(5, TimeUnit.SECONDS);

        assertThat(interceptorGate).isNotDone();
        assertThat(initialization).isCompletedExceptionally();
        verify(temporaryHandle).closeAsync();
        interceptorGate.complete(null);
        verify(temporaryHandle).closeAsync();
    }

    @Test
    public void testShutdownReleasesInitializationHandleWhoseOpenCompletesLate() throws Exception {
        String name = "late-initialization-handle";
        ManagedLedgerConfig config = storedLedger(name, false);
        openGate = new CompletableFuture<>();
        CompletableFuture<ManagedLedger> initialization = reopen(name, config);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !openedHandles.isEmpty());
        ReadHandle lateHandle = openedHandles.get(0);

        factory.shutdownAsync().get(5, TimeUnit.SECONDS);

        assertThat(initialization).isCompletedExceptionally();
        verify(lateHandle, never()).closeAsync();
        openGate.complete(null);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(lateHandle).closeAsync());
    }

    @DataProvider(name = "rejectedOpenPublication")
    public Object[][] rejectedOpenPublication() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "rejectedOpenPublication")
    public void testRejectedCachedOpenPublicationClosesRawHandle(boolean closeBeforeOpenCompletes) throws Exception {
        ManagedLedgerImpl writer = (ManagedLedgerImpl) factory.open("rejected-cached-open-publication",
                defaultConfig().setRetentionTime(1, TimeUnit.DAYS));
        long ledgerId = writer.addEntry(new byte[] {1}).getLedgerId();
        OrderedExecutor originalExecutor = bkc.getMainWorkerPool();
        OrderedExecutor rejectingExecutor = OrderedExecutor.newBuilder().numThreads(1)
                .name("rejected-read-handle-publication").build();
        doReturn(rejectingExecutor).when(bkc).getMainWorkerPool();
        ReadOnlyManagedLedgerImpl readOnly = new ReadOnlyManagedLedgerImpl(factory, bkc, factory.getMetaStore(),
                writer.getConfig(), executor, writer.getName());
        CompletableFuture<ReadHandle> rawOpening = new CompletableFuture<>();
        ReadHandle handle = mock(ReadHandle.class);
        when(handle.getId()).thenReturn(ledgerId);
        when(handle.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        try {
            readOnly.initialize().get(5, TimeUnit.SECONDS);
            OpenBuilder builder = mock(OpenBuilder.class, RETURNS_SELF);
            when(builder.execute()).thenReturn(rawOpening);
            doReturn(builder).when(bkc).newOpenLedgerOp();
            CompletableFuture<ReadHandle> opening;
            if (closeBeforeOpenCompletes) {
                opening = readOnly.getLedgerHandle(ledgerId);
                readOnly.close();
                rejectingExecutor.shutdown();
                rawOpening.complete(handle);
            } else {
                rejectingExecutor.shutdown();
                rawOpening.complete(handle);
                // An already completed open rejects publication immediately after its promise is registered.
                opening = readOnly.getLedgerHandle(ledgerId);
            }

            assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(RejectedExecutionException.class);
            verify(handle).closeAsync();
            assertThat(readOnly.ledgerCache.size()).isZero();
        } finally {
            rawOpening.complete(handle);
            readOnly.close();
            doReturn(originalExecutor).when(bkc).getMainWorkerPool();
            rejectingExecutor.shutdownNow();
        }
    }
}
