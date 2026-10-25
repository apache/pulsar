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
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.ManagedLedgerInitializeLedgerCallback;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(timeOut = 30000)
public class ShadowManagedLedgerLifecycleTest extends MockedBookKeeperTestCase {
    private final List<ShadowManagedLedgerImpl> shadows = new ArrayList<>();
    private final List<CompletableFuture<Void>> delayedOpens = new ArrayList<>();

    @Override
    protected void cleanUpTestCase() throws Exception {
        delayedOpens.forEach(open -> open.complete(null));
        for (ShadowManagedLedgerImpl shadow : shadows) {
            shadow.close();
        }
        shadows.clear();
        delayedOpens.clear();
    }

    @Test
    public void testInitializationClosesSourceHandleOpenedAfterClose() throws Exception {
        ManagedLedgerImpl source = (ManagedLedgerImpl) factory.open("source-late-initialization", defaultConfig());
        source.addEntry(new byte[] {1});
        BookKeeper bookKeeper = spy(bkc);
        CompletableFuture<Void> allowOpen = new CompletableFuture<>();
        CompletableFuture<LedgerHandle> opened = delayNextOpen(bookKeeper, allowOpen);
        ShadowManagedLedgerImpl shadow = newShadow("shadow-late-initialization", source.getName(),
                bookKeeper, factory.getMetaStore());
        CompletableFuture<Void> initialization = initialize(shadow);
        LedgerHandle lateHandle = opened.get(5, TimeUnit.SECONDS);

        shadow.close();
        allowOpen.complete(null);

        assertThatThrownBy(() -> initialization.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerAlreadyClosedException.class);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(lateHandle).closeAsync());
        assertThat(shadow.currentLedger).isNull();
        assertThat(shadow.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
    }

    @Test
    public void testSourceWatchClosesLateHandleAndIgnoresUpdatesAfterClose() throws Exception {
        ManagedLedgerImpl source = (ManagedLedgerImpl) factory.open("source-late-watch", defaultConfig());
        source.addEntry(new byte[] {1});
        ManagedLedgerImpl nextLedger = (ManagedLedgerImpl) factory.open("source-next-ledger", defaultConfig());
        nextLedger.addEntry(new byte[] {2});
        BookKeeper bookKeeper = spy(bkc);
        MetaStore store = spy(factory.getMetaStore());
        AtomicReference<MetaStore.UpdateCallback<ManagedLedgerInfo>> watcher = new AtomicReference<>();
        doAnswer(invocation -> {
            watcher.set(invocation.getArgument(1));
            return invocation.callRealMethod();
        }).when(store).watchManagedLedgerInfo(eq(source.getName()), any());
        ShadowManagedLedgerImpl shadow = newShadow("shadow-late-watch", source.getName(), bookKeeper, store);
        initialize(shadow).get(5, TimeUnit.SECONDS);
        LedgerHandle original = shadow.currentLedger;
        CompletableFuture<Void> allowOpen = new CompletableFuture<>();
        CompletableFuture<LedgerHandle> opened = delayNextOpen(bookKeeper, allowOpen);
        ManagedLedgerInfo updated = new ManagedLedgerInfo()
                .addAllLedgerInfos(source.getLedgersInfo().values())
                .addAllLedgerInfos(nextLedger.getLedgersInfo().values());
        watcher.get().onUpdate(updated, new Stat(source.getName(), Long.MAX_VALUE - 1, 0, 0, false, true));
        LedgerHandle lateHandle = opened.get(5, TimeUnit.SECONDS);

        shadow.close();
        allowOpen.complete(null);

        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(lateHandle).closeAsync());
        assertThat(shadow.currentLedger).isSameAs(original);
        assertThat(shadow.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
        clearInvocations(bookKeeper);
        watcher.get().onUpdate(updated, new Stat(source.getName(), Long.MAX_VALUE, 0, 0, false, true));
        CompletableFuture<Void> processed = new CompletableFuture<>();
        shadow.getExecutor().execute(() -> processed.complete(null));
        processed.get(5, TimeUnit.SECONDS);
        verify(bookKeeper, never()).newOpenLedgerOp();
    }

    private ShadowManagedLedgerImpl newShadow(String name, String source, BookKeeper bookKeeper, MetaStore store) {
        ManagedLedgerConfig config = defaultConfig();
        config.setShadowSourceName(source);
        ShadowManagedLedgerImpl shadow = new ShadowManagedLedgerImpl(factory, bookKeeper, store, config,
                executor, name, null);
        shadows.add(shadow);
        return shadow;
    }

    private CompletableFuture<Void> initialize(ShadowManagedLedgerImpl shadow) {
        CompletableFuture<Void> initialization = new CompletableFuture<>();
        shadow.initialize(new ManagedLedgerInitializeLedgerCallback() {
            @Override
            public void initializeComplete() {
                initialization.complete(null);
            }

            @Override
            public void initializeFailed(ManagedLedgerException error) {
                initialization.completeExceptionally(error);
            }
        }, null);
        return initialization;
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<LedgerHandle> delayNextOpen(BookKeeper bookKeeper, CompletableFuture<Void> allowOpen) {
        delayedOpens.add(allowOpen);
        CompletableFuture<LedgerHandle> opened = new CompletableFuture<>();
        doAnswer(invocation -> {
            OpenBuilder builder = spy((OpenBuilder) invocation.callRealMethod());
            doAnswer(execution -> ((CompletableFuture<ReadHandle>) execution.callRealMethod()).thenCompose(handle -> {
                LedgerHandle tracked = spy((LedgerHandle) handle);
                opened.complete(tracked);
                return allowOpen.thenApply(ignored -> tracked);
            })).when(builder).execute();
            return builder;
        }).when(bookKeeper).newOpenLedgerOp();
        return opened;
    }
}
