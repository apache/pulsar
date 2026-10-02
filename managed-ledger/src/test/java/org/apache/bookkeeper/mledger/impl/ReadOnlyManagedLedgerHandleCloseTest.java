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
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(timeOut = 30000)
public class ReadOnlyManagedLedgerHandleCloseTest extends MockedBookKeeperTestCase {
    private final List<LedgerHandle> openedHandles = new CopyOnWriteArrayList<>();
    private CompletableFuture<Void> openCompletion;

    @Override
    protected void setUpTestCase() throws Exception {
        openedHandles.clear();
        openCompletion = CompletableFuture.completedFuture(null);
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        bkc = spy(bkc);
        doAnswer(invocation -> trackOpens((OpenBuilder) invocation.callRealMethod()))
                .when(bkc).newOpenLedgerOp();
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
    }

    @Override
    protected void cleanUpTestCase() throws Exception {
        openCompletion.complete(null);
        // Initialization's temporary handle is covered by a separate fix.
        for (LedgerHandle handle : openedHandles) {
            doCallRealMethod().when(handle).closeAsync();
            handle.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings("unchecked")
    private OpenBuilder trackOpens(OpenBuilder builder) {
        OpenBuilder trackedBuilder = spy(builder);
        doAnswer(invocation -> ((CompletableFuture<ReadHandle>) invocation.callRealMethod()).thenCompose(handle -> {
            LedgerHandle trackedHandle = spy((LedgerHandle) handle);
            openedHandles.add(trackedHandle);
            return openCompletion.thenApply(ignored -> trackedHandle);
        })).when(trackedBuilder).execute();
        return trackedBuilder;
    }

    @DataProvider(name = "closeStates")
    public Object[][] closeStates() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "closeStates")
    public void testCloseReleasesCachedHandles(boolean fenced) throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS);
        ManagedLedger ledger = factory.open("readonly-close-cache", config);
        Position position = ledger.addEntry(new byte[] {1});
        ReadOnlyManagedLedgerImpl readOnly = new ReadOnlyManagedLedgerImpl(factory, bkc, factory.getMetaStore(),
                config, executor, ledger.getName());
        readOnly.initialize().get(5, TimeUnit.SECONDS);
        LedgerHandle handle = (LedgerHandle) readOnly.getLedgerHandle(position.getLedgerId()).get(5, TimeUnit.SECONDS);
        verify(handle, never()).closeAsync();
        if (fenced) {
            readOnly.setFenced();
            assertThatThrownBy(readOnly::close).isInstanceOf(ManagedLedgerFencedException.class);
        } else {
            readOnly.close();
            readOnly.close();
        }
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(handle).closeAsync());
        assertThat(readOnly.ledgerCache.size()).isZero();
    }

    @Test(dataProvider = "closeStates")
    public void testCloseRejectsSubsequentReadsWithoutOpeningHandles(boolean fenced) throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS);
        ManagedLedger ledger = factory.open("readonly-read-after-close", config);
        Position position = ledger.addEntry(new byte[] {1});
        ReadOnlyManagedLedgerImpl readOnly = new ReadOnlyManagedLedgerImpl(factory, bkc, factory.getMetaStore(),
                config, executor, ledger.getName());
        readOnly.initialize().get(5, TimeUnit.SECONDS);
        if (fenced) {
            readOnly.setFenced();
            assertThatThrownBy(readOnly::close).isInstanceOf(ManagedLedgerFencedException.class);
        } else {
            readOnly.close();
        }
        clearInvocations(bkc);
        CompletableFuture<Void> reading = new CompletableFuture<>();
        readOnly.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                entry.release();
                reading.complete(null);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                reading.completeExceptionally(exception);
            }
        }, null);
        assertThatThrownBy(() -> reading.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerAlreadyClosedException.class);
        assertThatThrownBy(() -> readOnly.reopenReadHandle(position.getLedgerId()).get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerAlreadyClosedException.class);
        verify(bkc, never()).newOpenLedgerOp();
        assertThat(readOnly.ledgerCache.size()).isZero();
    }

    @Test
    public void testCloseFailureDoesNotSkipOtherCachedHandles() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS)
                .setRetentionSizeInMB(-1).setMaxEntriesPerLedger(1);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        ManagedLedger ledger = factory.open("readonly-close-failure", config);
        Position first = ledger.addEntry(new byte[] {1});
        Position second = ledger.addEntry(new byte[] {2});
        assertThat(first.getLedgerId()).isNotEqualTo(second.getLedgerId());
        ReadOnlyManagedLedgerImpl readOnly = new ReadOnlyManagedLedgerImpl(factory, bkc, factory.getMetaStore(),
                config, executor, ledger.getName());
        readOnly.initialize().get(5, TimeUnit.SECONDS);
        ReadHandle firstHandle = readOnly.getLedgerHandle(first.getLedgerId()).get(5, TimeUnit.SECONDS);
        ReadHandle secondHandle = readOnly.getLedgerHandle(second.getLedgerId()).get(5, TimeUnit.SECONDS);
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("close failed")))
                .when(firstHandle).closeAsync();

        readOnly.close();
        readOnly.close();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(firstHandle).closeAsync();
            verify(secondHandle).closeAsync();
        });
        assertThat(readOnly.ledgerCache.size()).isZero();
    }

    @Test
    public void testCloseReleasesHandleWhoseOpenIsPending() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS);
        ManagedLedger ledger = factory.open("readonly-pending-open", config);
        Position position = ledger.addEntry(new byte[] {1});
        ReadOnlyManagedLedgerImpl readOnly = new ReadOnlyManagedLedgerImpl(factory, bkc, factory.getMetaStore(),
                config, executor, ledger.getName());
        readOnly.initialize().get(5, TimeUnit.SECONDS);
        openCompletion = new CompletableFuture<>();
        CompletableFuture<ReadHandle> opening = readOnly.getLedgerHandle(position.getLedgerId());
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> openedHandles.size() == 2);
        LedgerHandle handle = openedHandles.get(1);
        assertThat(opening).isNotDone();

        readOnly.close();
        assertThat(readOnly.ledgerCache.size()).isZero();
        verify(handle, never()).closeAsync();
        openCompletion.complete(null);
        opening.get(5, TimeUnit.SECONDS);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(handle).closeAsync());
    }
}
