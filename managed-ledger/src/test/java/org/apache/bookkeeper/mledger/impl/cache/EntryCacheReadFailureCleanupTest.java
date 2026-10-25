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
package org.apache.bookkeeper.mledger.impl.cache;

import static org.apache.bookkeeper.mledger.util.ManagedLedgerUtils.NO_MAX_SIZE_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.common.util.ThreadBoundExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryMBeanImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.bookkeeper.test.DirectThreadBoundExecutor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(timeOut = 10000)
public class EntryCacheReadFailureCleanupTest {
    private ManagedLedgerImpl ledger;
    private ManagedLedgerConfig config;
    private ReadHandle readHandle;
    private RangeEntryCacheManagerImpl manager;
    private List<ByteBuf> buffers;

    @BeforeMethod
    public void setUp() {
        ledger = mock(ManagedLedgerImpl.class);
        config = spy(new ManagedLedgerConfig());
        when(ledger.getName()).thenReturn("read-failure-cleanup");
        when(ledger.getConfig()).thenReturn(config);
        when(ledger.getExecutor()).thenReturn(new DirectThreadBoundExecutor());
        when(ledger.getOptionalLedgerInfo(1L)).thenReturn(Optional.empty());
        when(ledger.getMbean()).thenReturn(mock(ManagedLedgerMBeanImpl.class));
        ManagedLedgerFactoryImpl factory = mock(ManagedLedgerFactoryImpl.class);
        ManagedLedgerFactoryMBeanImpl factoryMBean = mock(ManagedLedgerFactoryMBeanImpl.class);
        when(factory.getMbean()).thenReturn(factoryMBean);
        when(ledger.getFactory()).thenReturn(factory);
        manager = mock(RangeEntryCacheManagerImpl.class);
        when(manager.getMlFactoryMBean()).thenReturn(factoryMBean);
        readHandle = mock(ReadHandle.class);
        when(readHandle.getId()).thenReturn(1L);
        buffers = new ArrayList<>();
    }

    private void prepareStorageRead() {
        List<LedgerEntry> entries = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            ByteBuf buffer = Unpooled.buffer(4).writeInt(i);
            buffers.add(buffer);
            entries.add(LedgerEntryImpl.create(1L, i, buffer.readableBytes(), buffer));
        }
        LedgerEntries result = LedgerEntriesImpl.create(entries);
        when(readHandle.readAsync(0, 1)).thenReturn(CompletableFuture.completedFuture(result));
    }

    private void releaseRemainingBuffers() {
        for (ByteBuf buffer : buffers) {
            if (buffer.refCnt() > 0) {
                buffer.release(buffer.refCnt());
            }
        }
    }

    @Test
    public void testCacheInsertionFailureReleasesEveryTransformedEntry() throws Exception {
        IllegalStateException failure = new IllegalStateException("cache insertion failed");
        RangeEntryCacheImpl cache = new RangeEntryCacheImpl(manager, ledger, false,
                new RangeCacheRemovalQueue(0, false), (owner, entry) -> {
                    throw failure;
                });
        prepareStorageRead();
        try {
            CompletableFuture<List<Entry>> reading =
                    cache.readFromStorage(readHandle, 0, 1, NO_MAX_SIZE_LIMIT, () -> 2);

            assertThatThrownBy(() -> reading.get(5, TimeUnit.SECONDS)).hasCause(failure);
            assertThat(cache.getSize()).isZero();
            assertThat(buffers).allSatisfy(buffer -> assertThat(buffer.refCnt()).isZero());
        } finally {
            cache.clear();
            releaseRemainingBuffers();
        }
    }

    @Test
    public void testDisabledCacheConversionFailureReleasesEntryBeforeMetadataInitialization() throws Exception {
        IllegalStateException failure = new IllegalStateException("entry conversion failed");
        // Fail after the second EntryImpl was allocated, before that entry's metadata is initialized.
        when(config.isPulsarMessageEntries()).thenReturn(false).thenThrow(failure);
        EntryCacheDisabled cache = new EntryCacheDisabled(ledger);
        prepareStorageRead();
        CompletableFuture<List<Entry>> reading = new CompletableFuture<>();
        try {
            cache.asyncReadEntry(readHandle, 0, 1, NO_MAX_SIZE_LIMIT, () -> 2,
                    new AsyncCallbacks.ReadEntriesCallback() {
                        @Override
                        public void readEntriesComplete(List<Entry> entries, Object ctx) {
                            reading.complete(entries);
                        }

                        @Override
                        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                            reading.completeExceptionally(exception);
                        }
                    }, null);

            assertThatThrownBy(() -> reading.get(5, TimeUnit.SECONDS)).hasRootCause(failure);
            assertThat(buffers).allSatisfy(buffer -> assertThat(buffer.refCnt()).isZero());
        } finally {
            releaseRemainingBuffers();
        }
    }
    @Test
    public void testDisabledCacheRejectedExecutorClosesRawEntriesAndFailsOnce() throws Exception {
        RejectedExecutionException rejection = new RejectedExecutionException("owner executor stopped");
        ThreadBoundExecutor executor = mock(ThreadBoundExecutor.class);
        doThrow(rejection).when(executor).execute(any(Runnable.class));
        when(ledger.getExecutor()).thenReturn(executor);
        EntryCacheDisabled cache = new EntryCacheDisabled(ledger);
        prepareStorageRead();
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        AtomicReference<Thread> failureThread = new AtomicReference<>();
        CompletableFuture<List<Entry>> reading = new CompletableFuture<>();
        try {
            cache.asyncReadEntry(readHandle, 0, 1, NO_MAX_SIZE_LIMIT, () -> 2,
                    new AsyncCallbacks.ReadEntriesCallback() {
                        @Override
                        public void readEntriesComplete(List<Entry> entries, Object ctx) {
                            successes.incrementAndGet();
                            reading.complete(entries);
                        }

                        @Override
                        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                            failureThread.set(Thread.currentThread());
                            failures.incrementAndGet();
                            reading.completeExceptionally(exception);
                        }
                    }, null);

            assertThatThrownBy(() -> reading.get(5, TimeUnit.SECONDS)).hasRootCause(rejection);
            assertThat(successes).hasValue(0);
            assertThat(failures).hasValue(1);
            assertThat(failureThread.get()).isSameAs(Thread.currentThread());
            assertThat(buffers).allSatisfy(buffer -> assertThat(buffer.refCnt()).isZero());
        } finally {
            releaseRemainingBuffers();
        }
    }

    @Test
    public void testDisabledCacheThrowingSuccessCallbackRetainsCallerOwnership() {
        EntryCacheDisabled cache = new EntryCacheDisabled(ledger);
        prepareStorageRead();
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        AtomicReference<List<Entry>> delivered = new AtomicReference<>();
        try {
            cache.asyncReadEntry(readHandle, 0, 1, NO_MAX_SIZE_LIMIT, () -> 2,
                    new AsyncCallbacks.ReadEntriesCallback() {
                        @Override
                        public void readEntriesComplete(List<Entry> entries, Object ctx) {
                            successes.incrementAndGet();
                            delivered.set(entries);
                            throw new IllegalStateException("caller callback failed");
                        }

                        @Override
                        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                            failures.incrementAndGet();
                        }
                    }, null);

            assertThat(successes).hasValue(1);
            assertThat(failures).hasValue(0);
            assertThat(delivered.get()).hasSize(2);
            assertThat(buffers).allSatisfy(buffer -> assertThat(buffer.refCnt()).isEqualTo(1));
            delivered.getAndSet(null).forEach(Entry::release);
            assertThat(buffers).allSatisfy(buffer -> assertThat(buffer.refCnt()).isZero());
        } finally {
            List<Entry> remaining = delivered.get();
            if (remaining != null) {
                remaining.forEach(Entry::release);
            }
            releaseRemainingBuffers();
        }
    }

}
