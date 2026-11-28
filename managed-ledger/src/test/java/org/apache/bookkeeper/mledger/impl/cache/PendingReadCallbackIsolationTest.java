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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.ThreadBoundExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.assertj.core.api.SoftAssertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PendingReadCallbackIsolationTest {
    @DataProvider
    public Object[][] completionCallbacks() {
        return new Object[][] {{false, false}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "completionCallbacks")
    public void completionCallbackFailureDoesNotSuppressOtherListenersOrReleaseTheirEntries(
            boolean throwInFirstCallback, boolean keepFirstEntries) {
        Fixture fixture = new Fixture();
        CompletableFuture<List<Entry>> storage = fixture.storageRead();
        AtomicInteger firstCalls = new AtomicInteger();
        AtomicInteger secondCalls = new AtomicInteger();
        List<Entry> keptEntries = new ArrayList<>();
        Object firstContext = new Object();
        Object secondContext = new Object();
        List<Object> receivedContexts = new ArrayList<>();
        fixture.read(successCallback(entries -> {
            firstCalls.incrementAndGet();
            if (keepFirstEntries) {
                keptEntries.addAll(entries);
            } else {
                entries.forEach(Entry::release);
            }
            if (throwInFirstCallback) {
                throw new IllegalStateException("Injected completion callback failure");
            }
        }, receivedContexts), firstContext);
        fixture.read(successCallback(entries -> {
            secondCalls.incrementAndGet();
            entries.forEach(Entry::release);
        }, receivedContexts), secondContext);
        EntryImpl storageEntry = EntryImpl.create(17, 1, new byte[] {7});
        ByteBuf storageBuffer = storageEntry.getDataBuffer().retain();
        try {
            storage.complete(List.of(storageEntry));
            SoftAssertions softly = new SoftAssertions();
            softly.assertThat(firstCalls.get()).as("First listener is notified once").isEqualTo(1);
            softly.assertThat(secondCalls.get()).as("Later listener is notified despite a callback exception")
                    .isEqualTo(1);
            softly.assertThat(receivedContexts).containsExactly(firstContext, secondContext);
            softly.assertThat(storageBuffer.refCnt())
                    .as("Only the test probe and entries kept by the callback retain the storage buffer")
                    .isEqualTo(keepFirstEntries ? 2 : 1);
            if (keepFirstEntries) {
                softly.assertThat(keptEntries).hasSize(1);
                softly.assertThat(((EntryImpl) keptEntries.getFirst()).refCnt())
                        .as("A throwing callback retains ownership of its delivered copy").isEqualTo(1);
                softly.assertThat(keptEntries.getFirst().getData()).containsExactly((byte) 7);
            }
            softly.assertAll();
        } finally {
            // Release only ownership held by the callback and the test's retained probe.
            keptEntries.forEach(entry -> {
                if (((EntryImpl) entry).refCnt() > 0) {
                    entry.release();
                }
            });
            storageBuffer.release();
        }
    }

    @DataProvider
    public Object[][] callbackFailures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "callbackFailures")
    public void failureCallbackExceptionDoesNotSuppressOtherListeners(boolean throwInFirstCallback) {
        Fixture fixture = new Fixture();
        CompletableFuture<List<Entry>> storage = fixture.storageRead();
        List<ManagedLedgerException> firstFailures = new ArrayList<>();
        List<ManagedLedgerException> secondFailures = new ArrayList<>();
        fixture.read(failureCallback(firstFailures, throwInFirstCallback), null);
        fixture.read(failureCallback(secondFailures, false), null);
        ManagedLedgerException storageFailure = new ManagedLedgerException("Injected storage failure");
        storage.completeExceptionally(storageFailure);
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(firstFailures).as("First listener receives the storage failure once")
                .containsExactly(storageFailure);
        softly.assertThat(secondFailures).as("Later listener receives the same original storage failure")
                .containsExactly(storageFailure);
        softly.assertAll();
    }

    @Test(dataProvider = "callbackFailures")
    public void partialReadFailureReleasesSuccessfulRangesWhenTheFailureCallbackThrows(boolean failOnLeft) {
        Fixture fixture = new Fixture();
        CompletableFuture<List<Entry>> middle = fixture.storageRead();
        long failedEntryId = failOnLeft ? 0 : 2;
        long successfulEntryId = failOnLeft ? 2 : 0;
        EntryImpl sideEntry = EntryImpl.create(17, successfulEntryId, new byte[] {3});
        EntryImpl middleEntry = EntryImpl.create(17, 1, new byte[] {4});
        ByteBuf sideBuffer = sideEntry.getDataBuffer().retain();
        ByteBuf middleBuffer = middleEntry.getDataBuffer().retain();
        ManagedLedgerException storageFailure = new ManagedLedgerException("Injected missing-range failure");
        AtomicInteger initialSuccess = new AtomicInteger();
        List<ManagedLedgerException> failures = new ArrayList<>();
        doAnswer(invocation -> {
            long first = invocation.getArgument(1);
            ReadEntriesCallback callback = invocation.getArgument(5);
            if (first == failedEntryId) {
                callback.readEntriesFailed(storageFailure, null);
            } else {
                callback.readEntriesComplete(List.of(sideEntry), null);
            }
            return null;
        }).when(fixture.cache).asyncReadEntry0(any(), anyLong(), anyLong(), anyLong(),
                any(), any(), any(), anyBoolean());
        try {
            fixture.read(successCallback(entries -> {
                initialSuccess.incrementAndGet();
                entries.forEach(Entry::release);
            }, new ArrayList<>()), null);
            fixture.pending.readEntries(fixture.handle, 0, 2, NO_MAX_SIZE_LIMIT,
                    () -> EntryImpl.NO_EXPECTED_READ_COUNT, failureCallback(failures, true), null);
            middle.complete(List.of(middleEntry));
            SoftAssertions softly = new SoftAssertions();
            softly.assertThat(initialSuccess.get()).as("The original middle-range listener succeeds once")
                    .isEqualTo(1);
            softly.assertThat(failures).as("The overlapping listener receives the missing-range failure once")
                    .containsExactly(storageFailure);
            softly.assertThat(sideBuffer.refCnt()).as("Only the probe retains the successful side range")
                    .isEqualTo(1);
            softly.assertThat(middleBuffer.refCnt()).as("Only the probe retains the successful middle range")
                    .isEqualTo(1);
            softly.assertAll();
        } finally {
            // Product ownership is checked above; these releases drop only the test's probes.
            sideBuffer.release();
            middleBuffer.release();
        }
    }

    private static ReadEntriesCallback successCallback(Consumer<List<Entry>> onSuccess, List<Object> contexts) {
        return new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object context) {
                contexts.add(context);
                onSuccess.accept(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object context) {
                throw new AssertionError(exception);
            }
        };
    }

    private static ReadEntriesCallback failureCallback(List<ManagedLedgerException> failures,
                                                       boolean throwAfterNotification) {
        return new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object context) {
                entries.forEach(Entry::release);
                throw new AssertionError("The storage request must fail");
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object context) {
                failures.add(exception);
                if (throwAfterNotification) {
                    throw new IllegalStateException("Injected failure callback exception");
                }
            }
        };
    }

    private static final class Fixture {
        final RangeEntryCacheImpl cache = mock(RangeEntryCacheImpl.class);
        final ManagedLedgerImpl ledger = mock(ManagedLedgerImpl.class);
        final ThreadBoundExecutor executor = mock(ThreadBoundExecutor.class);
        final ReadHandle handle = mock(ReadHandle.class);
        final PendingReadsManager pending;

        Fixture() {
            doAnswer(invocation -> {
                invocation.<Runnable>getArgument(0).run();
                return null;
            }).when(executor).executeOrRun(any(Runnable.class));
            when(ledger.getExecutor()).thenReturn(executor);
            when(cache.getManagedLedger()).thenReturn(ledger);
            when(handle.getId()).thenReturn(17L);
            pending = new PendingReadsManager(cache);
        }

        CompletableFuture<List<Entry>> storageRead() {
            CompletableFuture<List<Entry>> result = new CompletableFuture<>();
            when(cache.readFromStorage(eq(handle), eq(1L), eq(1L), anyLong(), any())).thenReturn(result);
            return result;
        }

        void read(ReadEntriesCallback callback, Object context) {
            pending.readEntries(handle, 1, 1, NO_MAX_SIZE_LIMIT,
                    () -> EntryImpl.NO_EXPECTED_READ_COUNT, callback, context);
        }
    }
}
