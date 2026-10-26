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
package org.apache.bookkeeper.mledger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OffloadReadHandleTest {
    private final List<ScheduledTask> scheduledTasks = new ArrayList<>();
    private AtomicLong clock;
    private ScheduledExecutorService scheduler;

    @BeforeMethod
    public void setUp() {
        clock = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
        scheduledTasks.clear();
        scheduler = mock(ScheduledExecutorService.class);
        doAnswer(invocation -> {
            scheduledTasks.add(new ScheduledTask(invocation.getArgument(0), invocation.getArgument(1),
                    invocation.getArgument(2)));
            return mock(ScheduledFuture.class);
        }).when(scheduler).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testReadAsyncRechecksAfterScheduledDelayWithoutBlocking() {
        AsyncTokenBucket tokenBucket = newTokenBucket(1_000_000_000L, 0, 1);
        LedgerEntries entries = entries(1);
        ReadHandle delegate = delegateReturning(entries);
        ReadHandle handle = create(delegate, tokenBucket);

        CompletableFuture<LedgerEntries> readFuture = handle.readAsync(0, 0);

        Assert.assertFalse(readFuture.isDone());
        assertOnlyScheduledTask(1);
        runNextScheduledTask();
        Assert.assertFalse(readFuture.isDone());
        verify(delegate, never()).readAsync(anyLong(), anyLong());
        assertOnlyScheduledTask(1);

        clock.incrementAndGet();
        runNextScheduledTask();

        Assert.assertSame(readFuture.join(), entries);
        verify(delegate).readAsync(0, 0);
        verify(entries, never()).close();
        Assert.assertEquals(scheduledTasks.size(), 0);
    }

    @Test
    public void testReadAsyncConsumesActualBytesAfterSuccessfulRead() {
        AsyncTokenBucket tokenBucket = newTokenBucket(100, 1_000, 1);
        LedgerEntries entries = entries(30, 70);
        ReadHandle handle = create(delegateReturning(entries), tokenBucket);

        CompletableFuture<LedgerEntries> readFuture = handle.readAsync(0, 1);

        Assert.assertSame(readFuture.join(), entries);
        Assert.assertEquals(tokenBucket.getTokens(), 900);
        verify(entries, never()).close();
    }

    @Test
    public void testReadAsyncDoesNotConsumeTokensAfterFailedRead() {
        AsyncTokenBucket tokenBucket = newTokenBucket(100, 100, 1);
        RuntimeException readFailure = new RuntimeException("read failed");
        ReadHandle delegate = mock(ReadHandle.class);
        when(delegate.readAsync(anyLong(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(readFailure));
        ReadHandle handle = create(delegate, tokenBucket);

        CompletableFuture<LedgerEntries> readFuture = handle.readAsync(0, 0);

        CompletionException thrown = Assert.expectThrows(CompletionException.class, readFuture::join);
        Assert.assertSame(thrown.getCause(), readFailure);
        Assert.assertEquals(tokenBucket.getTokens(), 100);
    }

    @Test
    public void testReadAsyncReturnsEmptyEntriesWithoutConsumingTokens() {
        AsyncTokenBucket tokenBucket = newTokenBucket(100, 100, 1);
        LedgerEntries entries = entries();
        ReadHandle handle = create(delegateReturning(entries), tokenBucket);

        CompletableFuture<LedgerEntries> readFuture = handle.readAsync(0, -1);

        Assert.assertSame(readFuture.join(), entries);
        Assert.assertEquals(tokenBucket.getTokens(), 100);
        verify(entries, never()).close();
    }

    @Test
    public void testReadAsyncBypassesSchedulingWhenThrottlingIsDisabled() {
        LedgerEntries entries = entries(10);
        ReadHandle handle = create(delegateReturning(entries), null);

        CompletableFuture<LedgerEntries> readFuture = handle.readAsync(0, 0);

        Assert.assertSame(readFuture.join(), entries);
        verify(scheduler, never()).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        verify(entries, never()).close();
    }

    @Test
    public void testReadAsyncReturnsFailureWhenSchedulingIsRejected() {
        AsyncTokenBucket tokenBucket = newTokenBucket(100, 0, 1);
        RejectedExecutionException schedulingFailure = new RejectedExecutionException("scheduler closed");
        doThrow(schedulingFailure).when(scheduler)
                .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        ReadHandle handle = create(mock(ReadHandle.class), tokenBucket);

        CompletableFuture<LedgerEntries> readFuture = handle.readAsync(0, 0);

        CompletionException thrown = Assert.expectThrows(CompletionException.class, readFuture::join);
        Assert.assertSame(thrown.getCause(), schedulingFailure);
    }

    @Test
    public void testReadAsyncReturnsFailureWhenDelegateThrowsSynchronously() {
        AsyncTokenBucket tokenBucket = newTokenBucket(100, 100, 1);
        RuntimeException readFailure = new RuntimeException("synchronous read failure");
        ReadHandle delegate = mock(ReadHandle.class);
        when(delegate.readAsync(anyLong(), anyLong())).thenThrow(readFailure);
        ReadHandle handle = create(delegate, tokenBucket);

        CompletableFuture<LedgerEntries> readFuture = handle.readAsync(0, 0);

        CompletionException thrown = Assert.expectThrows(CompletionException.class, readFuture::join);
        Assert.assertSame(thrown.getCause(), readFailure);
        Assert.assertEquals(tokenBucket.getTokens(), 100);
    }

    private ReadHandle create(ReadHandle delegate, AsyncTokenBucket tokenBucket) {
        return OffloadReadHandle.create(delegate, tokenBucket, scheduler).join();
    }

    private AsyncTokenBucket newTokenBucket(long rate, long initialTokens, long targetTokensAfterThrottling) {
        return AsyncTokenBucket.builder()
                .clock(clock::get)
                .addTokensResolutionNanos(0)
                .initialTokens(initialTokens)
                .capacity(Math.max(1, initialTokens))
                .rate(rate)
                .targetAmountOfTokensAfterThrottling(targetTokensAfterThrottling)
                .build();
    }

    private static ReadHandle delegateReturning(LedgerEntries entries) {
        ReadHandle delegate = mock(ReadHandle.class);
        when(delegate.readAsync(anyLong(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(entries));
        return delegate;
    }

    private static LedgerEntries entries(long... lengths) {
        List<LedgerEntry> entryList = new ArrayList<>();
        for (int i = 0; i < lengths.length; i++) {
            LedgerEntry entry = mock(LedgerEntry.class);
            when(entry.getLength()).thenReturn(lengths[i]);
            entryList.add(entry);
        }
        LedgerEntries entries = mock(LedgerEntries.class);
        when(entries.iterator()).thenReturn(entryList.iterator());
        return entries;
    }

    private void assertOnlyScheduledTask(long expectedDelayNanos) {
        Assert.assertEquals(scheduledTasks.size(), 1);
        ScheduledTask task = scheduledTasks.get(0);
        Assert.assertEquals(task.delay, expectedDelayNanos);
        Assert.assertEquals(task.unit, TimeUnit.NANOSECONDS);
    }

    private void runNextScheduledTask() {
        scheduledTasks.remove(0).runnable.run();
    }

    private static final class ScheduledTask {
        private final Runnable runnable;
        private final long delay;
        private final TimeUnit unit;

        private ScheduledTask(Runnable runnable, long delay, TimeUnit unit) {
            this.runnable = runnable;
            this.delay = delay;
            this.unit = unit;
        }
    }
}
