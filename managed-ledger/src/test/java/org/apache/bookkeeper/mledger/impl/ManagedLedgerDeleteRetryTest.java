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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedLedgerDeleteRetryTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 15_000)
    public void testOriginalFutureCompletesWhenRetrySchedulingIsRejected() throws Exception {
        LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.CRC32C, new byte[0]);
        long ledgerId = handle.getId();
        handle.close();
        OrderedScheduler retryScheduler = OrderedScheduler.newSchedulerBuilder()
                .numThreads(1).name("delete-retry-rejected").build();
        try {
            ManagedLedgerImpl ledger = new ManagedLedgerImpl(factory, bkc, factory.getMetaStore(),
                    new ManagedLedgerConfig(), retryScheduler, "delete-retry-rejected");
            retryScheduler.shutdownNow();
            bkc.failNow(BKException.Code.WriteException);

            CompletableFuture<Void> result = ledger.asyncDeleteLedger(ledgerId,
                    ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_RETRIES);

            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(RejectedExecutionException.class);
            assertThat(bkc.getLedgers()).contains(ledgerId);
        } finally {
            retryScheduler.shutdownNow();
        }
    }

    @DataProvider(name = "deleteFailures")
    public Object[][] deleteFailures() {
        return new Object[][] {{0, false}, {1, false}, {2, false}, {3, false}, {1, true}};
    }

    @Test(dataProvider = "deleteFailures", timeOut = 15_000)
    public void testOriginalFutureCompletesAfterDeleteRetries(int failures, boolean deleteBeforeRetry)
            throws Exception {
        LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.CRC32C, new byte[0]);
        long ledgerId = handle.getId();
        handle.close();
        OrderedScheduler retryScheduler = mock(OrderedScheduler.class);
        AtomicInteger scheduledRetries = new AtomicInteger();
        // Execute the real retry tasks without waiting for the production backoff. Support both overloads
        // so the regression also exercises the original implementation, which schedules a Callable.
        doAnswer(invocation -> {
            scheduledRetries.incrementAndGet();
            if (deleteBeforeRetry) {
                // The first attempt failed, but the ledger is gone when the retry runs:
                // PulsarMockBookKeeper will return NoSuchLedgerExistsException.
                bkc.deleteLedger(ledgerId);
            }
            Runnable task = invocation.getArgument(0);
            return executor.schedule(task, 0, TimeUnit.SECONDS);
        }).when(retryScheduler).schedule(any(Runnable.class),
                eq((long) ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_BACKOFF_TIME_SEC), eq(TimeUnit.SECONDS));
        doAnswer(invocation -> {
            scheduledRetries.incrementAndGet();
            if (deleteBeforeRetry) {
                bkc.deleteLedger(ledgerId);
            }
            Callable<?> task = invocation.getArgument(0);
            return executor.schedule(task, 0, TimeUnit.SECONDS);
        }).when(retryScheduler).schedule(ArgumentMatchers.<Callable<?>>any(),
                eq((long) ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_BACKOFF_TIME_SEC), eq(TimeUnit.SECONDS));

        ManagedLedgerImpl ledger = new ManagedLedgerImpl(factory, bkc, factory.getMetaStore(),
                new ManagedLedgerConfig(), retryScheduler, "delete-retry");
        for (int i = 0; i < failures; i++) {
            bkc.failAfter(i, BKException.Code.WriteException);
        }

        CompletableFuture<Void> result = ledger.asyncDeleteLedger(ledgerId,
                ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_RETRIES);
        if (failures == ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_RETRIES) {
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(BKException.BKWriteException.class);
            assertThat(bkc.getLedgers()).contains(ledgerId);
        } else {
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(bkc.getLedgers()).doesNotContain(ledgerId));
            result.get(5, TimeUnit.SECONDS);
        }
        assertThat(scheduledRetries.get()).isEqualTo(
                Math.min(failures, ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_RETRIES - 1));
    }
}
