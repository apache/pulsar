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
package org.apache.pulsar.broker.transaction.timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TransactionTimeoutTrackerImplTest {
    private Timer timer;
    private Clock clock;
    private TransactionMetadataStoreService service;
    private List<Timeout> timeouts;
    private List<Long> delays;
    private TransactionTimeoutTrackerImpl tracker;

    @BeforeMethod
    public void setUp() {
        timer = mock(Timer.class);
        clock = mock(Clock.class);
        service = mock(TransactionMetadataStoreService.class);
        timeouts = new ArrayList<>();
        delays = new ArrayList<>();
        when(clock.millis()).thenReturn(1000L);
        when(timer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
            Timeout timeout = mock(Timeout.class);
            timeouts.add(timeout);
            delays.add(invocation.<TimeUnit>getArgument(2).toMillis(invocation.getArgument(1)));
            return timeout;
        });
        tracker = new TransactionTimeoutTrackerImpl(1, timer, 100, service, clock);
    }

    @Test
    public void testRemoveRootAndLastTransaction() {
        tracker.addTransaction(1, 1000);
        tracker.addTransaction(2, 2000);
        tracker.removeTransaction(1);
        tracker.removeTransaction(1);
        assertThat(tracker.getTrackedTransactionCount()).isEqualTo(1);
        // Keep the earlier timer until it fires, rather than rescheduling for each completed root.
        assertThat(timeouts).hasSize(1);
        when(clock.millis()).thenReturn(2000L);
        tracker.run(timeouts.get(0));
        assertThat(timeouts).hasSize(2);
        assertThat(delays.get(1)).isEqualTo(1000);
        verifyNoInteractions(service);

        tracker.removeTransaction(2);
        assertThat(tracker.getTrackedTransactionCount()).isZero();
        verify(timeouts.get(1), never()).cancel();
        tracker.run(timeouts.get(1));
        assertThat(timeouts).hasSize(2);

        tracker.addTransaction(3, 1000);
        assertThat(timeouts).hasSize(3);
        when(clock.millis()).thenReturn(3000L);
        tracker.run(timeouts.get(2));
        verify(service).endTransactionForTimeout(new TxnID(1, 3));
        assertThat(tracker.getTrackedTransactionCount()).isZero();
    }

    @Test
    public void testEmptyQueueReusesPendingTimer() {
        tracker.addTransaction(1, 1000);
        tracker.removeTransaction(1);
        assertThat(tracker.getTrackedTransactionCount()).isZero();
        tracker.addTransaction(2, 2000);
        assertThat(timeouts).hasSize(1);
        verify(timeouts.get(0), never()).cancel();

        when(clock.millis()).thenReturn(2000L);
        tracker.run(timeouts.get(0));
        verifyNoInteractions(service);
        assertThat(delays).containsExactly(1000L, 1000L);
        when(clock.millis()).thenReturn(3000L);
        tracker.run(timeouts.get(1));
        verify(service).endTransactionForTimeout(new TxnID(1, 2));
    }

    @Test
    public void testReplacedCallbackCannotRescheduleOrExpireTransactions() {
        tracker.addTransaction(1, 2000);
        Timeout oldTimeout = timeouts.get(0);
        when(oldTimeout.cancel()).thenReturn(false);
        tracker.addTransaction(2, 1000);
        assertThat(delays).containsExactly(2000L, 1000L);

        when(clock.millis()).thenReturn(2000L);
        tracker.run(oldTimeout);
        verifyNoInteractions(service);
        assertThat(timeouts).hasSize(2);
        tracker.run(timeouts.get(1));
        verify(service).endTransactionForTimeout(new TxnID(1, 2));
        verify(service, never()).endTransactionForTimeout(new TxnID(1, 1));
        assertThat(delays).containsExactly(2000L, 1000L, 1000L);
    }

    @Test
    public void testAddWhileExpiredCallbackPendingKeepsEarliestDeadline() {
        tracker.addTransaction(1, 1000);
        Timeout oldTimeout = timeouts.get(0);
        when(oldTimeout.isExpired()).thenReturn(true);
        when(clock.millis()).thenReturn(2000L);
        tracker.addTransaction(2, 10000);
        assertThat(delays).containsExactly(1000L);
        tracker.run(oldTimeout);
        verify(service).endTransactionForTimeout(new TxnID(1, 1));
        verify(service, never()).endTransactionForTimeout(new TxnID(1, 2));
        assertThat(delays).containsExactly(1000L, 10000L);
    }

    @Test
    public void testTimeoutCallbackRunsAfterRemovalOutsideLock() {
        tracker.addTransaction(1, 1000);
        tracker.addTransaction(2, 1000);
        doAnswer(invocation -> {
            assertThat(Thread.holdsLock(tracker)).as("timeout callback must not hold the tracker lock").isFalse();
            assertThat(tracker.getTrackedTransactionCount()).isEqualTo(1);
            tracker.removeTransaction(1);
            tracker.removeTransaction(2);
            return null;
        }).when(service).endTransactionForTimeout(new TxnID(1, 1));
        when(clock.millis()).thenReturn(2000L);

        tracker.run(timeouts.get(0));

        verify(service).endTransactionForTimeout(new TxnID(1, 1));
        verify(service, never()).endTransactionForTimeout(new TxnID(1, 2));
        assertThat(tracker.getTrackedTransactionCount()).isZero();
        assertThat(timeouts).hasSize(1);
    }

    @Test
    public void testExpiredCallbackContinuesDrainingDuringAddAndRemove() {
        tracker.addTransaction(1, 1000);
        tracker.addTransaction(2, 1000);
        tracker.addTransaction(3, 1000);
        Timeout timeout = timeouts.get(0);
        when(timeout.isExpired()).thenReturn(true);
        doAnswer(invocation -> {
            tracker.removeTransaction(2);
            tracker.addTransaction(4, 10000);
            return null;
        }).when(service).endTransactionForTimeout(new TxnID(1, 1));
        when(clock.millis()).thenReturn(2000L);

        tracker.run(timeout);

        verify(service).endTransactionForTimeout(new TxnID(1, 1));
        verify(service).endTransactionForTimeout(new TxnID(1, 3));
        verify(service, never()).endTransactionForTimeout(new TxnID(1, 2));
        assertThat(tracker.getTrackedTransactionCount()).isEqualTo(1);
        assertThat(delays).containsExactly(1000L, 10000L);
    }

    @Test
    public void testCallbackFailureStillSchedulesRemainingTransactions() {
        tracker.addTransaction(1, 1000);
        tracker.addTransaction(2, 1000);
        doAnswer(invocation -> {
            throw new IllegalStateException("timeout callback failed");
        }).when(service).endTransactionForTimeout(new TxnID(1, 1));
        when(clock.millis()).thenReturn(2000L);

        assertThatThrownBy(() -> tracker.run(timeouts.get(0))).isInstanceOf(IllegalStateException.class);

        assertThat(delays).containsExactly(1000L, 0L);
        tracker.run(timeouts.get(1));
        verify(service).endTransactionForTimeout(new TxnID(1, 2));
        assertThat(tracker.getTrackedTransactionCount()).isZero();
    }

    @Test
    public void testReplayStartAndClose() {
        tracker.replayAddTransaction(0, 500);
        tracker.replayAddTransaction(1, 2000);
        verifyNoInteractions(timer);
        tracker.start();
        assertThat(delays).containsExactly(0L);
        tracker.run(timeouts.get(0));
        verify(service).endTransactionForTimeout(new TxnID(1, 0));
        assertThat(delays).containsExactly(0L, 1000L);

        tracker.close();
        tracker.close();
        verify(timeouts.get(1)).cancel();
        tracker.run(timeouts.get(1));
        tracker.replayAddTransaction(2, 3000);
        tracker.addTransaction(3, 1000);
        tracker.addTransaction(4, 1);
        tracker.removeTransaction(1);
        tracker.start();
        assertThat(tracker.getTrackedTransactionCount()).isZero();
        assertThat(timeouts).hasSize(2);
        verify(service, never()).endTransactionForTimeout(new TxnID(1, 1));
        verify(service, never()).endTransactionForTimeout(new TxnID(1, 4));
    }

    @Test
    public void testShortTimeoutRunsOutsideLock() {
        doAnswer(invocation -> {
            assertThat(Thread.holdsLock(tracker)).isFalse();
            return null;
        }).when(service).endTransactionForTimeout(new TxnID(1, 1));
        tracker.addTransaction(1, 99);
        verify(service).endTransactionForTimeout(new TxnID(1, 1));
        verifyNoInteractions(timer);
        assertThat(tracker.getTrackedTransactionCount()).isZero();
    }

    @Test
    public void testCallbackCanReplaceOtherTransactions() {
        tracker.addTransaction(1, 1000);
        tracker.addTransaction(2, 1000);
        doAnswer(invocation -> {
            tracker.removeTransaction(2);
            tracker.addTransaction(3, 2000);
            return null;
        }).when(service).endTransactionForTimeout(new TxnID(1, 1));
        when(clock.millis()).thenReturn(2000L);

        tracker.run(timeouts.get(0));

        assertThat(tracker.getTrackedTransactionCount()).isEqualTo(1);
        verify(service, never()).endTransactionForTimeout(new TxnID(1, 2));
        when(clock.millis()).thenReturn(4000L);
        tracker.run(timeouts.get(1));
        verify(service).endTransactionForTimeout(new TxnID(1, 3));
    }

    @Test
    public void testCallbackCanCloseTracker() {
        tracker.addTransaction(1, 1000);
        tracker.addTransaction(2, 1000);
        doAnswer(invocation -> {
            tracker.close();
            return null;
        }).when(service).endTransactionForTimeout(new TxnID(1, 1));
        when(clock.millis()).thenReturn(2000L);

        tracker.run(timeouts.get(0));

        assertThat(tracker.getTrackedTransactionCount()).isZero();
        verify(service, never()).endTransactionForTimeout(new TxnID(1, 2));
        assertThat(timeouts).hasSize(1);
    }
}
