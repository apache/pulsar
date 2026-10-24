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
package org.apache.pulsar.client.impl.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction.State;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.InvalidTxnStatusException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.TransactionNotFoundException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TransactionImplTest {

    @DataProvider
    public Object[][] terminalCoordinatorFailures() {
        return new Object[][] {
                {true, new InvalidTxnStatusException("Controlled terminal commit failure")},
                {true, new TransactionNotFoundException("Controlled missing transaction during commit")},
                {false, new InvalidTxnStatusException("Controlled terminal abort failure")},
                {false, new TransactionNotFoundException("Controlled missing transaction during abort")}
        };
    }

    @Test(timeOut = 30000)
    public void testCommitActionsWaitForSuccessAndCompleteBeforeCommitFuture() throws Exception {
        Fixture fixture = new Fixture();
        AtomicInteger actions = new AtomicInteger();
        fixture.transaction.registerCommitAction(() -> {
            assertThat(fixture.transaction.getState()).isEqualTo(State.COMMITTED);
            actions.incrementAndGet();
        });
        CompletableFuture<Void> commit = fixture.transaction.commit();
        AtomicInteger actionsObservedAtCommitCompletion = new AtomicInteger(-1);
        commit.thenRun(() -> actionsObservedAtCommitCompletion.set(actions.get()));

        assertThat(commit).isNotDone();
        assertThat(fixture.transaction.getState()).isEqualTo(State.COMMITTING);
        assertThat(actions).hasValue(0);
        fixture.commitResponse.complete(null);

        commit.get(1, TimeUnit.SECONDS);
        assertThat(actions).hasValue(1);
        assertThat(actionsObservedAtCommitCompletion).hasValue(1);
        fixture.transaction.registerCommitAction(actions::incrementAndGet);
        assertThat(actions).as("late actions observe the already committed transaction").hasValue(2);
    }

    @Test(timeOut = 30000)
    public void testWrappedAckInstallsCommitActionBeforeCommitRequest() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<Void> ackReceipt = new CompletableFuture<>();
        AtomicInteger actions = new AtomicInteger();
        CompletableFuture<Void> ack = ackReceipt.thenRun(() ->
                fixture.transaction.registerCommitAction(actions::incrementAndGet));
        fixture.transaction.registerAckOp(ack);

        CompletableFuture<Void> commit = fixture.transaction.commit();
        verify(fixture.coordinator, never()).commitAsync(any());
        assertThat(commit).isNotDone();
        ackReceipt.complete(null);
        verify(fixture.coordinator).commitAsync(fixture.transaction.getTxnID());
        assertThat(actions).hasValue(0);

        fixture.commitResponse.complete(null);
        commit.get(1, TimeUnit.SECONDS);
        assertThat(actions).as("commit cannot overtake ACK completion callback registration").hasValue(1);
    }

    @Test(timeOut = 30000)
    public void testRetryableCommitFailurePreservesActionsUntilSuccessfulRetry() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<Void> retryResponse = new CompletableFuture<>();
        when(fixture.coordinator.commitAsync(fixture.transaction.getTxnID()))
                .thenReturn(fixture.commitResponse, retryResponse);
        AtomicInteger actions = new AtomicInteger();
        fixture.transaction.registerCommitAction(actions::incrementAndGet);
        CompletableFuture<Void> firstCommit = fixture.transaction.commit();
        IllegalStateException failure = new IllegalStateException("Controlled unknown commit outcome");
        fixture.commitResponse.completeExceptionally(failure);

        assertThatThrownBy(() -> firstCommit.get(1, TimeUnit.SECONDS)).hasCause(failure);
        assertThat(fixture.transaction.getState()).isEqualTo(State.COMMITTING);
        assertThat(actions).hasValue(0);
        CompletableFuture<Void> retry = fixture.transaction.commit();
        assertThat(retry).isNotDone();
        assertThat(actions).hasValue(0);
        retryResponse.complete(null);

        retry.get(1, TimeUnit.SECONDS);
        assertThat(actions).hasValue(1);
        assertThatThrownBy(() -> fixture.transaction.commit().get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(InvalidTxnStatusException.class);
        assertThat(actions).as("retrying an already committed transaction cannot repeat cleanup").hasValue(1);
        verify(fixture.coordinator, times(2)).commitAsync(fixture.transaction.getTxnID());
    }

    @Test(timeOut = 30000)
    public void testAbortDiscardsCommitActions() throws Exception {
        Fixture fixture = new Fixture();
        AtomicInteger actions = new AtomicInteger();
        fixture.transaction.registerCommitAction(actions::incrementAndGet);
        CompletableFuture<Void> abort = fixture.transaction.abort();
        assertThat(actions).hasValue(0);
        assertThat(abort).isNotDone();
        fixture.abortResponse.complete(null);

        abort.get(1, TimeUnit.SECONDS);
        assertThat(fixture.transaction.getState()).isEqualTo(State.ABORTED);
        fixture.transaction.registerCommitAction(actions::incrementAndGet);
        assertThat(actions).as("neither existing nor late actions run after abort").hasValue(0);
        verify(fixture.coordinator, never()).commitAsync(any());
    }

    @Test(timeOut = 30000)
    public void testFailedAckAbortsWithoutRunningCommitActions() throws Exception {
        Fixture fixture = new Fixture();
        AtomicInteger actions = new AtomicInteger();
        fixture.transaction.registerCommitAction(actions::incrementAndGet);
        CompletableFuture<Void> receipt = new CompletableFuture<>();
        fixture.transaction.registerAckOp(receipt.thenRun(() ->
                fixture.transaction.registerCommitAction(actions::incrementAndGet)));
        CompletableFuture<Void> commit = fixture.transaction.commit();
        receipt.completeExceptionally(new IllegalStateException("Controlled ACK receipt failure"));

        verify(fixture.coordinator, never()).commitAsync(any());
        verify(fixture.coordinator).abortAsync(fixture.transaction.getTxnID());
        assertThat(commit).isNotDone();
        fixture.abortResponse.complete(null);

        assertThatThrownBy(() -> commit.get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(PulsarClientException.TransactionHasOperationFailedException.class);
        assertThat(fixture.transaction.getState()).isEqualTo(State.ABORTED);
        fixture.transaction.registerCommitAction(actions::incrementAndGet);
        assertThat(actions).hasValue(0);
    }

    @Test(dataProvider = "terminalCoordinatorFailures", timeOut = 30000)
    public void testTerminalFailureDiscardsCommitActions(boolean commit,
                                                        TransactionCoordinatorClientException failure) {
        Fixture fixture = new Fixture();
        AtomicInteger actions = new AtomicInteger();
        fixture.transaction.registerCommitAction(actions::incrementAndGet);
        CompletableFuture<Void> operation = commit ? fixture.transaction.commit() : fixture.transaction.abort();
        CompletableFuture<Void> response = commit ? fixture.commitResponse : fixture.abortResponse;
        response.completeExceptionally(failure);

        assertThatThrownBy(() -> operation.get(1, TimeUnit.SECONDS)).hasCause(failure);
        assertThat(fixture.transaction.getState()).isEqualTo(State.ERROR);
        fixture.transaction.registerCommitAction(actions::incrementAndGet);
        assertThat(actions).as("terminal failures never imply a confirmed commit").hasValue(0);
    }

    @Test(timeOut = 30000)
    public void testTimeoutDiscardsExistingAndLateCommitActions() throws Exception {
        Fixture fixture = new Fixture();
        AtomicInteger actions = new AtomicInteger();
        fixture.transaction.registerCommitAction(actions::incrementAndGet);
        fixture.transaction.run(fixture.timeout);

        assertThat(fixture.transaction.getState()).isEqualTo(State.TIME_OUT);
        fixture.transaction.registerCommitAction(actions::incrementAndGet);
        assertThatThrownBy(() -> fixture.transaction.commit().get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(InvalidTxnStatusException.class);
        assertThat(actions).hasValue(0);
        verify(fixture.coordinator, never()).commitAsync(any());
    }

    @Test(timeOut = 30000)
    public void testLateTimeoutDoesNotDiscardActionsWhileCommitting() throws Exception {
        Fixture fixture = new Fixture();
        AtomicInteger actions = new AtomicInteger();
        fixture.transaction.registerCommitAction(actions::incrementAndGet);
        CompletableFuture<Void> commit = fixture.transaction.commit();
        fixture.transaction.run(fixture.timeout);

        assertThat(fixture.transaction.getState()).isEqualTo(State.COMMITTING);
        assertThat(actions).hasValue(0);
        fixture.commitResponse.complete(null);
        commit.get(1, TimeUnit.SECONDS);
        assertThat(actions).hasValue(1);
    }

    @Test(timeOut = 30000)
    public void testCommitActionFailureDoesNotChangeCommitResultOrSkipOtherActions() throws Exception {
        Fixture fixture = new Fixture();
        AtomicInteger actions = new AtomicInteger();
        fixture.transaction.registerCommitAction(actions::incrementAndGet);
        fixture.transaction.registerCommitAction(() -> {
            throw new IllegalStateException("Controlled cleanup failure");
        });
        fixture.transaction.registerCommitAction(actions::incrementAndGet);
        CompletableFuture<Void> commit = fixture.transaction.commit();
        fixture.commitResponse.complete(null);

        commit.get(1, TimeUnit.SECONDS);
        assertThat(fixture.transaction.getState()).isEqualTo(State.COMMITTED);
        assertThat(actions).as("one failing cleanup must not prevent other consumers from being cleaned up")
                .hasValue(2);
        assertThatCode(() -> fixture.transaction.registerCommitAction(() -> {
            throw new IllegalStateException("Controlled late cleanup failure");
        })).doesNotThrowAnyException();
    }

    @Test(timeOut = 30000)
    public void testConcurrentSuccessfulCommitsWaitForTheSameCleanup() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<Void> secondResponse = new CompletableFuture<>();
        when(fixture.coordinator.commitAsync(fixture.transaction.getTxnID()))
                .thenReturn(fixture.commitResponse, secondResponse);
        CountDownLatch actionStarted = new CountDownLatch(1);
        CountDownLatch releaseAction = new CountDownLatch(1);
        AtomicInteger actions = new AtomicInteger();
        fixture.transaction.registerCommitAction(() -> {
            actionStarted.countDown();
            try {
                assertThat(releaseAction.await(5, TimeUnit.SECONDS)).as("cleanup is released by the test").isTrue();
                actions.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        });
        CompletableFuture<Void> firstCommit = fixture.transaction.commit();
        CompletableFuture<Void> secondCommit = fixture.transaction.commit();
        ExecutorService executor =
                Executors.newSingleThreadExecutor(new DefaultThreadFactory("txn-commit-cleanup-test"));
        try {
            CompletableFuture<Void> firstResponseCompletion =
                    CompletableFuture.runAsync(() -> fixture.commitResponse.complete(null), executor);
            try {
                assertThat(actionStarted.await(5, TimeUnit.SECONDS)).as("the first response starts cleanup").isTrue();
                secondResponse.complete(null);
                assertThat(firstCommit).as("the first commit waits for its running cleanup").isNotDone();
                assertThat(secondCommit).as("another successful commit must wait for the same running cleanup")
                        .isNotDone();
                assertThat(actions).hasValue(0);
            } finally {
                releaseAction.countDown();
            }

            firstResponseCompletion.get(5, TimeUnit.SECONDS);
            firstCommit.get(1, TimeUnit.SECONDS);
            secondCommit.get(1, TimeUnit.SECONDS);
            assertThat(actions).as("concurrent commit responses cannot repeat the cleanup").hasValue(1);
            assertThat(fixture.transaction.getState()).isEqualTo(State.COMMITTED);
        } finally {
            releaseAction.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static final class Fixture {
        private final TransactionCoordinatorClient coordinator = mock(TransactionCoordinatorClient.class);
        private final Timeout timeout = mock(Timeout.class);
        private final CompletableFuture<Void> commitResponse = new CompletableFuture<>();
        private final CompletableFuture<Void> abortResponse = new CompletableFuture<>();
        private final TransactionImpl transaction;

        private Fixture() {
            PulsarClientImpl client = mock(PulsarClientImpl.class);
            Timer timer = mock(Timer.class);
            when(client.getTimer()).thenReturn(timer);
            when(client.getTransactionCoordinatorClient()).thenReturn(coordinator);
            when(timer.newTimeout(any(), anyLong(), any())).thenReturn(timeout);
            transaction = new TransactionImpl(client, 30000, 1, 2);
            when(coordinator.commitAsync(transaction.getTxnID())).thenReturn(commitResponse);
            when(coordinator.abortAsync(transaction.getTxnID())).thenReturn(abortResponse);
        }
    }
}
