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
package org.apache.pulsar.broker.transaction.buffer.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;

/**
 * Coordinates snapshot recovery and resource closure so resources remain available until processor-owned recovery
 * work finishes.
 */
abstract class AbstractSnapshotAbortedTxnProcessor implements AbortedTxnProcessor {

    private enum State {
        OPEN,
        RECOVERY_QUEUED,
        RECOVERY_RUNNING,
        RECOVERY_FINISHED,
        CLOSED
    }

    private final ScheduledExecutorService recoveryExecutor;

    private volatile State state = State.OPEN;
    private Future<?> recoveryTask;
    private CompletableFuture<Position> recoveryFuture = CompletableFuture.completedFuture(null);
    // Completes before the recovery result, so close waits only for processor-owned work.
    private CompletableFuture<Void> recoveryWorkFinishedFuture = CompletableFuture.completedFuture(null);
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    AbstractSnapshotAbortedTxnProcessor(ScheduledExecutorService recoveryExecutor) {
        this.recoveryExecutor = recoveryExecutor;
    }

    @Override
    public CompletableFuture<Position> recoverFromSnapshot() {
        CompletableFuture<Position> newRecoveryFuture;
        CompletableFuture<Void> newRecoveryWorkFinishedFuture;
        RejectedExecutionException submissionFailure = null;
        synchronized (this) {
            if (this.state == State.CLOSED) {
                return CompletableFuture.failedFuture(closedException());
            }
            if (!recoveryFuture.isDone()) {
                return recoveryFuture.copy();
            }
            // Bind the task to this recovery attempt so a later retry cannot complete the wrong future.
            newRecoveryFuture = new CompletableFuture<>();
            newRecoveryWorkFinishedFuture = new CompletableFuture<>();
            this.recoveryFuture = newRecoveryFuture;
            this.recoveryWorkFinishedFuture = newRecoveryWorkFinishedFuture;
            this.state = State.RECOVERY_QUEUED;
            try {
                this.recoveryTask = recoveryExecutor.submit(
                        () -> runRecovery(newRecoveryFuture, newRecoveryWorkFinishedFuture));
            } catch (RejectedExecutionException e) {
                this.state = State.RECOVERY_FINISHED;
                submissionFailure = e;
            }
        }
        if (submissionFailure != null) {
            newRecoveryWorkFinishedFuture.complete(null);
            newRecoveryFuture.completeExceptionally(submissionFailure);
        }
        // Do not let callers complete the internal recovery result.
        return newRecoveryFuture.copy();
    }

    private void runRecovery(CompletableFuture<Position> recoveryResult,
                             CompletableFuture<Void> recoveryWorkFinished) {
        Position recoveredPosition = null;
        Throwable recoveryFailure = null;
        boolean closeWon = false;
        try {
            if (!tryStartRecovery()) {
                closeWon = true;
            } else {
                recoveredPosition = doRecoverFromSnapshot(recoveryExecutor);
                if (!tryMarkRecoveryFinished()) {
                    closeWon = true;
                }
            }
        } catch (Throwable throwable) {
            if (tryMarkRecoveryFinished()) {
                recoveryFailure = throwable;
            } else {
                closeWon = true;
            }
        }
        recoveryWorkFinished.complete(null);
        if (closeWon) {
            failRecoveryAfterClose(recoveryResult);
        } else if (recoveryFailure == null) {
            recoveryResult.complete(recoveredPosition);
        } else {
            recoveryResult.completeExceptionally(recoveryFailure);
        }
    }

    /**
     * Recovers the processor state synchronously on the recovery executor thread.
     * If closure wins while this method is running, its result is discarded and recovery fails as closed.
     *
     * @param executor executor used by recovery-related operations
     * @return the recovery position, or {@code null} when no snapshot exists
     */
    abstract Position doRecoverFromSnapshot(ScheduledExecutorService executor) throws Exception;

    final boolean isClosed() {
        return this.state == State.CLOSED;
    }

    private synchronized boolean tryStartRecovery() {
        if (this.state != State.RECOVERY_QUEUED) {
            return false;
        }
        this.state = State.RECOVERY_RUNNING;
        return true;
    }

    private synchronized boolean tryMarkRecoveryFinished() {
        if (this.state != State.RECOVERY_RUNNING) {
            return false;
        }
        this.state = State.RECOVERY_FINISHED;
        return true;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        State previousState;
        CompletableFuture<Position> currentRecoveryFuture;
        CompletableFuture<Void> currentRecoveryWorkFinishedFuture;
        synchronized (this) {
            if (this.state == State.CLOSED) {
                return closeFuture;
            }
            previousState = this.state;
            this.state = State.CLOSED;
            currentRecoveryFuture = this.recoveryFuture;
            currentRecoveryWorkFinishedFuture = this.recoveryWorkFinishedFuture;
            if (this.recoveryTask != null && previousState == State.RECOVERY_QUEUED) {
                this.recoveryTask.cancel(false);
            }
        }
        currentRecoveryWorkFinishedFuture.thenCompose(v -> closeResources())
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        closeFuture.completeExceptionally(throwable);
                    } else {
                        closeFuture.complete(null);
                    }
                });
        if (previousState == State.RECOVERY_QUEUED) {
            failRecoveryAfterClose(currentRecoveryFuture);
        }
        if (previousState != State.RECOVERY_RUNNING) {
            currentRecoveryWorkFinishedFuture.complete(null);
        }
        return closeFuture;
    }

    private void failRecoveryAfterClose(CompletableFuture<Position> recoveryResult) {
        closeFuture.whenComplete((__, ___) -> recoveryResult.completeExceptionally(closedException()));
    }

    /**
     * Closes resources after processor-owned recovery work has finished.
     *
     * <p>This method may be invoked on the caller of {@link #closeAsync()} or on the recovery executor thread.
     * Implementations must not rely on thread affinity and should return without blocking.
     */
    abstract CompletableFuture<Void> closeResources();

    private static BrokerServiceException closedException() {
        return new BrokerServiceException.ServiceUnitNotReadyException(
                "Transaction buffer snapshot processor is closed");
    }
}
