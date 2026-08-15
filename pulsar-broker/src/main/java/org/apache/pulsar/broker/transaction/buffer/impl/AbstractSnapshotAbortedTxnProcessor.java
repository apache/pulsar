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
 * Coordinates snapshot recovery and resource closure so resources remain available until recovery stops.
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
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    protected AbstractSnapshotAbortedTxnProcessor(ScheduledExecutorService recoveryExecutor) {
        this.recoveryExecutor = recoveryExecutor;
    }

    @Override
    public final synchronized CompletableFuture<Position> recoverFromSnapshot() {
        if (this.state == State.CLOSED) {
            return CompletableFuture.failedFuture(closedException());
        }
        if (!recoveryFuture.isDone()) {
            return recoveryFuture.copy();
        }
        // Bind the task to this recovery attempt so a later retry cannot complete the wrong future.
        CompletableFuture<Position> future = new CompletableFuture<>();
        this.recoveryFuture = future;
        this.state = State.RECOVERY_QUEUED;
        try {
            this.recoveryTask = recoveryExecutor.submit(() -> runRecovery(future));
        } catch (RejectedExecutionException e) {
            this.state = State.RECOVERY_FINISHED;
            future.completeExceptionally(e);
        }
        // Do not expose the internal future used to determine when recovery has stopped.
        return future.copy();
    }

    private void runRecovery(CompletableFuture<Position> future) {
        try {
            if (!tryStartRecovery()) {
                future.completeExceptionally(closedException());
                return;
            }
            Position recoveredPosition = doRecoverFromSnapshot(recoveryExecutor);
            if (tryMarkRecoveryFinished()) {
                future.complete(recoveredPosition);
            } else {
                future.completeExceptionally(closedException());
            }
        } catch (Throwable throwable) {
            future.completeExceptionally(tryMarkRecoveryFinished() ? throwable : closedException());
        }
    }

    /**
     * Recovers the processor state synchronously on the recovery executor thread.
     *
     * @param executor executor used by recovery-related operations
     * @return the recovery position, or {@code null} when no snapshot exists
     */
    protected abstract Position doRecoverFromSnapshot(ScheduledExecutorService executor) throws Exception;

    protected final boolean isClosed() {
        return this.state == State.CLOSED;
    }

    private synchronized boolean tryStartRecovery() {
        if (this.state == State.CLOSED) {
            return false;
        }
        this.state = State.RECOVERY_RUNNING;
        return true;
    }

    private synchronized boolean tryMarkRecoveryFinished() {
        if (this.state == State.CLOSED) {
            return false;
        }
        this.state = State.RECOVERY_FINISHED;
        return true;
    }

    @Override
    public final CompletableFuture<Void> closeAsync() {
        boolean recoveryOwnsFutureCompletion;
        CompletableFuture<Position> currentRecoveryFuture;
        synchronized (this) {
            if (this.state == State.CLOSED) {
                return closeFuture;
            }
            State previousState = this.state;
            this.state = State.CLOSED;
            recoveryOwnsFutureCompletion = previousState == State.RECOVERY_RUNNING
                    || previousState == State.RECOVERY_FINISHED;
            currentRecoveryFuture = this.recoveryFuture;
            if (this.recoveryTask != null && previousState == State.RECOVERY_QUEUED) {
                this.recoveryTask.cancel(false);
            }
        }
        if (!recoveryOwnsFutureCompletion) {
            currentRecoveryFuture.completeExceptionally(closedException());
        }
        currentRecoveryFuture.handle((v, throwable) -> null)
                .thenCompose(v -> closeResources())
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        closeFuture.completeExceptionally(throwable);
                    } else {
                        closeFuture.complete(null);
                    }
                });
        return closeFuture;
    }

    /**
     * Closes resources after recovery has stopped.
     *
     * <p>This method may be invoked on the caller of {@link #closeAsync()} or on the recovery executor thread.
     * Implementations must not rely on thread affinity and should return without blocking.
     */
    protected abstract CompletableFuture<Void> closeResources();

    private static BrokerServiceException closedException() {
        return new BrokerServiceException.ServiceUnitNotReadyException(
                "Transaction buffer snapshot processor is closed");
    }
}
