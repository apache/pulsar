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

import com.google.common.collect.Lists;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.AccessLevel;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.InvalidTxnStatusException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.TransactionNotFoundException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * The default implementation of {@link Transaction}.
 *
 * <p>All the error handling and retry logic are handled by this class.
 * The original pulsar client doesn't handle any transaction logic. It is only responsible
 * for sending the messages and acknowledgements carrying the transaction id and retrying on
 * failures. This decouples the transactional operations from non-transactional operations as
 * much as possible.
 */
@CustomLog
@Getter
public class TransactionImpl implements Transaction , TimerTask {

    private final PulsarClientImpl client;
    private final long transactionTimeoutMs;
    private final long txnIdLeastBits;
    private final long txnIdMostBits;

    private final TxnID txnId;

    private final Map<String, CompletableFuture<Void>> registerPartitionMap;
    private final Map<Pair<String, String>, CompletableFuture<Void>> registerSubscriptionMap;
    private final TransactionCoordinatorClient tcClient;

    private CompletableFuture<Void> opFuture;
    @Getter(AccessLevel.NONE)
    private final CompletableFuture<State> terminalFuture = new CompletableFuture<>();
    @Getter(AccessLevel.NONE)
    private final Set<CompletableFuture<Void>> commitActions = ConcurrentHashMap.newKeySet();
    @Getter(AccessLevel.NONE)
    private final Object completionLock = new Object();
    @Getter(AccessLevel.NONE)
    private CompletableFuture<Void> pendingCommit;

    private volatile long opCount = 0L;
    private static final AtomicLongFieldUpdater<TransactionImpl> OP_COUNT_UPDATE =
            AtomicLongFieldUpdater.newUpdater(TransactionImpl.class, "opCount");


    private volatile State state;
    private static final AtomicReferenceFieldUpdater<TransactionImpl, State> STATE_UPDATE =
        AtomicReferenceFieldUpdater.newUpdater(TransactionImpl.class, State.class, "state");

    private volatile boolean hasOpsFailed = false;
    private final Timeout timeout;

    @Override
    public void run(Timeout timeout) throws Exception {
        if (STATE_UPDATE.compareAndSet(this, State.OPEN, State.TIME_OUT)) {
            completeTerminalState(State.TIME_OUT);
        }
    }

    TransactionImpl(PulsarClientImpl client,
                    long transactionTimeoutMs,
                    long txnIdLeastBits,
                    long txnIdMostBits) {
        this.state = State.OPEN;
        this.client = client;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.txnIdLeastBits = txnIdLeastBits;
        this.txnIdMostBits = txnIdMostBits;
        this.txnId = new TxnID(this.txnIdMostBits, this.txnIdLeastBits);

        this.registerPartitionMap = new ConcurrentHashMap<>();
        this.registerSubscriptionMap = new ConcurrentHashMap<>();
        this.tcClient = client.getTransactionCoordinatorClient();
        this.opFuture = CompletableFuture.completedFuture(null);
        this.timeout = client.getTimer().newTimeout(this, transactionTimeoutMs, TimeUnit.MILLISECONDS);

    }

    // register the topics that will be modified by this transaction
    public CompletableFuture<Void> registerProducedTopic(String topic) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        if (checkIfOpen(completableFuture)) {
            // we need to issue the request to TC to register the produced topic
            return registerPartitionMap.compute(topic, (key, future) -> {
                if (future != null) {
                    return future.thenCompose(ignored -> CompletableFuture.completedFuture(null));
                } else {
                    return tcClient.addPublishPartitionToTxnAsync(
                                    txnId, Lists.newArrayList(topic))
                            .thenCompose(ignored -> CompletableFuture.completedFuture(null));
                }
            });
        }
        return completableFuture;
    }

    public void registerSendOp(CompletableFuture<MessageId> newSendFuture) {
        if (OP_COUNT_UPDATE.getAndIncrement(this) == 0) {
            opFuture = new CompletableFuture<>();
        }
        // the opCount is always bigger than 0 if there is an exception,
        // and then the opFuture will never be replaced.
        newSendFuture.whenComplete((messageId, e) -> {
            if (e != null) {
                log.error().attr("txnIdMostBits", txnIdMostBits)
                        .attr("txnIdLeastBits", txnIdLeastBits)
                        .exception(e)
                        .log("The transaction got an exception when sending messages");
                if (!hasOpsFailed) {
                    hasOpsFailed = true;
                }
            }
            CompletableFuture<Void> future = opFuture;
            if (OP_COUNT_UPDATE.decrementAndGet(this) == 0) {
                future.complete(null);
            }
        });
    }

    // register the topics that will be modified by this transaction
    public CompletableFuture<Void> registerAckedTopic(String topic, String subscription) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        if (checkIfOpen(completableFuture)) {
            // we need to issue the request to TC to register the acked topic
            return registerSubscriptionMap.compute(Pair.of(topic, subscription), (key, future) -> {
                if (future != null) {
                    return future.thenCompose(ignored -> CompletableFuture.completedFuture(null));
                } else {
                    return tcClient.addSubscriptionToTxnAsync(
                                    txnId, topic, subscription)
                            .thenCompose(ignored -> CompletableFuture.completedFuture(null));
                }
            });
        }
        return completableFuture;
    }

    public void registerAckOp(CompletableFuture<Void> newAckFuture) {
        if (OP_COUNT_UPDATE.getAndIncrement(this) == 0) {
            opFuture = new CompletableFuture<>();
        }
        // the opCount is always bigger than 0 if there is an exception,
        // and then the opFuture will never be replaced.
        newAckFuture.whenComplete((ignore, e) -> {
            if (e != null) {
                log.error().attr("txnIdMostBits", txnIdMostBits)
                        .attr("txnIdLeastBits", txnIdLeastBits)
                        .exception(e)
                        .log("The transaction got an exception when acking messages");
                if (!hasOpsFailed) {
                    hasOpsFailed = true;
                }
            }
            CompletableFuture<Void> future = opFuture;
            if (OP_COUNT_UPDATE.decrementAndGet(this) == 0) {
                future.complete(null);
            }
        });
    }

    /**
     * Registers internal cleanup to run only after the coordinator confirms a successful commit.
     * Register the action before its acknowledgment operation completes so commit waits for registration.
     * Abort, timeout and terminal failures discard the action; retryable commit failures keep it pending.
     */
    public void registerCommitAction(Runnable action) {
        CompletableFuture<Void> actionFuture = terminalFuture.thenAccept(terminalState -> {
            if (terminalState == State.COMMITTED) {
                action.run();
            }
        }).exceptionally(ex -> {
            log.debug().attr("txnId", txnId).exception(ex).log("Transaction commit cleanup failed");
            return null;
        });
        commitActions.add(actionFuture);
        actionFuture.whenComplete((ignored, ex) -> commitActions.remove(actionFuture));
    }

    private CompletableFuture<Void> completeTerminalState(State terminalState) {
        terminalFuture.complete(terminalState);
        // Wait for any registered commit cleanup that is still executing.
        return FutureUtil.waitForAll(commitActions);
    }

    @Override
    public CompletableFuture<Void> commit() {
        timeout.cancel();
        CompletableFuture<Void> commitFuture;
        synchronized (completionLock) {
            // Join before checking state: a successful response may still be running commit cleanup.
            if (pendingCommit != null && !pendingCommit.isDone()) {
                return pendingCommit.thenApply(ignored -> null);
            }
            // Compete with abort and timeout for the transition out of OPEN.
            if (!STATE_UPDATE.compareAndSet(this, State.OPEN, State.COMMITTING)) {
                State actualState = state;
                if (actualState != State.COMMITTING) {
                    return unexpectedState(actualState, State.COMMITTING);
                }
            }
            commitFuture = new CompletableFuture<>();
            pendingCommit = commitFuture;
        }
        opFuture.whenComplete((v, e) -> {
            if (hasOpsFailed) {
                checkState(State.COMMITTING).thenCompose(__ -> {
                    this.state = State.ABORTING;
                    return internalAbort();
                }).whenComplete((vx, ex) -> commitFuture.completeExceptionally(
                        new PulsarClientException.TransactionHasOperationFailedException()));
            } else {
                FutureUtil.supplySafely(() -> tcClient.commitAsync(txnId))
                        .whenComplete((vx, ex) -> {
                            if (ex != null) {
                                if (ex instanceof TransactionNotFoundException
                                        || ex instanceof InvalidTxnStatusException) {
                                    this.state = State.ERROR;
                                    completeTerminalState(State.ERROR);
                                }
                                commitFuture.completeExceptionally(ex);
                            } else {
                                this.state = State.COMMITTED;
                                completeTerminalState(State.COMMITTED)
                                        .whenComplete((ignored, cleanupError) -> commitFuture.complete(vx));
                            }
                        });
            }
        });
        // Keep caller cancellation and explicit completion from ending the shared attempt.
        return commitFuture.thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<Void> abort() {
        timeout.cancel();
        if (!STATE_UPDATE.compareAndSet(this, State.OPEN, State.ABORTING)) {
            State actualState = state;
            if (actualState != State.ABORTING) {
                return unexpectedState(actualState, State.ABORTING);
            }
        }
        return internalAbort();
    }

    private CompletableFuture<Void> internalAbort() {
        CompletableFuture<Void> abortFuture = new CompletableFuture<>();
        opFuture.whenComplete((v, e) -> {
            FutureUtil.supplySafely(() -> tcClient.abortAsync(txnId)).whenComplete((vx, ex) -> {

                if (ex != null) {
                    if (ex instanceof TransactionNotFoundException
                            || ex instanceof InvalidTxnStatusException) {
                        this.state = State.ERROR;
                        completeTerminalState(State.ERROR);
                    }
                    abortFuture.completeExceptionally(ex);
                } else {
                    this.state = State.ABORTED;
                    completeTerminalState(State.ABORTED);
                    abortFuture.complete(null);
                }

            });
        });

        return abortFuture;
    }

    @Override
    public TxnID getTxnID() {
        return this.txnId;
    }

    @Override
    public State getState() {
        return state;
    }

    public <T> boolean checkIfOpen(CompletableFuture<T> completableFuture) {
        if (state == State.OPEN) {
            return true;
        } else {
            completableFuture
                    .completeExceptionally(new InvalidTxnStatusException(
                            txnId.toString(), state.name(), State.OPEN.name()));
            return false;
        }
    }

    private CompletableFuture<Void> checkState(State... expectedStates) {
        final State actualState = STATE_UPDATE.get(this);
        for (State expectedState : expectedStates) {
            if (actualState == expectedState) {
                return CompletableFuture.completedFuture(null);
            }
        }
        return unexpectedState(actualState, expectedStates);
    }

    private CompletableFuture<Void> unexpectedState(State actualState, State... expectedStates) {
        return FutureUtil.failedFuture(new InvalidTxnStatusException("[" + txnIdMostBits + ":"
                + txnIdLeastBits + "] with unexpected state: " + actualState.name() + ", expect: "
                + Arrays.toString(expectedStates)));
    }
}
