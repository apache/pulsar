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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
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
@Slf4j
@Getter
public class TransactionImpl implements Transaction , TimerTask {

    private final PulsarClientImpl client;
    private final long transactionTimeoutMs;
    private final long txnIdLeastBits;
    private final long txnIdMostBits;

    private final TxnID txnId;

    private final Map<String, CompletableFuture<Void>> registerPartitionMap;
    private final Map<Pair<String, String>, CompletableFuture<Void>> registerSubscriptionMap;
    private final TransactionCoordinatorClientImpl tcClient;

    private CompletableFuture<Void> opFuture;

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
        STATE_UPDATE.compareAndSet(this, State.OPEN, State.TIME_OUT);
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
        this.tcClient = client.getTcClient();
        this.opFuture = CompletableFuture.completedFuture(null);
        this.timeout = client.getTimer().newTimeout(this, transactionTimeoutMs, TimeUnit.MILLISECONDS);

    }

    // register the topics that will be modified by this transaction
    public CompletableFuture<Void> registerProducedTopic(String topic) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        if (checkIfOpen(completableFuture)) {
            synchronized (TransactionImpl.this) {
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
                log.error("The transaction [{}:{}] get an exception when send messages.",
                        txnIdMostBits, txnIdLeastBits, e);
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
            synchronized (TransactionImpl.this) {
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
                log.error("The transaction [{}:{}] get an exception when ack messages.",
                        txnIdMostBits, txnIdLeastBits, e);
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

    @Override
    public CompletableFuture<Void> commit() {
        timeout.cancel();
        return checkIfOpenOrCommitting().thenCompose((value) -> {
            CompletableFuture<Void> commitFuture = new CompletableFuture<>();
            this.state = State.COMMITTING;
            opFuture.whenComplete((v, e) -> {
                if (hasOpsFailed) {
                    abort().whenComplete((vx, ex) -> commitFuture.completeExceptionally(new PulsarClientException
                            .TransactionHasOperationFailedException()));
                } else {
                    tcClient.commitAsync(txnId)
                            .whenComplete((vx, ex) -> {
                                if (ex != null) {
                                    if (ex instanceof TransactionNotFoundException
                                            || ex instanceof InvalidTxnStatusException) {
                                        this.state = State.ERROR;
                                    }
                                    commitFuture.completeExceptionally(ex);
                                } else {
                                    this.state = State.COMMITTED;
                                    commitFuture.complete(vx);
                                }
                            });
                }
            });
            return commitFuture;
        });
    }

    @Override
    public CompletableFuture<Void> abort() {
        timeout.cancel();
        return checkIfOpenOrAborting().thenCompose(value -> {
            CompletableFuture<Void> abortFuture = new CompletableFuture<>();
            this.state = State.ABORTING;
            opFuture.whenComplete((v, e) -> {
                tcClient.abortAsync(txnId).whenComplete((vx, ex) -> {

                    if (ex != null) {
                        if (ex instanceof TransactionNotFoundException
                                || ex instanceof InvalidTxnStatusException) {
                            this.state = State.ERROR;
                        }
                        abortFuture.completeExceptionally(ex);
                    } else {
                        this.state = State.ABORTED;
                        abortFuture.complete(null);
                    }

                });
            });

            return abortFuture;
        });
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

    private CompletableFuture<Void> checkIfOpenOrCommitting() {
        if (state == State.OPEN || state == State.COMMITTING) {
            return CompletableFuture.completedFuture(null);
        } else {
            return invalidTxnStatusFuture();
        }
    }

    private CompletableFuture<Void> checkIfOpenOrAborting() {
        if (state == State.OPEN || state == State.ABORTING) {
            return CompletableFuture.completedFuture(null);
        } else {
            return invalidTxnStatusFuture();
        }
    }

    private CompletableFuture<Void> invalidTxnStatusFuture() {
        return FutureUtil.failedFuture(new InvalidTxnStatusException("[" + txnIdMostBits + ":"
                + txnIdLeastBits + "] with unexpected state : "
                + state.name() + ", expect " + State.OPEN + " state!"));
    }
}
