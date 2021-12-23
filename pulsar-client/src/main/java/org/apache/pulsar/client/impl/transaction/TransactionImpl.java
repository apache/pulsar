/**
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

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import com.google.common.collect.Lists;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.InvalidTxnStatusException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.TransactionNotFoundException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.ConsumerImpl;
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

    private final Map<String, CompletableFuture<Void>> registerPartitionMap;
    private final Map<Pair<String, String>, CompletableFuture<Void>> registerSubscriptionMap;
    private final TransactionCoordinatorClientImpl tcClient;
    private Map<ConsumerImpl<?>, Integer> cumulativeAckConsumers;

    /**
     *  The number of operations are executing  in this transaction.
     */
    private final AtomicLong opsExecutingInTxn = new AtomicLong(0);
    /**
     *  The latest execution status can be obtained here.
     */
    private volatile CompletableFuture<Void> executedFuture = new CompletableFuture<>();
    private volatile State state;
    private static final AtomicReferenceFieldUpdater<TransactionImpl, State> STATE_UPDATE =
        AtomicReferenceFieldUpdater.newUpdater(TransactionImpl.class, State.class, "state");

    @Override
    public void run(Timeout timeout) throws Exception {
        STATE_UPDATE.compareAndSet(this, State.OPEN, State.TIMEOUT);
    }

    public enum State {
        OPEN,
        COMMITTING,
        ABORTING,
        COMMITTED,
        ABORTED,
        ERROR,
        TIMEOUT
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

        this.registerPartitionMap = new ConcurrentHashMap<>();
        this.registerSubscriptionMap = new ConcurrentHashMap<>();
        this.tcClient = client.getTcClient();

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
                                new TxnID(txnIdMostBits, txnIdLeastBits), Lists.newArrayList(topic))
                                .thenCompose(ignored -> CompletableFuture.completedFuture(null));
                    }
                });
            }
        } else {
            return completableFuture;
        }
    }

    public synchronized void registerSendOp(CompletableFuture<MessageId> sendFuture) {
        registerInternal(sendFuture);
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
                                new TxnID(txnIdMostBits, txnIdLeastBits), topic, subscription)
                                .thenCompose(ignored -> CompletableFuture.completedFuture(null));
                    }
                });
            }
        } else {
            return completableFuture;
        }
    }

    public synchronized <T> void registerInternal(CompletableFuture<T> completableFuture) {
        //There have been an exception, it means this transaction should not be commit,
        //so there is no need to record the execution of other operations
        if (!executedFuture.isCompletedExceptionally()) {
            opsExecutingInTxn.incrementAndGet();
            executedFuture = new CompletableFuture<>();
            completableFuture.thenRun(() -> {
                synchronized (this) {
                    opsExecutingInTxn.decrementAndGet();
                    // This is the last operation so far.
                    if (opsExecutingInTxn.get() == 0) {
                        executedFuture.complete(null);
                    }
                }
            }).exceptionally(e -> {
                //Complete this future exceptionally and there is no need to executed this method again.
                synchronized (this) {
                    executedFuture.completeExceptionally(new PulsarClientException
                            .TransactionCanNotCommitException(txnIdMostBits, txnIdLeastBits, e.getCause()));
                }
                return null;
            });
        }
    }
    public synchronized void registerAckOp(CompletableFuture<Void> ackFuture) {
       registerInternal(ackFuture);
    }

    public synchronized void registerCumulativeAckConsumer(ConsumerImpl<?> consumer) {
        if (this.cumulativeAckConsumers == null) {
            this.cumulativeAckConsumers = new HashMap<>();
        }
        cumulativeAckConsumers.put(consumer, 0);
    }

    @Override
    public CompletableFuture<Void> commit() {
        return checkIfOpenOrCommitting().thenCompose((value) -> {
            CompletableFuture<Void> commitFuture = new CompletableFuture<>();
            executedFuture.thenRun(() -> {
                this.state = State.COMMITTING;
                tcClient.commitAsync(new TxnID(txnIdMostBits, txnIdLeastBits))
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
            }).exceptionally(e -> {
                commitFuture.completeExceptionally(new PulsarClientException
                        .TransactionCanNotCommitException(this.txnIdMostBits, this.txnIdLeastBits, e));
                return null;
            });
            return commitFuture;
        });
    }

    @Override
    public CompletableFuture<Void> abort() {
        return checkIfOpenOrAborting().thenCompose(value -> {
            CompletableFuture<Void> abortFuture = new CompletableFuture<>();
            executedFuture.thenRun(() -> {
                this.state = State.ABORTING;
                if (cumulativeAckConsumers != null) {
                    cumulativeAckConsumers.forEach((consumer, integer) ->
                            cumulativeAckConsumers
                                    .putIfAbsent(consumer, consumer.clearIncomingMessagesAndGetMessageNumber()));
                }
                tcClient.abortAsync(new TxnID(txnIdMostBits, txnIdLeastBits)).whenComplete((vx, ex) -> {
                    if (cumulativeAckConsumers != null) {
                        cumulativeAckConsumers.forEach(ConsumerImpl::increaseAvailablePermits);
                        cumulativeAckConsumers.clear();
                    }

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
        return new TxnID(txnIdMostBits, txnIdLeastBits);
    }

    public <T> boolean checkIfOpen(CompletableFuture<T> completableFuture) {
        if (state == State.OPEN) {
            return true;
        } else {
            completableFuture
                    .completeExceptionally(new InvalidTxnStatusException(
                            new TxnID(txnIdMostBits, txnIdLeastBits).toString(), state.name(), State.OPEN.name()));
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
