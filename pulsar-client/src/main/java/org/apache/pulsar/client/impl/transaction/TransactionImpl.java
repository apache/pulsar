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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;

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
public class TransactionImpl implements Transaction {

    private final PulsarClientImpl client;
    private final long transactionTimeoutMs;
    private final long txnIdLeastBits;
    private final long txnIdMostBits;

    private final Map<String, CompletableFuture<Void>> registerPartitionMap;
    private final Map<String, CompletableFuture<Void>> registerSubscriptionMap;
    private final TransactionCoordinatorClientImpl tcClient;
    private Map<ConsumerImpl<?>, Integer> cumulativeAckConsumers;

    private final ArrayList<CompletableFuture<MessageId>> sendFutureList;
    private final ArrayList<CompletableFuture<Void>> ackFutureList;

    TransactionImpl(PulsarClientImpl client,
                    long transactionTimeoutMs,
                    long txnIdLeastBits,
                    long txnIdMostBits) {
        this.client = client;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.txnIdLeastBits = txnIdLeastBits;
        this.txnIdMostBits = txnIdMostBits;

        this.registerPartitionMap = new ConcurrentHashMap<>();
        this.registerSubscriptionMap = new ConcurrentHashMap<>();
        this.tcClient = client.getTcClient();

        this.sendFutureList = new ArrayList<>();
        this.ackFutureList = new ArrayList<>();
    }

    // register the topics that will be modified by this transaction
    public synchronized CompletableFuture<Void> registerProducedTopic(String topic) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
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

    public synchronized void registerSendOp(CompletableFuture<MessageId> sendFuture) {
        sendFutureList.add(sendFuture);
    }

    // register the topics that will be modified by this transaction
    public synchronized CompletableFuture<Void> registerAckedTopic(String topic, String subscription) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        // we need to issue the request to TC to register the acked topic
        return registerSubscriptionMap.compute(topic, (key, future) -> {
            if (future != null) {
                return future.thenCompose(ignored -> CompletableFuture.completedFuture(null));
            } else {
                return tcClient.addSubscriptionToTxnAsync(
                        new TxnID(txnIdMostBits, txnIdLeastBits), topic, subscription)
                        .thenCompose(ignored -> CompletableFuture.completedFuture(null));
            }
        });
    }

    public synchronized void registerAckOp(CompletableFuture<Void> ackFuture) {
        ackFutureList.add(ackFuture);
    }

    public synchronized void registerCumulativeAckConsumer(ConsumerImpl<?> consumer) {
        if (this.cumulativeAckConsumers == null) {
            this.cumulativeAckConsumers = new HashMap<>();
        }
        cumulativeAckConsumers.put(consumer, 0);
    }

    @Override
    public CompletableFuture<Void> commit() {
        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        allOpComplete().whenComplete((v, e) -> {
            if (e != null) {
                abort().whenComplete((vx, ex) -> commitFuture.completeExceptionally(e));
            } else {
                tcClient.commitAsync(new TxnID(txnIdMostBits, txnIdLeastBits))
                        .whenComplete((vx, ex) -> {
                    if (ex != null) {
                        commitFuture.completeExceptionally(ex);
                    } else {
                        commitFuture.complete(vx);
                    }
                });
            }
        });
        return commitFuture;
    }

    @Override
    public CompletableFuture<Void> abort() {
        CompletableFuture<Void> abortFuture = new CompletableFuture<>();
        allOpComplete().whenComplete((v, e) -> {
            if (e != null) {
                log.error(e.getMessage());
            }
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
                    abortFuture.completeExceptionally(ex);
                } else {
                    abortFuture.complete(null);
                }

            });
        });

        return abortFuture;
    }

    private CompletableFuture<Void> allOpComplete() {
        List<CompletableFuture<?>> futureList = new ArrayList<>();
        futureList.addAll(sendFutureList);
        futureList.addAll(ackFutureList);
        return CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]));
    }
}
