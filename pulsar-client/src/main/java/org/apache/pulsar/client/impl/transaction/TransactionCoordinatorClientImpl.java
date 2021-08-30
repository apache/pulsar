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

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.CoordinatorClientStateException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TransactionMetaStoreHandler;
import org.apache.pulsar.client.util.MathUtils;
import org.apache.pulsar.common.api.proto.Subscription;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Transaction coordinator client based topic assigned.
 */
public class TransactionCoordinatorClientImpl implements TransactionCoordinatorClient {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionCoordinatorClientImpl.class);

    private final PulsarClientImpl pulsarClient;
    private TransactionMetaStoreHandler[] handlers;
    private ConcurrentLongHashMap<TransactionMetaStoreHandler> handlerMap = new ConcurrentLongHashMap<>(16, 1);
    private final AtomicLong epoch = new AtomicLong(0);

    private static final AtomicReferenceFieldUpdater<TransactionCoordinatorClientImpl, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(TransactionCoordinatorClientImpl.class, State.class, "state");
    private volatile State state = State.NONE;

    public TransactionCoordinatorClientImpl(PulsarClient pulsarClient) {
        this.pulsarClient = (PulsarClientImpl) pulsarClient;
    }

    @Override
    public void start() throws TransactionCoordinatorClientException {
        try {
            startAsync().get();
        } catch (Exception e) {
            throw TransactionCoordinatorClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> startAsync() {
        if (STATE_UPDATER.compareAndSet(this, State.NONE, State.STARTING)) {
            return pulsarClient.getLookup().getPartitionedTopicMetadata(TopicName.TRANSACTION_COORDINATOR_ASSIGN)
                .thenCompose(partitionMeta -> {
                    List<CompletableFuture<Void>> connectFutureList = new ArrayList<>();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Transaction meta store assign partition is {}.", partitionMeta.partitions);
                    }
                    if (partitionMeta.partitions > 0) {
                        handlers = new TransactionMetaStoreHandler[partitionMeta.partitions];
                        for (int i = 0; i < partitionMeta.partitions; i++) {
                            CompletableFuture<Void> connectFuture = new CompletableFuture<>();
                            connectFutureList.add(connectFuture);
                            TransactionMetaStoreHandler handler = new TransactionMetaStoreHandler(
                                    i, pulsarClient, getTCAssignTopicName(i), connectFuture);
                            handlers[i] = handler;
                            handlerMap.put(i, handler);
                        }
                    } else {
                        handlers = new TransactionMetaStoreHandler[1];
                        CompletableFuture<Void> connectFuture = new CompletableFuture<>();
                        connectFutureList.add(connectFuture);
                        TransactionMetaStoreHandler handler = new TransactionMetaStoreHandler(0, pulsarClient,
                                getTCAssignTopicName(-1), connectFuture);
                        handlers[0] = handler;
                        handlerMap.put(0, handler);
                    }

                    STATE_UPDATER.set(TransactionCoordinatorClientImpl.this, State.READY);

                    return FutureUtil.waitForAll(connectFutureList);
                });
        } else {
            return FutureUtil.failedFuture(new CoordinatorClientStateException("Can not start while current state is " + state));
        }
    }

    private String getTCAssignTopicName(int partition) {
        if (partition >= 0) {
            return TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString() + TopicName.PARTITIONED_TOPIC_SUFFIX + partition;
        } else {
            return TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString();
        }
    }

    @Override
    public void close() throws TransactionCoordinatorClientException {
        try {
            closeAsync().get();
        } catch (Exception e) {
            throw TransactionCoordinatorClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (getState() == State.CLOSING || getState() == State.CLOSED) {
            LOG.warn("The transaction meta store is closing or closed, doing nothing.");
            result.complete(null);
        } else {
            if (handlers != null) {
                for (TransactionMetaStoreHandler handler : handlers) {
                    try {
                        handler.close();
                    } catch (IOException e) {
                        LOG.warn("Close transaction meta store handler error", e);
                    }
                }
            }
            this.handlers = null;
            result.complete(null);
        }
        return result;
    }

    @Override
    public TxnID newTransaction() throws TransactionCoordinatorClientException {
        try {
            return newTransactionAsync().get();
        } catch (Exception e) {
            throw TransactionCoordinatorClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<TxnID> newTransactionAsync() {
        return newTransactionAsync(DEFAULT_TXN_TTL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public TxnID newTransaction(long timeout, TimeUnit unit) throws TransactionCoordinatorClientException {
        try {
            return newTransactionAsync(timeout, unit).get();
        } catch (Exception e) {
            throw TransactionCoordinatorClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<TxnID> newTransactionAsync(long timeout, TimeUnit unit) {
        return nextHandler().newTransactionAsync(timeout, unit);
    }

    @Override
    public void addPublishPartitionToTxn(TxnID txnID, List<String> partitions) throws TransactionCoordinatorClientException {
        try {
            addPublishPartitionToTxnAsync(txnID, partitions).get();
        } catch (Exception e) {
            throw TransactionCoordinatorClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> addPublishPartitionToTxnAsync(TxnID txnID, List<String> partitions) {
        TransactionMetaStoreHandler handler = handlerMap.get(txnID.getMostSigBits());
        if (handler == null) {
            return FutureUtil.failedFuture(
                    new TransactionCoordinatorClientException.MetaStoreHandlerNotExistsException(txnID.getMostSigBits()));
        }
        return handler.addPublishPartitionToTxnAsync(txnID, partitions);
    }

    @Override
    public void addSubscriptionToTxn(TxnID txnID, String topic, String subscription)
            throws TransactionCoordinatorClientException {
        try {
            addSubscriptionToTxnAsync(txnID, topic, subscription).get();
        } catch (Exception e) {
            throw TransactionCoordinatorClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> addSubscriptionToTxnAsync(TxnID txnID, String topic, String subscription) {
        TransactionMetaStoreHandler handler = handlerMap.get(txnID.getMostSigBits());
        if (handler == null) {
            return FutureUtil.failedFuture(
                    new TransactionCoordinatorClientException.MetaStoreHandlerNotExistsException(txnID.getMostSigBits()));
        }
        Subscription sub = new Subscription()
                .setTopic(topic)
                .setSubscription(subscription);
        return handler.addSubscriptionToTxn(txnID, Collections.singletonList(sub));
    }

    @Override
    public void commit(TxnID txnID) throws TransactionCoordinatorClientException {
        try {
            commitAsync(txnID).get();
        } catch (Exception e) {
            throw TransactionCoordinatorClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> commitAsync(TxnID txnID) {
        TransactionMetaStoreHandler handler = handlerMap.get(txnID.getMostSigBits());
        if (handler == null) {
            return FutureUtil.failedFuture(
                    new TransactionCoordinatorClientException.MetaStoreHandlerNotExistsException(txnID.getMostSigBits()));
        }
        return handler.endTxnAsync(txnID, TxnAction.COMMIT);
    }

    @Override
    public void abort(TxnID txnID) throws TransactionCoordinatorClientException {
        try {
            abortAsync(txnID).get();
        } catch (Exception e) {
            throw TransactionCoordinatorClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> abortAsync(TxnID txnID) {
        TransactionMetaStoreHandler handler = handlerMap.get(txnID.getMostSigBits());
        if (handler == null) {
            return FutureUtil.failedFuture(
                    new TransactionCoordinatorClientException.MetaStoreHandlerNotExistsException(txnID.getMostSigBits()));
        }
        return handler.endTxnAsync(txnID, TxnAction.ABORT);
    }

    @Override
    public State getState() {
        return state;
    }

    private TransactionMetaStoreHandler nextHandler() {
        int index = MathUtils.signSafeMod(epoch.incrementAndGet(), handlers.length);
        return handlers[index];
    }
}
