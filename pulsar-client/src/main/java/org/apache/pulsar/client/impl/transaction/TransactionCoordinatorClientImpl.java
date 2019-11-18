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
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionMetaStoreClientException;
import org.apache.pulsar.client.api.transaction.TransactionMetaStoreClientException.TransactionMetaStoreClientStateException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TransactionMetaStoreHandler;
import org.apache.pulsar.client.util.MathUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    private Producer<byte[]> producerForAssignTopic = null;
    private TransactionMetaStoreHandler[] handlers;
    private final AtomicLong epoch = new AtomicLong(0);
    private volatile Timeout requestTimeout = null;


    private static final AtomicReferenceFieldUpdater<TransactionCoordinatorClientImpl, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(TransactionCoordinatorClientImpl.class, State.class, "state");
    private volatile State state = State.NONE;

    public TransactionCoordinatorClientImpl(PulsarClient pulsarClient) {
        this.pulsarClient = (PulsarClientImpl) pulsarClient;
    }

    @Override
    public void start() throws TransactionMetaStoreClientException {
        try {
            startAsync().get();
        } catch (Exception e) {
            throw TransactionMetaStoreClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> startAsync() {
        if (STATE_UPDATER.compareAndSet(this, State.NONE, State.STARTING)) {
            return pulsarClient.getLookup().getPartitionedTopicMetadata(TopicName.TRANSACTION_COORDINATOR_ASSIGN)
                .thenAccept(partitionMeta -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Transaction meta store assign partition is {}.", partitionMeta.partitions);
                    }
                    if (partitionMeta.partitions > 0) {
                        handlers = new TransactionMetaStoreHandler[partitionMeta.partitions];
                        for (int i = 0; i < partitionMeta.partitions; i++) {
                            handlers[i] = new TransactionMetaStoreHandler(i, pulsarClient,
                                TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString() + TopicName.PARTITIONED_TOPIC_SUFFIX + i);
                        }
                    } else {
                        handlers = new TransactionMetaStoreHandler[1];
                        handlers[0] = new TransactionMetaStoreHandler(0, pulsarClient,
                            TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());
                    }

                    STATE_UPDATER.set(TransactionCoordinatorClientImpl.this, State.READY);

                });
        } else {
            return FutureUtil.failedFuture(new TransactionMetaStoreClientStateException("Can not start while current state is " + state));
        }
    }

    @Override
    public void close() throws TransactionMetaStoreClientException {
        try {
            closeAsync().get();
        } catch (Exception e) {
            throw TransactionMetaStoreClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (getState() == State.CLOSING || getState() == State.CLOSED) {
            LOG.warn("The transaction meta store is closing or closed, doing nothing.");
            result.complete(null);
        } else {
            for (TransactionMetaStoreHandler handler : handlers) {
                try {
                    handler.close();
                } catch (IOException e) {
                    LOG.warn("Close transaction meta store handler error", e);
                }
            }
            this.handlers = null;
            result.complete(null);
        }
        return result;
    }

    @Override
    public TxnID newTransaction() throws TransactionMetaStoreClientException {
        try {
            return newTransactionAsync().get();
        } catch (Exception e) {
            throw TransactionMetaStoreClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<TxnID> newTransactionAsync() {
        return newTransactionAsync(DEFAULT_TXN_TTL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public TxnID newTransaction(long timeout, TimeUnit unit) throws TransactionMetaStoreClientException {
        try {
            return newTransactionAsync(timeout, unit).get();
        } catch (Exception e) {
            throw TransactionMetaStoreClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<TxnID> newTransactionAsync(long timeout, TimeUnit unit) {
        return nextHandler().newTransactionAsync(timeout, unit);
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
