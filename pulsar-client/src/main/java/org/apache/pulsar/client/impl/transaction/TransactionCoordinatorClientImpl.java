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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.CustomLog;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.CoordinatorClientStateException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TransactionMetaStoreHandler;
import org.apache.pulsar.common.api.proto.Subscription;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Transaction coordinator client. Coordinator <em>location</em> is delegated to a {@link TcDiscovery}
 * strategy chosen from the broker's {@code supports_tc_metadata_discovery} feature flag at
 * {@link #startAsync()}: {@link WatchTcAssignmentsDiscovery} when the broker advertises the
 * metadata-store election, else {@link AssignTopicTcDiscovery} (the assign-topic mechanism). The
 * routing surface here ({@code newTransaction} round-robin, {@code commit}/{@code abort} by
 * {@code TxnID.mostSigBits}) is the same for both.
 */
@CustomLog
public class TransactionCoordinatorClientImpl implements TransactionCoordinatorClient {

    private final PulsarClientImpl pulsarClient;
    private volatile TcDiscovery discovery;

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
            return selectDiscovery()
                    .thenCompose(selected -> {
                        this.discovery = selected;
                        log.info().attr("discovery", selected.getClass().getSimpleName())
                                .log("Transaction coordinator discovery selected");
                        return selected.start();
                    })
                    .thenRun(() -> STATE_UPDATER.set(this, State.READY));
        } else {
            return FutureUtil.failedFuture(
                    new CoordinatorClientStateException("Can not start while current state is " + state));
        }
    }

    /**
     * Choose the discovery strategy by client/SDK kind, not by broker capability. A v5 SDK client
     * sets the internal {@code scalableTransactions} config flag and uses the metadata-store
     * coordinator (assignment watch); a v4 SDK client leaves it unset and uses the legacy
     * assign-topic coordinator. This keeps v4 and v5 transactions independent on the same cluster:
     * flipping the broker default to enable the v5 TC must not silently re-route v4 clients to it,
     * since the v5 TC notifies participants via metadata-store events that the legacy transaction
     * buffer / pending-ack store don't consume.
     */
    private CompletableFuture<TcDiscovery> selectDiscovery() {
        if (!pulsarClient.getConfiguration().isScalableTransactions()) {
            return CompletableFuture.completedFuture(new AssignTopicTcDiscovery(pulsarClient));
        }
        // The metadata-store assignment watch needs a binary connection. A v5 client on an
        // http:// service URL is a misconfiguration — fail clearly rather than silently downgrade.
        if (!pulsarClient.getLookup().isBinaryProtoLookupService()) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidServiceURL(
                    "Scalable-topics transactions require a pulsar:// service URL", null));
        }
        return CompletableFuture.completedFuture(new WatchTcAssignmentsDiscovery(pulsarClient));
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
            log.warn("The transaction meta store is closing or closed, doing nothing.");
            result.complete(null);
        } else {
            if (discovery != null) {
                try {
                    discovery.close();
                } catch (Exception e) {
                    log.warn().exception(e).log("Close transaction coordinator discovery error");
                }
                discovery = null;
            }
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
        TransactionMetaStoreHandler handler = discovery.nextHandler();
        if (handler == null) {
            return FutureUtil.failedFuture(new TransactionCoordinatorClientException(
                    "No transaction coordinator is currently available"));
        }
        return handler.newTransactionAsync(timeout, unit);
    }

    @Override
    public void addPublishPartitionToTxn(TxnID txnID, List<String> partitions)
            throws TransactionCoordinatorClientException {
        try {
            addPublishPartitionToTxnAsync(txnID, partitions).get();
        } catch (Exception e) {
            throw TransactionCoordinatorClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> addPublishPartitionToTxnAsync(TxnID txnID, List<String> partitions) {
        TransactionMetaStoreHandler handler = discovery.handlerForCoordinator(txnID.getMostSigBits());
        if (handler == null) {
            return FutureUtil.failedFuture(
                    new TransactionCoordinatorClientException.MetaStoreHandlerNotExistsException(
                            txnID.getMostSigBits()));
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
        TransactionMetaStoreHandler handler = discovery.handlerForCoordinator(txnID.getMostSigBits());
        if (handler == null) {
            return FutureUtil.failedFuture(
                    new TransactionCoordinatorClientException.MetaStoreHandlerNotExistsException(
                            txnID.getMostSigBits()));
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
        TransactionMetaStoreHandler handler = discovery.handlerForCoordinator(txnID.getMostSigBits());
        if (handler == null) {
            return FutureUtil.failedFuture(
                    new TransactionCoordinatorClientException.MetaStoreHandlerNotExistsException(
                            txnID.getMostSigBits()));
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
        TransactionMetaStoreHandler handler = discovery.handlerForCoordinator(txnID.getMostSigBits());
        if (handler == null) {
            return FutureUtil.failedFuture(
                    new TransactionCoordinatorClientException.MetaStoreHandlerNotExistsException(
                            txnID.getMostSigBits()));
        }
        return handler.endTxnAsync(txnID, TxnAction.ABORT);
    }

    @Override
    public State getState() {
        return state;
    }

    /** @return the current coordinator handlers. Visible for testing. */
    @VisibleForTesting
    public Collection<TransactionMetaStoreHandler> getHandlers() {
        return discovery == null ? List.of() : discovery.handlers();
    }

    /**
     * @return {@code true} if coordinator discovery uses the metadata-store assignment watch (rather
     *     than the assign-topic fallback). Visible for testing so integration tests can assert the
     *     watch path was actually exercised.
     */
    @VisibleForTesting
    public boolean isUsingMetadataDiscovery() {
        return discovery instanceof WatchTcAssignmentsDiscovery;
    }
}
