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
     * Choose the discovery strategy. The metadata-store assignment watch needs a binary-protocol
     * connection, so it's only usable when the client is configured with a {@code pulsar://}
     * service URL; with an {@code http://} service URL we always use the assign-topic flow (which
     * resolves coordinators via the admin/HTTP-capable partitioned-metadata lookup). When binary
     * lookup is available, probe the broker's {@code supports_tc_metadata_discovery} feature flag to
     * decide; if the broker doesn't advertise it (old broker, or scalable-topics TC disabled), fall
     * back to the assign-topic flow.
     */
    private CompletableFuture<TcDiscovery> selectDiscovery() {
        if (!pulsarClient.getLookup().isBinaryProtoLookupService()) {
            return CompletableFuture.completedFuture(new AssignTopicTcDiscovery(pulsarClient));
        }
        // Probe a broker connection to read the feature flag. Use getAnyBrokerProxyConnection() (not
        // getConnectionToServiceUrl()): when connecting through a proxy, the latter yields the proxy's
        // own CONNECTED, which carries the proxy lookup handshake's flags rather than a broker's;
        // getAnyBrokerProxyConnection() pairs to an actual broker (directly or proxied) so the
        // forwarded feature flags reflect the broker — the same connection the watch itself uses.
        // If the probe fails, fall back to the assign-topic flow, whose lookup retries across hosts
        // and still works against v5 brokers (the assign topic exists during the deprecation window),
        // so falling back is always safe.
        return pulsarClient.getAnyBrokerProxyConnection()
                .thenApply(cnx -> cnx.isSupportsTcMetadataDiscovery()
                        ? (TcDiscovery) new WatchTcAssignmentsDiscovery(pulsarClient)
                        : new AssignTopicTcDiscovery(pulsarClient))
                .exceptionally(ex -> {
                    log.info().exception(ex)
                            .log("TC discovery feature probe failed; using assign-topic discovery");
                    return new AssignTopicTcDiscovery(pulsarClient);
                });
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
