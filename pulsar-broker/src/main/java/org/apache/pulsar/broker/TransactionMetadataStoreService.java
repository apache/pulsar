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
package org.apache.pulsar.broker;

import static org.apache.pulsar.transaction.coordinator.proto.TxnStatus.ABORTING;
import static org.apache.pulsar.transaction.coordinator.proto.TxnStatus.COMMITTING;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.transaction.buffer.exceptions.UnsupportedTxnActionException;
import org.apache.pulsar.broker.transaction.recover.TransactionRecoverTrackerImpl;
import org.apache.pulsar.broker.transaction.timeout.TransactionTimeoutTrackerFactoryImpl;
import org.apache.pulsar.client.api.PulsarClientException.BrokerPersistenceException;
import org.apache.pulsar.client.api.PulsarClientException.ConnectException;
import org.apache.pulsar.client.api.PulsarClientException.LookupException;
import org.apache.pulsar.client.api.transaction.TransactionBufferClient;
import org.apache.pulsar.client.api.transaction.TransactionBufferClientException.ReachMaxPendingOpsException;
import org.apache.pulsar.client.api.transaction.TransactionBufferClientException.RequestTimeoutException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.TransactionSubscription;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTrackerFactory;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.CoordinatorNotFoundException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.TransactionMetadataStoreStateException;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionMetadataStoreService {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionMetadataStoreService.class);

    private final Map<TransactionCoordinatorID, TransactionMetadataStore> stores;
    private final TransactionMetadataStoreProvider transactionMetadataStoreProvider;
    private final PulsarService pulsarService;
    private final TransactionBufferClient tbClient;
    private final TransactionTimeoutTrackerFactory timeoutTrackerFactory;
    private static final long endTransactionRetryIntervalTime = 1000;
    private final Timer transactionOpRetryTimer;
    // this semaphore for loading one transaction coordinator with the same tc id on the same time
    private final ConcurrentLongHashMap<Semaphore> tcLoadSemaphores;
    // one connect request open the transactionMetaStore the other request will add to the queue, when the open op
    // finished the request will be poll and complete the future
    private final ConcurrentLongHashMap<ConcurrentLinkedDeque<CompletableFuture<Void>>> pendingConnectRequests;

    private static final long HANDLE_PENDING_CONNECT_TIME_OUT = 30000L;

    public TransactionMetadataStoreService(TransactionMetadataStoreProvider transactionMetadataStoreProvider,
                                           PulsarService pulsarService, TransactionBufferClient tbClient,
                                           HashedWheelTimer timer) {
        this.pulsarService = pulsarService;
        this.stores = new ConcurrentHashMap<>();
        this.transactionMetadataStoreProvider = transactionMetadataStoreProvider;
        this.tbClient = tbClient;
        this.timeoutTrackerFactory = new TransactionTimeoutTrackerFactoryImpl(this, timer);
        this.transactionOpRetryTimer = timer;
        this.tcLoadSemaphores = new ConcurrentLongHashMap<>();
        this.pendingConnectRequests = new ConcurrentLongHashMap<>();
    }

    @Deprecated
    public void start() {
        pulsarService.getNamespaceService().addNamespaceBundleOwnershipListener(new NamespaceBundleOwnershipListener() {

            @Override
            public void onLoad(NamespaceBundle bundle) {
                pulsarService.getNamespaceService().getOwnedTopicListForNamespaceBundle(bundle)
                        .whenComplete((topics, ex) -> {
                            if (ex == null) {
                                for (String topic : topics) {
                                    TopicName name = TopicName.get(topic);
                                    if (TopicName.TRANSACTION_COORDINATOR_ASSIGN.getLocalName()
                                            .equals(TopicName.get(name.getPartitionedTopicName()).getLocalName())
                                            && name.isPartitioned()) {
                                        handleTcClientConnect(TransactionCoordinatorID.get(name.getPartitionIndex()));
                                    }
                                }
                            } else {
                                LOG.error("Failed to get owned topic list when triggering on-loading bundle {}.",
                                        bundle, ex);
                            }
                        });
            }

            @Override
            public void unLoad(NamespaceBundle bundle) {
                pulsarService.getNamespaceService().getOwnedTopicListForNamespaceBundle(bundle)
                        .whenComplete((topics, ex) -> {
                            if (ex == null) {
                                for (String topic : topics) {
                                    TopicName name = TopicName.get(topic);
                                    if (TopicName.TRANSACTION_COORDINATOR_ASSIGN.getLocalName()
                                            .equals(TopicName.get(name.getPartitionedTopicName()).getLocalName())
                                            && name.isPartitioned()) {
                                        removeTransactionMetadataStore(
                                                TransactionCoordinatorID.get(name.getPartitionIndex()));
                                    }
                                }
                            } else {
                                LOG.error("Failed to get owned topic list error when triggering un-loading bundle {}.",
                                        bundle, ex);
                            }
                        });
            }
            @Override
            public boolean test(NamespaceBundle namespaceBundle) {
                return namespaceBundle.getNamespaceObject().equals(NamespaceName.SYSTEM_NAMESPACE);
            }
        });
    }

    public CompletableFuture<Void> handleTcClientConnect(TransactionCoordinatorID tcId) {
        if (stores.get(tcId) != null) {
            return CompletableFuture.completedFuture(null);
        } else {
            return pulsarService.getBrokerService().checkTopicNsOwnership(TopicName
                    .TRANSACTION_COORDINATOR_ASSIGN.getPartition((int) tcId.getId()).toString()).thenCompose(v -> {
                        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                final Semaphore tcLoadSemaphore = this.tcLoadSemaphores
                        .computeIfAbsent(tcId.getId(), (id) -> new Semaphore(1));
                Deque<CompletableFuture<Void>> deque = pendingConnectRequests
                        .computeIfAbsent(tcId.getId(), (id) -> new ConcurrentLinkedDeque<>());
                if (tcLoadSemaphore.tryAcquire()) {
                    // when tcLoadSemaphore.release(), this command will acquire semaphore, so we should jude the store
                    // exist again.
                    if (stores.get(tcId) != null) {
                        return CompletableFuture.completedFuture(null);
                    }

                    openTransactionMetadataStore(tcId).thenAccept((store) -> {
                        stores.put(tcId, store);
                        LOG.info("Added new transaction meta store {}", tcId);
                        long endTime = System.currentTimeMillis() + HANDLE_PENDING_CONNECT_TIME_OUT;
                        while (true) {
                            // prevent thread in a busy loop.
                            if (System.currentTimeMillis() < endTime) {
                                CompletableFuture<Void> future = deque.poll();
                                if (future != null) {
                                    // complete queue request future
                                    future.complete(null);
                                } else {
                                    break;
                                }
                            } else {
                                deque.clear();
                                break;
                            }
                        }

                        completableFuture.complete(null);
                        tcLoadSemaphore.release();
                    }).exceptionally(e -> {
                        completableFuture.completeExceptionally(e.getCause());
                        // release before handle request queue, in order to client reconnect infinite loop
                        tcLoadSemaphore.release();
                        long endTime = System.currentTimeMillis() + HANDLE_PENDING_CONNECT_TIME_OUT;
                        while (true) {
                            // prevent thread in a busy loop.
                            if (System.currentTimeMillis() < endTime) {
                                CompletableFuture<Void> future = deque.poll();
                                if (future != null) {
                                    // this means that this tc client connection connect fail
                                    future.completeExceptionally(e);
                                } else {
                                    break;
                                }
                            } else {
                                deque.clear();
                                break;
                            }
                        }
                        LOG.error("Add transaction metadata store with id {} error", tcId.getId(), e);
                        return null;
                    });
                } else {
                    // only one command can open transaction metadata store,
                    // other will be added to the deque, when the op of openTransactionMetadataStore finished
                    // then handle the requests witch in the queue
                    deque.add(completableFuture);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Handle tc client connect added into pending queue! tcId : {}", tcId.toString());
                    }
                }
                return completableFuture;
            });
        }
    }

    public CompletableFuture<TransactionMetadataStore> openTransactionMetadataStore(TransactionCoordinatorID tcId) {
        return pulsarService.getBrokerService()
                .getManagedLedgerConfig(TopicName.get(MLTransactionLogImpl
                        .TRANSACTION_LOG_PREFIX + tcId)).thenCompose(v -> {
                            TransactionTimeoutTracker timeoutTracker = timeoutTrackerFactory.newTracker(tcId);
                            TransactionRecoverTracker recoverTracker =
                                    new TransactionRecoverTrackerImpl(TransactionMetadataStoreService.this,
                                    timeoutTracker, tcId.getId());
                            return transactionMetadataStoreProvider
                                    .openStore(tcId, pulsarService.getManagedLedgerFactory(), v,
                                            timeoutTracker, recoverTracker);
                });
    }

    public CompletableFuture<Void> removeTransactionMetadataStore(TransactionCoordinatorID tcId) {
        final Semaphore tcLoadSemaphore = this.tcLoadSemaphores
                .computeIfAbsent(tcId.getId(), (id) -> new Semaphore(1));
        if (tcLoadSemaphore.tryAcquire()) {
            TransactionMetadataStore metadataStore = stores.remove(tcId);
            if (metadataStore != null) {
                metadataStore.closeAsync().whenComplete((v, ex) -> {
                    if (ex != null) {
                        LOG.error("Close transaction metadata store with id " + tcId, ex);
                    } else {
                        LOG.info("Removed and closed transaction meta store {}", tcId);
                    }
                });
            }
            tcLoadSemaphore.release();
            return CompletableFuture.completedFuture(null);
        } else {
            return FutureUtil.failedFuture(
                    new ServiceUnitNotReadyException("Could not remove "
                            + "TransactionMetadataStore, it is doing other operations!"));
        }
    }

    public CompletableFuture<TxnID> newTransaction(TransactionCoordinatorID tcId, long timeoutInMills) {
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.newTransaction(timeoutInMills);
    }

    public CompletableFuture<Void> addProducedPartitionToTxn(TxnID txnId, List<String> partitions) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnId);
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.addProducedPartitionToTxn(txnId, partitions);
    }

    public CompletableFuture<Void> addAckedPartitionToTxn(TxnID txnId, List<TransactionSubscription> partitions) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnId);
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.addAckedPartitionToTxn(txnId, partitions);
    }

    public CompletableFuture<TxnMeta> getTxnMeta(TxnID txnId) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnId);
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.getTxnMeta(txnId);
    }

    public long getLowWaterMark(TxnID txnID) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnID);
        TransactionMetadataStore store = stores.get(tcId);

        if (store == null) {
            return 0;
        }
        return store.getLowWaterMark();
    }

    public CompletableFuture<Void> updateTxnStatus(TxnID txnId, TxnStatus newStatus, TxnStatus expectedStatus,
                                                   boolean isTimeout) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnId);
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.updateTxnStatus(txnId, newStatus, expectedStatus, isTimeout);
    }

    public CompletableFuture<Void> endTransaction(TxnID txnID, int txnAction, boolean isTimeout) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        TxnStatus newStatus;
        switch (txnAction) {
            case TxnAction.COMMIT_VALUE:
                newStatus = COMMITTING;
                break;
            case TxnAction.ABORT_VALUE:
                newStatus = ABORTING;
                break;
            default:
                UnsupportedTxnActionException exception =
                        new UnsupportedTxnActionException(txnID, txnAction);
                LOG.error(exception.getMessage());
                completableFuture.completeExceptionally(exception);
                return completableFuture;
        }

        getTxnMeta(txnID).thenAccept(txnMeta -> {
            TxnStatus txnStatus = txnMeta.status();
            if (txnStatus == TxnStatus.OPEN) {
                updateTxnStatus(txnID, newStatus, TxnStatus.OPEN, isTimeout).thenAccept(v ->
                        endTxnInTransactionBuffer(txnID, txnAction).thenAccept(a ->
                                completableFuture.complete(null)).exceptionally(e -> {
                            if (!isRetryableException(e.getCause())) {
                                LOG.error("EndTxnInTransactionBuffer fail! TxnId : {}, "
                                        + "TxnAction : {}", txnID, txnAction, e);
                            } else {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("EndTxnInTransactionBuffer retry! TxnId : {}, "
                                            + "TxnAction : {}", txnID, txnAction, e);
                                }
                                transactionOpRetryTimer.newTimeout(timeout ->
                                        endTransaction(txnID, txnAction, isTimeout),
                                        endTransactionRetryIntervalTime, TimeUnit.MILLISECONDS);

                            }
                            completableFuture.completeExceptionally(e);
                            return null;
                        })).exceptionally(e -> {
                    if (!isRetryableException(e.getCause())) {
                        LOG.error("EndTransaction UpdateTxnStatus fail! TxnId : {}, "
                                + "TxnAction : {}", txnID, txnAction, e);
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("EndTransaction UpdateTxnStatus op retry! TxnId : {}, "
                                    + "TxnAction : {}", txnID, txnAction, e);
                        }
                        transactionOpRetryTimer.newTimeout(timeout -> endTransaction(txnID, txnAction, isTimeout),
                                endTransactionRetryIntervalTime, TimeUnit.MILLISECONDS);

                    }
                    completableFuture.completeExceptionally(e);
                    return null;
                });
            } else {
                if ((txnStatus == COMMITTING && txnAction == TxnAction.COMMIT.getValue())
                        || (txnStatus == ABORTING && txnAction == TxnAction.ABORT.getValue())) {
                    endTxnInTransactionBuffer(txnID, txnAction).thenAccept(k ->
                            completableFuture.complete(null)).exceptionally(e -> {
                        if (isRetryableException(e.getCause())) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("EndTxnInTransactionBuffer retry! TxnId : {}, "
                                        + "TxnAction : {}", txnID, txnAction, e);
                            }
                            transactionOpRetryTimer.newTimeout(timeout ->
                                            endTransaction(txnID, txnAction, isTimeout),
                                    endTransactionRetryIntervalTime, TimeUnit.MILLISECONDS);
                        } else {
                            LOG.error("EndTxnInTransactionBuffer fail! TxnId : {}, "
                                    + "TxnAction : {}", txnID, txnAction, e);
                        }
                        completableFuture.completeExceptionally(e);
                        return null;
                    });
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("EndTxnInTransactionBuffer op retry! TxnId : {}, TxnAction : {}", txnID, txnAction);
                    }
                    completableFuture.completeExceptionally(new InvalidTxnStatusException(txnID, newStatus, txnStatus));
                }
            }
        }).exceptionally(e -> {
            if (isRetryableException(e.getCause())) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("End transaction op retry! TxnId : {}, TxnAction : {}", txnID, txnAction, e);
                }
                transactionOpRetryTimer.newTimeout(timeout -> endTransaction(txnID, txnAction, isTimeout),
                        endTransactionRetryIntervalTime, TimeUnit.MILLISECONDS);
            }
            completableFuture.completeExceptionally(e);
            return null;
        });
        return completableFuture;
    }

    // when managedLedger fence will remove this tc and reload
    public void handleOpFail(Throwable e, TransactionCoordinatorID tcId) {
        if (e.getCause() instanceof ManagedLedgerException.ManagedLedgerFencedException
                || e instanceof ManagedLedgerException.ManagedLedgerFencedException) {
            removeTransactionMetadataStore(tcId);
        }
    }

    public void endTransactionForTimeout(TxnID txnID) {
        getTxnMeta(txnID).thenCompose(txnMeta -> {
            if (txnMeta.status() == TxnStatus.OPEN) {
                return endTransaction(txnID, TxnAction.ABORT_VALUE, true);
            } else {
                return null;
            }
        }).exceptionally(e -> {
            if (isRetryableException(e.getCause())) {
                endTransaction(txnID, TxnAction.ABORT_VALUE, true);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Transaction have been handle complete, "
                            + "don't need to handle by transaction timeout! TxnId : {}", txnID);
                }
            }
            return null;
        });
    }

    private CompletableFuture<Void> endTxnInTransactionBuffer(TxnID txnID, int txnAction) {
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        List<CompletableFuture<TxnID>> completableFutureList = new ArrayList<>();
        this.getTxnMeta(txnID).whenComplete((txnMeta, throwable) -> {
            if (throwable != null) {
                resultFuture.completeExceptionally(throwable);
                return;
            }
            long lowWaterMark = getLowWaterMark(txnID);

            txnMeta.ackedPartitions().forEach(tbSub -> {
                CompletableFuture<TxnID> actionFuture = new CompletableFuture<>();
                if (TxnAction.COMMIT_VALUE == txnAction) {
                    actionFuture = tbClient.commitTxnOnSubscription(
                            tbSub.getTopic(), tbSub.getSubscription(), txnID.getMostSigBits(),
                            txnID.getLeastSigBits(), lowWaterMark);
                } else if (TxnAction.ABORT_VALUE == txnAction) {
                    actionFuture = tbClient.abortTxnOnSubscription(
                            tbSub.getTopic(), tbSub.getSubscription(), txnID.getMostSigBits(),
                            txnID.getLeastSigBits(), lowWaterMark);
                } else {
                    actionFuture.completeExceptionally(new Throwable("Unsupported txnAction " + txnAction));
                }
                completableFutureList.add(actionFuture);
            });

            txnMeta.producedPartitions().forEach(partition -> {
                CompletableFuture<TxnID> actionFuture = new CompletableFuture<>();
                if (TxnAction.COMMIT_VALUE == txnAction) {
                    actionFuture = tbClient.commitTxnOnTopic(partition, txnID.getMostSigBits(),
                            txnID.getLeastSigBits(), lowWaterMark);
                } else if (TxnAction.ABORT_VALUE == txnAction) {
                    actionFuture = tbClient.abortTxnOnTopic(partition, txnID.getMostSigBits(),
                            txnID.getLeastSigBits(), lowWaterMark);
                } else {
                    actionFuture.completeExceptionally(new Throwable("Unsupported txnAction " + txnAction));
                }
                completableFutureList.add(actionFuture);
            });

            try {
                FutureUtil.waitForAll(completableFutureList).whenComplete((ignored, waitThrowable) -> {
                    if (waitThrowable != null) {
                        resultFuture.completeExceptionally(waitThrowable);
                        return;
                    }
                    resultFuture.complete(null);
                });
            } catch (Exception e) {
                resultFuture.completeExceptionally(e);
            }
        });

        return resultFuture.thenCompose((future) -> endTxnInTransactionMetadataStore(txnID, txnAction));
    }

    private static boolean isRetryableException(Throwable e) {
        return (e instanceof TransactionMetadataStoreStateException
                || e instanceof RequestTimeoutException
                || e instanceof ManagedLedgerException
                || e instanceof BrokerPersistenceException
                || e instanceof LookupException
                || e instanceof ReachMaxPendingOpsException
                || e instanceof ConnectException)
                && !(e instanceof ManagedLedgerException.ManagedLedgerFencedException);
    }

    private CompletableFuture<Void> endTxnInTransactionMetadataStore(TxnID txnID, int txnAction) {
        if (TxnAction.COMMIT.getValue() == txnAction) {
            return updateTxnStatus(txnID, TxnStatus.COMMITTED, COMMITTING, false);
        } else if (TxnAction.ABORT.getValue() == txnAction) {
            return updateTxnStatus(txnID, TxnStatus.ABORTED, ABORTING, false);
        } else {
            return FutureUtil.failedFuture(new InvalidTxnStatusException("Unsupported txnAction " + txnAction));
        }
    }

    private TransactionCoordinatorID getTcIdFromTxnId(TxnID txnId) {
        return new TransactionCoordinatorID(txnId.getMostSigBits());
    }

    @VisibleForTesting
    public Map<TransactionCoordinatorID, TransactionMetadataStore> getStores() {
        return Collections.unmodifiableMap(stores);
    }
}
