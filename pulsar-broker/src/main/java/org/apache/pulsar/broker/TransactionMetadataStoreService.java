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

import static org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl.getMLTransactionLogName;
import static org.apache.pulsar.transaction.coordinator.proto.TxnStatus.ABORTING;
import static org.apache.pulsar.transaction.coordinator.proto.TxnStatus.COMMITTING;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.transaction.exception.coordinator.TransactionCoordinatorException;
import org.apache.pulsar.broker.transaction.recover.TransactionRecoverTrackerImpl;
import org.apache.pulsar.broker.transaction.timeout.TransactionTimeoutTrackerFactoryImpl;
import org.apache.pulsar.client.api.PulsarClientException.BrokerPersistenceException;
import org.apache.pulsar.client.api.PulsarClientException.ConnectException;
import org.apache.pulsar.client.api.PulsarClientException.LookupException;
import org.apache.pulsar.client.api.transaction.TransactionBufferClient;
import org.apache.pulsar.client.api.transaction.TransactionBufferClientException.ReachMaxPendingOpsException;
import org.apache.pulsar.client.api.transaction.TransactionBufferClientException.RequestTimeoutException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.util.ExecutorProvider;
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
    private final ExecutorService internalPinnedExecutor;

    private static final long HANDLE_PENDING_CONNECT_TIME_OUT = 30000L;

    private final ThreadFactory threadFactory =
            new ExecutorProvider.ExtendedThreadFactory("transaction-coordinator-thread-factory");


    public TransactionMetadataStoreService(TransactionMetadataStoreProvider transactionMetadataStoreProvider,
                                           PulsarService pulsarService, TransactionBufferClient tbClient,
                                           HashedWheelTimer timer) {
        this.pulsarService = pulsarService;
        this.stores = new ConcurrentHashMap<>();
        this.transactionMetadataStoreProvider = transactionMetadataStoreProvider;
        this.tbClient = tbClient;
        this.timeoutTrackerFactory = new TransactionTimeoutTrackerFactoryImpl(this, timer);
        this.transactionOpRetryTimer = timer;
        this.tcLoadSemaphores = ConcurrentLongHashMap.<Semaphore>newBuilder().build();
        this.pendingConnectRequests =
                ConcurrentLongHashMap.<ConcurrentLinkedDeque<CompletableFuture<Void>>>newBuilder().build();
        this.internalPinnedExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
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
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        internalPinnedExecutor.execute(() -> {
            if (stores.get(tcId) != null) {
                completableFuture.complete(null);
            } else {
                pulsarService.getBrokerService().checkTopicNsOwnership(TopicName
                        .TRANSACTION_COORDINATOR_ASSIGN.getPartition((int) tcId.getId()).toString())
                        .thenRun(() -> internalPinnedExecutor.execute(() -> {
                    final Semaphore tcLoadSemaphore = this.tcLoadSemaphores
                            .computeIfAbsent(tcId.getId(), (id) -> new Semaphore(1));
                    Deque<CompletableFuture<Void>> deque = pendingConnectRequests
                            .computeIfAbsent(tcId.getId(), (id) -> new ConcurrentLinkedDeque<>());
                    if (tcLoadSemaphore.tryAcquire()) {
                        // when tcLoadSemaphore.release(), this command will acquire semaphore,
                        // so we should jude the store exist again.
                        if (stores.get(tcId) != null) {
                            completableFuture.complete(null);
                            tcLoadSemaphore.release();
                            return;
                        }

                        TransactionTimeoutTracker timeoutTracker = timeoutTrackerFactory.newTracker(tcId);
                        TransactionRecoverTracker recoverTracker =
                                new TransactionRecoverTrackerImpl(TransactionMetadataStoreService.this,
                                        timeoutTracker, tcId.getId());
                        openTransactionMetadataStore(tcId, timeoutTracker, recoverTracker).thenAccept(
                                store -> internalPinnedExecutor.execute(() -> {
                                    // TransactionMetadataStore initialization
                                    // need to use TransactionMetadataStore itself.
                                    // we need to put store into stores map before
                                    // handle committing and aborting transaction.
                                    stores.put(tcId, store);
                                    LOG.info("Added new transaction meta store {}", tcId);
                                    recoverTracker.handleCommittingAndAbortingTransaction();
                                    timeoutTracker.start();

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
                                })).exceptionally(e -> {
                            internalPinnedExecutor.execute(() -> {
                                completableFuture.completeExceptionally(e.getCause());
                                // release before handle request queue,
                                //in order to client reconnect infinite loop
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
                            });
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
                })).exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    completableFuture.completeExceptionally(realCause);
                    return null;
                });
            }
        });
        return completableFuture;
    }

    public CompletableFuture<TransactionMetadataStore>
    openTransactionMetadataStore(TransactionCoordinatorID tcId,
                                 TransactionTimeoutTracker timeoutTracker,
                                 TransactionRecoverTracker recoverTracker) {
        return pulsarService.getBrokerService().getManagedLedgerConfig(getMLTransactionLogName(tcId)).thenCompose(
                v -> transactionMetadataStoreProvider.openStore(tcId, pulsarService.getManagedLedgerFactory(), v,
                        timeoutTracker, recoverTracker));
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

    public CompletableFuture<TxnID> newTransaction(TransactionCoordinatorID tcId, long timeoutInMills,
                                                   String owner) {
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.newTransaction(timeoutInMills, owner);
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
        CompletableFuture<Void> future = new CompletableFuture<>();
        endTransaction(txnID, txnAction, isTimeout, future);
        return future;
    }

    public void endTransaction(TxnID txnID, int txnAction, boolean isTimeout,
                                                  CompletableFuture<Void> future) {
        TxnStatus newStatus;
        switch (txnAction) {
            case TxnAction.COMMIT_VALUE:
                newStatus = COMMITTING;
                break;
            case TxnAction.ABORT_VALUE:
                newStatus = ABORTING;
                break;
            default:
                TransactionCoordinatorException.UnsupportedTxnActionException exception =
                        new TransactionCoordinatorException.UnsupportedTxnActionException(txnID, txnAction);
                LOG.error(exception.getMessage());
                future.completeExceptionally(exception);
                return;
        }
        getTxnMeta(txnID)
                .thenCompose(txnMeta -> {
                    if (txnMeta.status() == TxnStatus.OPEN) {
                        return updateTxnStatus(txnID, newStatus, TxnStatus.OPEN, isTimeout)
                                .thenCompose(__ -> endTxnInTransactionBuffer(txnID, txnAction));
                    }
                    return fakeAsyncCheckTxnStatus(txnMeta.status(), txnAction, txnID, newStatus)
                            .thenCompose(__ -> endTxnInTransactionBuffer(txnID, txnAction));
                }).whenComplete((__, ex)-> {
                    if (ex == null) {
                        future.complete(null);
                        return;
                    }
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    if (!isRetryableException(realCause)) {
                        LOG.error("End transaction fail! TxnId : {}, "
                                + "TxnAction : {}", txnID, txnAction, realCause);
                        future.completeExceptionally(ex);
                        return;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("EndTxnInTransactionBuffer retry! TxnId : {}, "
                                + "TxnAction : {}", txnID, txnAction, realCause);
                    }
                    transactionOpRetryTimer.newTimeout(timeout ->
                                    endTransaction(txnID, txnAction, isTimeout, future),
                            endTransactionRetryIntervalTime, TimeUnit.MILLISECONDS);
                });
    }

    private CompletionStage<Void> fakeAsyncCheckTxnStatus(TxnStatus txnStatus, int txnAction,
                                                          TxnID txnID, TxnStatus expectStatus) {
        boolean isLegal;
        switch (txnStatus) {
            case COMMITTING:
                isLegal =  (txnAction == TxnAction.COMMIT.getValue());
                break;
            case ABORTING:
                isLegal =  (txnAction == TxnAction.ABORT.getValue());
                break;
            default:
                isLegal = false;
        }
        if (!isLegal) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("EndTxnInTransactionBuffer op retry! TxnId : {}, TxnAction : {}", txnID, txnAction);
            }
            return FutureUtil.failedFuture(
                    new InvalidTxnStatusException(txnID, expectStatus, txnStatus));
        }
       return CompletableFuture.completedFuture(null);
    }

    // when managedLedger fence will remove this tc and reload
    public void handleOpFail(Throwable e, TransactionCoordinatorID tcId) {
        if (e instanceof ManagedLedgerException.ManagedLedgerFencedException) {
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
        return getTxnMeta(txnID)
                .thenCompose(txnMeta -> {
                    long lowWaterMark = getLowWaterMark(txnID);
                    Stream<CompletableFuture<?>> onSubFutureStream = txnMeta.ackedPartitions().stream().map(tbSub -> {
                        switch (txnAction) {
                            case TxnAction.COMMIT_VALUE:
                                return tbClient.commitTxnOnSubscription(
                                        tbSub.getTopic(), tbSub.getSubscription(), txnID.getMostSigBits(),
                                        txnID.getLeastSigBits(), lowWaterMark);
                            case TxnAction.ABORT_VALUE:
                                return tbClient.abortTxnOnSubscription(
                                        tbSub.getTopic(), tbSub.getSubscription(), txnID.getMostSigBits(),
                                        txnID.getLeastSigBits(), lowWaterMark);
                            default:
                                return FutureUtil.failedFuture(
                                        new IllegalStateException("Unsupported txnAction " + txnAction));
                        }
                    });
                    Stream<CompletableFuture<?>> onTopicFutureStream =
                            txnMeta.producedPartitions().stream().map(partition -> {
                                switch (txnAction) {
                                    case TxnAction.COMMIT_VALUE:
                                        return tbClient.commitTxnOnTopic(partition, txnID.getMostSigBits(),
                                                txnID.getLeastSigBits(), lowWaterMark);
                                    case TxnAction.ABORT_VALUE:
                                        return tbClient.abortTxnOnTopic(partition, txnID.getMostSigBits(),
                                            txnID.getLeastSigBits(), lowWaterMark);
                                    default:
                                        return FutureUtil.failedFuture(
                                                new IllegalStateException("Unsupported txnAction " + txnAction));
                        }
                    });
                    return FutureUtil.waitForAll(Stream.concat(onSubFutureStream, onTopicFutureStream)
                                    .collect(Collectors.toList()))
                            .thenCompose(__ -> endTxnInTransactionMetadataStore(txnID, txnAction));
                });
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

    public CompletableFuture<Boolean> verifyTxnOwnership(TxnID txnID, String checkOwner) {
        return getTxnMeta(txnID)
                .thenCompose(meta -> {
                    // owner was null in the old versions or no auth enabled
                    if (meta.getOwner() == null) {
                        return CompletableFuture.completedFuture(true);
                    }
                    if (meta.getOwner().equals(checkOwner)) {
                        return CompletableFuture.completedFuture(true);
                    }
                    return CompletableFuture.completedFuture(false);
                });
    }


    public void close () {
        this.internalPinnedExecutor.shutdown();
        stores.forEach((tcId, metadataStore) -> {
            metadataStore.closeAsync().whenComplete((v, ex) -> {
                if (ex != null) {
                    LOG.error("Close transaction metadata store with id " + tcId, ex);
                } else {
                    LOG.info("Removed and closed transaction meta store {}", tcId);
                }
            });
        });
        stores.clear();
    }
}
