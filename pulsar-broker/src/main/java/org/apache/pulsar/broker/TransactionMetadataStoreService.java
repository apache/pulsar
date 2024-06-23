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
package org.apache.pulsar.broker;

import static org.apache.commons.lang3.StringUtils.isBlank;
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
import java.util.concurrent.CompletionException;
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
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataPreserver;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.TransactionSubscription;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTrackerFactory;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.CoordinatorNotFoundException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.TransactionMetadataStoreStateException;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataPreserverImpl;
import org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriterConfig;
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
    // one connect request opens the transactionMetaStore the other request will add to the queue, when the open op
    // finishes the request will be polled and will complete the future
    private final ConcurrentLongHashMap<ConcurrentLinkedDeque<CompletableFuture<Void>>> pendingConnectRequests;
    private final ExecutorService internalPinnedExecutor;

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
        this.tcLoadSemaphores = ConcurrentLongHashMap.<Semaphore>newBuilder().build();
        this.pendingConnectRequests =
                ConcurrentLongHashMap.<ConcurrentLinkedDeque<CompletableFuture<Void>>>newBuilder().build();
        ThreadFactory threadFactory =
                new ExecutorProvider.ExtendedThreadFactory("transaction-coordinator-thread-factory");
        this.internalPinnedExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    public CompletableFuture<Void> handleTcClientConnect(TransactionCoordinatorID tcId) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        internalPinnedExecutor.execute(() -> {
            if (stores.get(tcId) != null) {
                completableFuture.complete(null);
            } else {
                pulsarService.getBrokerService().checkTopicNsOwnership(SystemTopicNames
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
                        TransactionMetadataPreserver preserver;
                        try {
                            preserver = new MLTransactionMetadataPreserverImpl(tcId,
                                    pulsarService.getConfiguration().getTransactionMetaPersistCount(),
                                    pulsarService.getConfiguration().getTransactionMetaPersistTimeInHour(),
                                    pulsarService.getConfiguration().getTransactionMetaExpireCheckIntervalInSecond(),
                                    pulsarService.getClient());
                            preserver.replay();
                        } catch (Throwable e) {
                            LOG.error("Failed to create transaction metadata preserver for tcId {}, reason:{}",
                                    tcId, e);
                            completableFuture.completeExceptionally(e);
                            tcLoadSemaphore.release();
                            failPendingConnectRequests(e, deque);
                            return;
                        }
                        openTransactionMetadataStore(tcId, preserver, timeoutTracker, recoverTracker).thenAccept(
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
                                Throwable realCause = FutureUtil.unwrapCompletionException(e);
                                completableFuture.completeExceptionally(realCause);
                                // release before handle request queue,
                                //in order to client reconnect infinite loop
                                tcLoadSemaphore.release();
                                failPendingConnectRequests(e, deque);
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
                            LOG.debug("Handle tc client connect added into pending queue! tcId : {}", tcId);
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

    private void failPendingConnectRequests(Throwable e, Deque<CompletableFuture<Void>> deque) {
        Throwable realCause = FutureUtil.unwrapCompletionException(e);
        long endTime = System.currentTimeMillis() + HANDLE_PENDING_CONNECT_TIME_OUT;
        while (true) {
            // prevent thread in a busy loop.
            if (System.currentTimeMillis() < endTime) {
                CompletableFuture<Void> future = deque.poll();
                if (future != null) {
                    // this means that this tc client connection connect fail
                    future.completeExceptionally(realCause);
                } else {
                    break;
                }
            } else {
                deque.clear();
                break;
            }
        }
    }

    public CompletableFuture<TransactionMetadataStore>
    openTransactionMetadataStore(TransactionCoordinatorID tcId,
                                 TransactionMetadataPreserver preserver,
                                 TransactionTimeoutTracker timeoutTracker,
                                 TransactionRecoverTracker recoverTracker) {
        final Timer brokerClientSharedTimer = pulsarService.getBrokerClientSharedTimer();
        final ServiceConfiguration serviceConfiguration = pulsarService.getConfiguration();
        final TxnLogBufferedWriterConfig txnLogBufferedWriterConfig = new TxnLogBufferedWriterConfig();
        txnLogBufferedWriterConfig.setBatchEnabled(serviceConfiguration.isTransactionLogBatchedWriteEnabled());
        txnLogBufferedWriterConfig
                .setBatchedWriteMaxRecords(serviceConfiguration.getTransactionLogBatchedWriteMaxRecords());
        txnLogBufferedWriterConfig.setBatchedWriteMaxSize(serviceConfiguration.getTransactionLogBatchedWriteMaxSize());
        txnLogBufferedWriterConfig
                .setBatchedWriteMaxDelayInMillis(serviceConfiguration.getTransactionLogBatchedWriteMaxDelayInMillis());

        return pulsarService.getBrokerService().getManagedLedgerConfig(getMLTransactionLogName(tcId)).thenCompose(
                v -> transactionMetadataStoreProvider.openStore(tcId, pulsarService.getManagedLedgerFactory(), v,
                        timeoutTracker, recoverTracker, preserver,
                        pulsarService.getConfig().getMaxActiveTransactionsPerCoordinator(), txnLogBufferedWriterConfig,
                        brokerClientSharedTimer));
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
        return newTransaction(tcId, timeoutInMills, owner, null);
    }

    public CompletableFuture<TxnID> newTransaction(TransactionCoordinatorID tcId, long timeoutInMills,
                                                   String owner, String clientName) {
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.newTransaction(timeoutInMills, owner, clientName);
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

    public CompletableFuture<TxnMeta> getTxnMeta(TxnID txnId, String clientName) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnId);
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.getTxnMeta(txnId, clientName);
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

    public CompletableFuture<Void> appendTxnMetaToPreserver(TxnID txnID, TxnMeta txnMeta, String clientName) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnID);
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.appendTxnMetaToPreserver(txnMeta, clientName);
    }

    public CompletableFuture<Void> endTransaction(TxnID txnID, int txnAction, boolean isTimeout) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        endTransaction(txnID, txnAction, isTimeout, future, null);
        return future;
    }

    public CompletableFuture<Void> endTransaction(TxnID txnID, int txnAction,
                                                  boolean isTimeout, String clientName) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        endTransaction(txnID, txnAction, isTimeout, future, clientName);
        return future;
    }


    public void endTransaction(TxnID txnID, int txnAction, boolean isTimeout,
                               CompletableFuture<Void> future, String clientName) {
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
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnID);
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            future.completeExceptionally(new CoordinatorNotFoundException(tcId));
            return;
        }
        getTxnMeta(txnID)
                .thenCompose(txnMeta -> {
                    if (txnMeta.status() == TxnStatus.OPEN) {
                        return updateTxnStatus(txnID, newStatus, TxnStatus.OPEN, isTimeout)
                                .thenCompose(__ -> appendTxnMetaToPreserver(txnID, txnMeta, clientName))
                                .thenCompose(__ -> endTxnInTransactionBuffer(txnID, txnAction));
                    } else if (txnMeta.status() == TxnStatus.COMMITTED
                            && txnAction == TxnAction.COMMIT_VALUE) {
                        future.complete(null);
                        return future;
                    } else if (txnMeta.status() == TxnStatus.ABORTED
                            && txnAction == TxnAction.ABORT_VALUE) {
                        future.complete(null);
                        return future;
                    }

                    return fakeAsyncCheckTxnStatus(txnMeta.status(), txnAction, txnID, newStatus)
                            .thenCompose(__ -> appendTxnMetaToPreserver(txnID, txnMeta, clientName))
                            .thenCompose(__ -> endTxnInTransactionBuffer(txnID, txnAction));
                }).whenComplete((__, ex) -> {
                    if (ex == null) {
                        future.complete(null);
                        return;
                    }
                    if (!isRetryableException(ex)) {
                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                        if (realCause instanceof CoordinatorException.TransactionNotFoundException
                                && !isBlank(clientName) && store.transactionMetadataPreserverEnabled()) {
                            TxnMeta txnMeta = store.getTxnMetaFromPreserver(txnID, clientName);
                            if (txnAction == TxnAction.COMMIT_VALUE && txnMeta != null
                                    && (txnMeta.status() == TxnStatus.COMMITTED
                                    || txnMeta.status() == TxnStatus.COMMITTING)) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("try to commit a transaction that is already committed. "
                                            + "TxnId : {}, clientName:{}.", txnID, clientName);
                                }
                                future.complete(null);
                                return;
                            } else if (txnAction == TxnAction.ABORT_VALUE && txnMeta != null
                                    && (txnMeta.status() == TxnStatus.ABORTED
                                    || txnMeta.status() == TxnStatus.ABORTING)) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("try to abort a transaction that is already aborted. "
                                            + "TxnId : {}, clientName:{}.", txnID, clientName);
                                }
                                future.complete(null);
                                return;
                            } else if (txnMeta != null) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("try to commit a aborted txn or abort a committed txn. " +
                                            "TxnId : {}, clientName:{}.", txnID, clientName);
                                }
                                TxnStatus expectStatus = txnAction == TxnAction.COMMIT_VALUE
                                        ? TxnStatus.COMMITTED : TxnStatus.ABORTED;
                                future.completeExceptionally(new CompletionException(
                                        new InvalidTxnStatusException(txnID, expectStatus, txnMeta.status())));
                                return;
                            }
                        }
                        LOG.error("End transaction fail! TxnId : {}, "
                                + "TxnAction : {}", txnID, txnAction, ex);
                        future.completeExceptionally(ex);
                        return;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("EndTxnInTransactionBuffer retry! TxnId : {}, "
                                + "TxnAction : {}", txnID, txnAction, ex);
                    }
                    transactionOpRetryTimer.newTimeout(timeout ->
                                    endTransaction(txnID, txnAction, isTimeout, future, clientName),
                            endTransactionRetryIntervalTime, TimeUnit.MILLISECONDS);
                });
    }

    private CompletionStage<Void> fakeAsyncCheckTxnStatus(TxnStatus txnStatus, int txnAction,
                                                          TxnID txnID, TxnStatus expectStatus) {
        boolean isLegal = switch (txnStatus) {
            case COMMITTING -> (txnAction == TxnAction.COMMIT.getValue());
            case ABORTING -> (txnAction == TxnAction.ABORT.getValue());
            default -> false;
        };
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Transaction timeout! TxnId : {}", txnID);
        }
        final String[] clientName = {null};
        getTxnMeta(txnID).thenCompose(txnMeta -> {
            clientName[0] = txnMeta.getClientName();
            if (txnMeta.status() == TxnStatus.OPEN) {
                return endTransaction(txnID, TxnAction.ABORT_VALUE, true, clientName[0]);
            } else {
                return null;
            }
        }).exceptionally(e -> {
            if (isRetryableException(e)) {
                endTransaction(txnID, TxnAction.ABORT_VALUE, true, clientName[0]);
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

    private static boolean isRetryableException(Throwable ex) {
        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
        return (realCause instanceof TransactionMetadataStoreStateException
                || realCause instanceof RequestTimeoutException
                || realCause instanceof ManagedLedgerException
                || realCause instanceof BrokerPersistenceException
                || realCause instanceof LookupException
                || realCause instanceof ReachMaxPendingOpsException
                || realCause instanceof ConnectException)
                && !(realCause instanceof ManagedLedgerException.ManagedLedgerFencedException);
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
        return verifyTxnOwnership(txnID, checkOwner, null);
    }

    public CompletableFuture<Boolean> verifyTxnOwnership(TxnID txnID, String checkOwner, String clientName) {
        return getTxnMeta(txnID, clientName)
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
        stores.forEach((tcId, metadataStore) ->
            metadataStore.closeAsync().whenComplete((v, ex) -> {
                if (ex != null) {
                    LOG.error("Close transaction metadata store with id " + tcId, ex);
                } else {
                    LOG.info("Removed and closed transaction meta store {}", tcId);
                }
        }));
        stores.clear();
    }
}
