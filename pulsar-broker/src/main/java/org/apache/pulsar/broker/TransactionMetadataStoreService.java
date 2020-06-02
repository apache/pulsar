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

import com.google.common.annotations.VisibleForTesting;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.CoordinatorNotFoundException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionMetadataStoreService {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionMetadataStoreService.class);

    private final Map<TransactionCoordinatorID, TransactionMetadataStore> stores;
    private final TransactionMetadataStoreProvider transactionMetadataStoreProvider;
    private final PulsarService pulsarService;

    public TransactionMetadataStoreService(TransactionMetadataStoreProvider transactionMetadataStoreProvider,
                                           PulsarService pulsarService) {
        this.pulsarService = pulsarService;
        this.stores = new ConcurrentHashMap<>();
        this.transactionMetadataStoreProvider = transactionMetadataStoreProvider;
    }

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
                                    addTransactionMetadataStore(TransactionCoordinatorID.get(name.getPartitionIndex()));
                                }
                            }
                        } else {
                            LOG.error("Failed to get owned topic list when triggering on-loading bundle {}.", bundle, ex);
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
                                    removeTransactionMetadataStore(TransactionCoordinatorID.get(name.getPartitionIndex()));
                                }
                            }
                        } else {
                            LOG.error("Failed to get owned topic list error when triggering un-loading bundle {}.", bundle, ex);
                        }
                     });
            }
            @Override
            public boolean test(NamespaceBundle namespaceBundle) {
                return namespaceBundle.getNamespaceObject().equals(NamespaceName.SYSTEM_NAMESPACE);
            }
        });
    }

    public void addTransactionMetadataStore(TransactionCoordinatorID tcId) {
        transactionMetadataStoreProvider.openStore(tcId, pulsarService.getManagedLedgerFactory())
            .whenComplete((store, ex) -> {
                if (ex != null) {
                    LOG.error("Add transaction metadata store with id {} error", tcId.getId(), ex);
                } else {
                    stores.put(tcId, store);
                    LOG.info("Added new transaction meta store {}", tcId);
                }
            });
    }

    public void removeTransactionMetadataStore(TransactionCoordinatorID tcId) {
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
    }

    public CompletableFuture<TxnID> newTransaction(TransactionCoordinatorID tcId) {
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.newTransaction();
    }

    public CompletableFuture<Void> addProducedPartitionToTxn(TxnID txnId, List<String> partitions) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnId);
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.addProducedPartitionToTxn(txnId, partitions);
    }

    public CompletableFuture<Void> addAckedPartitionToTxn(TxnID txnId, List<String> partitions) {
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

    public CompletableFuture<Void> updateTxnStatus(TxnID txnId, TxnStatus newStatus, TxnStatus expectedStatus) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnId);
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.updateTxnStatus(txnId, newStatus, expectedStatus);
    }

    private TransactionCoordinatorID getTcIdFromTxnId(TxnID txnId) {
        return new TransactionCoordinatorID(txnId.getMostSigBits());
    }

    public TransactionMetadataStoreProvider getTransactionMetadataStoreProvider() {
        return transactionMetadataStoreProvider;
    }

    @VisibleForTesting
    public Map<TransactionCoordinatorID, TransactionMetadataStore> getStores() {
        return Collections.unmodifiableMap(stores);
    }
}
