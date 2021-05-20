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
package org.apache.pulsar.broker.admin.impl;

import static javax.ws.rs.core.Response.Status.METHOD_NOT_ALLOWED;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.net.BookieId;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStatus;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;

@Slf4j
public abstract class TransactionsBase extends AdminResource {

    protected void internalGetCoordinatorStatus(AsyncResponse asyncResponse, boolean authoritative,
                                                Integer coordinatorId) {
        if (pulsar().getConfig().isTransactionCoordinatorEnabled()) {
            if (coordinatorId != null) {
                validateTopicOwnership(TopicName.TRANSACTION_COORDINATOR_ASSIGN.getPartition(coordinatorId),
                        authoritative);
                asyncResponse.resume(pulsar().getTransactionMetadataStoreService().getStores()
                        .get(TransactionCoordinatorID.get(coordinatorId)).getStatus());
            } else {
                getPartitionedTopicMetadataAsync(TopicName.TRANSACTION_COORDINATOR_ASSIGN,
                        false, false).thenAccept(partitionMetadata -> {
                    if (partitionMetadata.partitions == 0) {
                        asyncResponse.resume(new RestException(Response.Status.NOT_FOUND,
                                "Transaction coordinator not found"));
                        return;
                    }
                    List<CompletableFuture<TransactionCoordinatorStatus>> transactionMetadataStoreInfoFutures =
                            Lists.newArrayList();
                    for (int i = 0; i < partitionMetadata.partitions; i++) {
                        try {
                            transactionMetadataStoreInfoFutures
                                    .add(pulsar().getAdminClient().transactions().getCoordinatorStatusById(i));
                        } catch (PulsarServerException e) {
                            asyncResponse.resume(new RestException(e));
                            return;
                        }
                    }
                    List<TransactionCoordinatorStatus> metadataStoreInfoList = new ArrayList<>();
                    FutureUtil.waitForAll(transactionMetadataStoreInfoFutures).whenComplete((result, e) -> {
                        if (e != null) {
                            asyncResponse.resume(new RestException(e));
                            return;
                        }

                        for (CompletableFuture<TransactionCoordinatorStatus> transactionMetadataStoreInfoFuture
                                : transactionMetadataStoreInfoFutures) {
                            try {
                                metadataStoreInfoList.add(transactionMetadataStoreInfoFuture.get());
                            } catch (Exception exception) {
                                asyncResponse.resume(new RestException(exception.getCause()));
                                return;
                            }
                        }
                        asyncResponse.resume(metadataStoreInfoList);
                    });
                }).exceptionally(ex -> {
                    log.error("[{}] Failed to get transaction coordinator state.", clientAppId(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
            }
        } else {
            asyncResponse.resume(new RestException(SERVICE_UNAVAILABLE,
                    "This Broker is not configured with transactionCoordinatorEnabled=true."));
        }
    }

    protected void internalGetCoordinatorInternalStats(AsyncResponse asyncResponse, boolean authoritative,
                                                       boolean metadata, int coordinatorId) {
        try {
            if (pulsar().getConfig().isTransactionCoordinatorEnabled()) {
                TopicName topicName = TopicName.TRANSACTION_COORDINATOR_ASSIGN.getPartition(coordinatorId);
                validateTopicOwnership(topicName, authoritative);
                TransactionMetadataStore metadataStore = pulsar().getTransactionMetadataStoreService()
                        .getStores().get(TransactionCoordinatorID.get(coordinatorId));
                if (metadataStore instanceof MLTransactionMetadataStore) {
                    asyncResponse.resume(getManageLedgerInternalStats(
                            ((MLTransactionMetadataStore) metadataStore).getManagedLedger(),
                            metadata, topicName.toString()).get());
                } else {
                    asyncResponse.resume(new RestException(METHOD_NOT_ALLOWED,
                            "Broker don't use MLTransactionMetadataStore!"));
                }
            } else {
                asyncResponse.resume(new RestException(SERVICE_UNAVAILABLE,
                        "This Broker is not configured with transactionCoordinatorEnabled=true."));
            }
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e.getCause()));
        }
    }

    private CompletableFuture<ManagedLedgerInternalStats> getManageLedgerInternalStats(ManagedLedger ledger,
                                                                                       boolean includeLedgerMetadata, String topic) {
        CompletableFuture<ManagedLedgerInternalStats> statFuture = new CompletableFuture<>();
        ManagedLedgerInternalStats stats = new ManagedLedgerInternalStats();

        ManagedLedgerImpl ml = (ManagedLedgerImpl) ledger;
        stats.entriesAddedCounter = ml.getEntriesAddedCounter();
        stats.numberOfEntries = ml.getNumberOfEntries();
        stats.totalSize = ml.getTotalSize();
        stats.currentLedgerEntries = ml.getCurrentLedgerEntries();
        stats.currentLedgerSize = ml.getCurrentLedgerSize();
        stats.lastLedgerCreatedTimestamp = DateFormatter.format(ml.getLastLedgerCreatedTimestamp());
        if (ml.getLastLedgerCreationFailureTimestamp() != 0) {
            stats.lastLedgerCreationFailureTimestamp = DateFormatter.format(ml.getLastLedgerCreationFailureTimestamp());
        }

        stats.waitingCursorsCount = ml.getWaitingCursorsCount();
        stats.pendingAddEntriesCount = ml.getPendingAddEntriesCount();

        stats.lastConfirmedEntry = ml.getLastConfirmedEntry().toString();
        stats.state = ml.getState();

        stats.ledgers = Lists.newArrayList();
        pulsar().getAvailableBookiesAsync().whenComplete((bookies, e) -> {
            if (e != null) {
                log.error("[{}] Failed to fetch available bookies.", topic, e);
                statFuture.completeExceptionally(e);
            } else {
                ml.getLedgersInfo().forEach((id, li) -> {
                    PersistentTopicInternalStats.LedgerInfo info = new PersistentTopicInternalStats.LedgerInfo();
                    info.ledgerId = li.getLedgerId();
                    info.entries = li.getEntries();
                    info.size = li.getSize();
                    info.offloaded = li.hasOffloadContext() && li.getOffloadContext().getComplete();
                    stats.ledgers.add(info);
                    if (includeLedgerMetadata) {
                        ml.getLedgerMetadata(li.getLedgerId()).handle((lMetadata, ex) -> {
                            if (ex == null) {
                                info.metadata = lMetadata;
                            }
                            return null;
                        });
                        ml.getEnsemblesAsync(li.getLedgerId()).handle((ensembles, ex) -> {
                            if (ex == null) {
                                info.underReplicated = !bookies.containsAll(ensembles.stream().map(BookieId::toString)
                                        .collect(Collectors.toList()));
                            }
                            return null;
                        });
                    }

                    stats.cursors = Maps.newTreeMap();
                    ml.getCursors().forEach(c -> {
                        ManagedCursorImpl cursor = (ManagedCursorImpl) c;
                        PersistentTopicInternalStats.CursorStats cs = new PersistentTopicInternalStats.CursorStats();
                        cs.markDeletePosition = cursor.getMarkDeletedPosition().toString();
                        cs.readPosition = cursor.getReadPosition().toString();
                        cs.waitingReadOp = cursor.hasPendingReadRequest();
                        cs.pendingReadOps = cursor.getPendingReadOpsCount();
                        cs.messagesConsumedCounter = cursor.getMessagesConsumedCounter();
                        cs.cursorLedger = cursor.getCursorLedger();
                        cs.cursorLedgerLastEntry = cursor.getCursorLedgerLastEntry();
                        cs.individuallyDeletedMessages = cursor.getIndividuallyDeletedMessages();
                        cs.lastLedgerSwitchTimestamp = DateFormatter.format(cursor.getLastLedgerSwitchTimestamp());
                        cs.state = cursor.getState();
                        cs.numberOfEntriesSinceFirstNotAckedMessage =
                                cursor.getNumberOfEntriesSinceFirstNotAckedMessage();
                        cs.totalNonContiguousDeletedMessagesRange = cursor.getTotalNonContiguousDeletedMessagesRange();
                        cs.properties = cursor.getProperties();
                        stats.cursors.put(cursor.getName(), cs);
                    });
                });
                statFuture.complete(stats);
            }
        });

        return statFuture;
    }
}
