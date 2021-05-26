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

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static javax.ws.rs.core.Response.Status.TEMPORARY_REDIRECT;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.Transactions;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStats;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionMetadata;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.CoordinatorNotFoundException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.TransactionNotFoundException;

@Slf4j
public abstract class TransactionsBase extends AdminResource {

    protected void internalGetCoordinatorStats(AsyncResponse asyncResponse, boolean authoritative,
                                               Integer coordinatorId) {
        if (pulsar().getConfig().isTransactionCoordinatorEnabled()) {
            if (coordinatorId != null) {
                validateTopicOwnership(TopicName.TRANSACTION_COORDINATOR_ASSIGN.getPartition(coordinatorId),
                        authoritative);
                asyncResponse.resume(pulsar().getTransactionMetadataStoreService().getStores()
                        .get(TransactionCoordinatorID.get(coordinatorId)).getCoordinatorStats());
            } else {
                getPartitionedTopicMetadataAsync(TopicName.TRANSACTION_COORDINATOR_ASSIGN,
                        false, false).thenAccept(partitionMetadata -> {
                    if (partitionMetadata.partitions == 0) {
                        asyncResponse.resume(new RestException(Response.Status.NOT_FOUND,
                                "Transaction coordinator not found"));
                        return;
                    }
                    List<CompletableFuture<TransactionCoordinatorStats>> transactionMetadataStoreInfoFutures =
                            Lists.newArrayList();
                    for (int i = 0; i < partitionMetadata.partitions; i++) {
                        try {
                            transactionMetadataStoreInfoFutures
                                    .add(pulsar().getAdminClient().transactions().getCoordinatorStatsById(i));
                        } catch (PulsarServerException e) {
                            asyncResponse.resume(new RestException(e));
                            return;
                        }
                    }
                    Map<Integer, TransactionCoordinatorStats> stats = new HashMap<>();
                    FutureUtil.waitForAll(transactionMetadataStoreInfoFutures).whenComplete((result, e) -> {
                        if (e != null) {
                            asyncResponse.resume(new RestException(e));
                            return;
                        }

                        for (int i = 0; i < transactionMetadataStoreInfoFutures.size(); i++) {
                            try {
                                stats.put(i, transactionMetadataStoreInfoFutures.get(i).get());
                            } catch (Exception exception) {
                                asyncResponse.resume(new RestException(exception.getCause()));
                                return;
                            }
                        }

                        asyncResponse.resume(stats);
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

    protected void internalGetTransactionInPendingAckStats(AsyncResponse asyncResponse, boolean authoritative,
                                                           long mostSigBits, long leastSigBits, String topic,
                                                           String subName) {
        if (pulsar().getConfig().isTransactionCoordinatorEnabled()) {
            validateTopicOwnership(TopicName.get(topic), authoritative);
            CompletableFuture<Optional<Topic>> topicFuture = pulsar().getBrokerService()
                    .getTopics().get(TopicName.get(topic).toString());
            if (topicFuture != null) {
                topicFuture.whenComplete((optionalTopic, e) -> {
                    if (e != null) {
                        asyncResponse.resume(new RestException(e));
                        return;
                    }
                    if (!optionalTopic.isPresent()) {
                        asyncResponse.resume(new RestException(TEMPORARY_REDIRECT,
                                "Topic is not owned by this broker!"));
                        return;
                    }
                    Topic topicObject = optionalTopic.get();
                    if (topicObject instanceof PersistentTopic) {
                        asyncResponse.resume(((PersistentTopic) topicObject)
                                .getTransactionInPendingAckStats(new TxnID(mostSigBits, leastSigBits), subName));
                    } else {
                        asyncResponse.resume(new RestException(BAD_REQUEST, "Topic is not a persistent topic!"));
                    }
                });
            } else {
                asyncResponse.resume(new RestException(TEMPORARY_REDIRECT, "Topic is not owned by this broker!"));
            }
        } else {
            asyncResponse.resume(new RestException(SERVICE_UNAVAILABLE,
                    "This Broker is not configured with transactionCoordinatorEnabled=true."));
        }
    }

    protected void internalGetTransactionInBufferStats(AsyncResponse asyncResponse, boolean authoritative,
                                                       long mostSigBits, long leastSigBits,
                                                       String topic) {
        if (pulsar().getConfig().isTransactionCoordinatorEnabled()) {
            validateTopicOwnership(TopicName.get(topic), authoritative);
            CompletableFuture<Optional<Topic>> topicFuture = pulsar().getBrokerService()
                    .getTopics().get(TopicName.get(topic).toString());
            if (topicFuture != null) {
                topicFuture.whenComplete((optionalTopic, e) -> {
                    if (e != null) {
                        asyncResponse.resume(new RestException(e));
                        return;
                    }
                    if (!optionalTopic.isPresent()) {
                        asyncResponse.resume(new RestException(TEMPORARY_REDIRECT,
                                "Topic is not owned by this broker!"));
                        return;
                    }
                    Topic topicObject = optionalTopic.get();
                    if (topicObject instanceof PersistentTopic) {
                        TransactionInBufferStats transactionInBufferStats = ((PersistentTopic) topicObject)
                                .getTransactionInBufferStats(new TxnID(mostSigBits, leastSigBits));
                        asyncResponse.resume(transactionInBufferStats);
                    } else {
                        asyncResponse.resume(new RestException(BAD_REQUEST, "Topic is not a persistent topic!"));
                    }
                });
            } else {
                asyncResponse.resume(new RestException(TEMPORARY_REDIRECT, "Topic is not owned by this broker!"));
            }
        } else {
            asyncResponse.resume(new RestException(SERVICE_UNAVAILABLE,
                    "This Broker is not configured with transactionCoordinatorEnabled=true."));
        }
    }

    protected void internalGetTransactionBufferStats(AsyncResponse asyncResponse,
                                                     boolean authoritative, String topic) {
        if (pulsar().getConfig().isTransactionCoordinatorEnabled()) {
            validateTopicOwnership(TopicName.get(topic), authoritative);
            CompletableFuture<Optional<Topic>> topicFuture = pulsar().getBrokerService()
                    .getTopics().get(TopicName.get(topic).toString());
            if (topicFuture != null) {
                topicFuture.whenComplete((optionalTopic, e) -> {
                    if (e != null) {
                        asyncResponse.resume(new RestException(e));
                        return;
                    }

                    if (!optionalTopic.isPresent()) {
                        asyncResponse.resume(new RestException(TEMPORARY_REDIRECT,
                                "Topic is not owned by this broker!"));
                        return;
                    }
                    Topic topicObject = optionalTopic.get();
                    if (topicObject instanceof PersistentTopic) {
                        asyncResponse.resume(((PersistentTopic) topicObject).getTransactionBufferStats());
                    } else {
                        asyncResponse.resume(new RestException(BAD_REQUEST, "Topic is not a persistent topic!"));
                    }
                });
            } else {
                asyncResponse.resume(new RestException(TEMPORARY_REDIRECT, "Topic is not owned by this broker!"));
            }
        } else {
            asyncResponse.resume(new RestException(SERVICE_UNAVAILABLE, "Broker don't support transaction!"));
        }
    }

    protected void internalGetPendingAckStats(AsyncResponse asyncResponse, boolean authoritative,
                                              String topic, String subName) {
        if (pulsar().getConfig().isTransactionCoordinatorEnabled()) {
            validateTopicOwnership(TopicName.get(topic), authoritative);
            CompletableFuture<Optional<Topic>> topicFuture = pulsar().getBrokerService()
                    .getTopics().get(TopicName.get(topic).toString());
            if (topicFuture != null) {
                topicFuture.whenComplete((optionalTopic, e) -> {
                    if (e != null) {
                        asyncResponse.resume(new RestException(e));
                        return;
                    }

                    if (!optionalTopic.isPresent()) {
                        asyncResponse.resume(new RestException(TEMPORARY_REDIRECT,
                                "Topic is not owned by this broker!"));
                        return;
                    }
                    Topic topicObject = optionalTopic.get();
                    if (topicObject instanceof PersistentTopic) {
                        asyncResponse.resume(((PersistentTopic) topicObject).getTransactionPendingAckStats(subName));
                    } else {
                        asyncResponse.resume(new RestException(BAD_REQUEST, "Topic is not a persistent topic!"));
                    }
                });
            } else {
                asyncResponse.resume(new RestException(TEMPORARY_REDIRECT, "Topic is not owned by this broker!"));
            }
        } else {
            asyncResponse.resume(new RestException(SERVICE_UNAVAILABLE, "Broker don't support transaction!"));
        }
    }

    protected void internalGetTransactionMetadata(AsyncResponse asyncResponse,
                                                  boolean authoritative, int coordinatorId, long sequenceID) {
        try {
            if (pulsar().getConfig().isTransactionCoordinatorEnabled()) {
                validateTopicOwnership(TopicName.TRANSACTION_COORDINATOR_ASSIGN.getPartition(coordinatorId),
                        authoritative);
                Transactions transactions = pulsar().getAdminClient().transactions();
                TxnMeta txnMeta = pulsar().getTransactionMetadataStoreService()
                        .getTxnMeta(new TxnID(coordinatorId, sequenceID)).get();
                TxnID txnID = txnMeta.id();
                TransactionMetadata transactionMetadata = new TransactionMetadata();
                transactionMetadata.txnId = txnID.toString();
                transactionMetadata.status = txnMeta.status().name();
                transactionMetadata.openTimestamp = txnMeta.getOpenTimestamp();
                transactionMetadata.timeoutAt = txnMeta.getTimeoutAt();

                List<CompletableFuture<TransactionInPendingAckStats>> ackedPartitionsFutures = new ArrayList<>();
                Map<String, Map<String, CompletableFuture<TransactionInPendingAckStats>>> ackFutures = new HashMap<>();
                txnMeta.ackedPartitions().forEach(transactionSubscription -> {
                    String topic = transactionSubscription.getTopic();
                    String subName = transactionSubscription.getSubscription();
                    CompletableFuture<TransactionInPendingAckStats> future =
                            transactions.getTransactionInPendingAckStats(txnID, topic, subName);
                    ackedPartitionsFutures.add(future);
                    if (ackFutures.containsKey(topic)) {
                        ackFutures.get(topic)
                                .put(transactionSubscription.getSubscription(), future);
                    } else {
                        Map<String, CompletableFuture<TransactionInPendingAckStats>> pendingAckStatsMap =
                                new HashMap<>();
                        pendingAckStatsMap.put(transactionSubscription.getSubscription(), future);
                        ackFutures.put(topic, pendingAckStatsMap);
                    }
                });

                List<CompletableFuture<TransactionInBufferStats>> producedPartitionsFutures = new ArrayList<>();
                Map<String, CompletableFuture<TransactionInBufferStats>> produceFutures = new HashMap<>();
                txnMeta.producedPartitions().forEach(topic -> {
                    CompletableFuture<TransactionInBufferStats> future =
                            transactions.getTransactionInBufferStats(txnID, topic);
                    producedPartitionsFutures.add(future);
                    produceFutures.put(topic, future);

                });

                FutureUtil.waitForAll(ackedPartitionsFutures).whenComplete((v, e) -> {
                    if (e != null) {
                        asyncResponse.resume(new RestException(e));
                        return;
                    }
                    FutureUtil.waitForAll(producedPartitionsFutures).whenComplete((x, t) -> {
                        if (t != null) {
                            asyncResponse.resume(new RestException(t));
                            return;
                        }

                        Map<String, Map<String, TransactionInPendingAckStats>> ackedPartitions = new HashMap<>();
                        Map<String, TransactionInBufferStats> producedPartitions = new HashMap<>();

                        for (String topic : ackFutures.keySet()) {
                            Map<String, TransactionInPendingAckStats> subs = new HashMap<>();
                            for (String sub : ackFutures.get(topic).keySet()) {
                                try {
                                    subs.put(sub, ackFutures.get(topic).get(sub).get());
                                } catch (Exception exception) {
                                    asyncResponse.resume(new RestException(exception.getCause()));
                                    return;
                                }
                            }

                            ackedPartitions.put(topic, subs);
                        }

                        for (String topic : produceFutures.keySet()) {
                            try {
                                producedPartitions.put(topic, produceFutures.get(topic).get());
                            } catch (Exception exception) {
                                asyncResponse.resume(new RestException(exception.getCause()));
                                return;
                            }
                        }
                        transactionMetadata.ackedPartitions = ackedPartitions;
                        transactionMetadata.producedPartitions = producedPartitions;
                        asyncResponse.resume(transactionMetadata);
                    });
                });
            } else {
                asyncResponse.resume(new RestException(SERVICE_UNAVAILABLE,
                        "This Broker is not configured with transactionCoordinatorEnabled=true."));
            }
        } catch (Exception e) {
            if (e instanceof ExecutionException) {
                if (e.getCause() instanceof CoordinatorNotFoundException
                        || e.getCause() instanceof TransactionNotFoundException) {
                    asyncResponse.resume(new RestException(NOT_FOUND, e.getCause()));
                    return;
                }
                asyncResponse.resume(new RestException(e.getCause()));
            } else {
                asyncResponse.resume(new RestException(e));
            }
        }
    }
}
