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

import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStatus;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;

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
}
