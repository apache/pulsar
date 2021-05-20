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
package org.apache.pulsar.client.admin.internal;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import org.apache.pulsar.client.admin.Transactions;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStatus;

public class TransactionsImpl extends BaseResource implements Transactions {
    private final WebTarget adminV3Transactions;

    public TransactionsImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminV3Transactions = web.path("/admin/v3/transactions");
    }

    @Override
    public CompletableFuture<TransactionCoordinatorStatus> getCoordinatorStatusById(int coordinatorId) {
        WebTarget path = adminV3Transactions.path("coordinatorStatus");
        path = path.queryParam("coordinatorId", coordinatorId);
        final CompletableFuture<TransactionCoordinatorStatus> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<TransactionCoordinatorStatus>() {
                    @Override
                    public void completed(TransactionCoordinatorStatus status) {
                        future.complete(status);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<List<TransactionCoordinatorStatus>> getCoordinatorStatusList() {
        WebTarget path = adminV3Transactions.path("coordinatorStatus");
        final CompletableFuture<List<TransactionCoordinatorStatus>> statusList = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<TransactionCoordinatorStatus>>() {
                    @Override
                    public void completed(List<TransactionCoordinatorStatus> topics) {
                        statusList.complete(topics);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        statusList.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return statusList;
    }

}
