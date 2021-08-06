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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import org.apache.pulsar.client.admin.Transactions;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStats;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionMetadata;
import org.apache.pulsar.common.policies.data.TransactionPendingAckStats;

public class TransactionsImpl extends BaseResource implements Transactions {
    private final WebTarget adminV3Transactions;

    public TransactionsImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminV3Transactions = web.path("/admin/v3/transactions");
    }

    @Override
    public CompletableFuture<TransactionCoordinatorStats> getCoordinatorStatsById(int coordinatorId) {
        WebTarget path = adminV3Transactions.path("coordinatorStats");
        path = path.queryParam("coordinatorId", coordinatorId);
        final CompletableFuture<TransactionCoordinatorStats> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<TransactionCoordinatorStats>() {
                    @Override
                    public void completed(TransactionCoordinatorStats stats) {
                        future.complete(stats);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<Map<Integer, TransactionCoordinatorStats>> getCoordinatorStats() {
        WebTarget path = adminV3Transactions.path("coordinatorStats");
        final CompletableFuture<Map<Integer, TransactionCoordinatorStats>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<Integer, TransactionCoordinatorStats>>() {
                    @Override
                    public void completed(Map<Integer, TransactionCoordinatorStats> stats) {
                        future.complete(stats);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<TransactionInBufferStats> getTransactionInBufferStats(TxnID txnID, String topic) {
        WebTarget path = adminV3Transactions.path("transactionInBufferStats");
        path = path.queryParam("mostSigBits", txnID.getMostSigBits());
        path = path.queryParam("leastSigBits", txnID.getLeastSigBits());
        path = path.queryParam("topic", topic);
        final CompletableFuture<TransactionInBufferStats> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<TransactionInBufferStats>() {
                    @Override
                    public void completed(TransactionInBufferStats stats) {
                        future.complete(stats);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<TransactionInPendingAckStats> getTransactionInPendingAckStats(TxnID txnID, String topic,
                                                                                           String subName) {
        WebTarget path = adminV3Transactions.path("transactionInPendingAckStats");
        path = path.queryParam("mostSigBits", txnID.getMostSigBits());
        path = path.queryParam("leastSigBits", txnID.getLeastSigBits());
        path = path.queryParam("topic", topic);
        path = path.queryParam("subName", subName);
        final CompletableFuture<TransactionInPendingAckStats> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<TransactionInPendingAckStats>() {
                    @Override
                    public void completed(TransactionInPendingAckStats stats) {
                        future.complete(stats);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<TransactionMetadata> getTransactionMetadata(TxnID txnID) {
        WebTarget path = adminV3Transactions.path("transactionMetadata");
        path = path.queryParam("mostSigBits", txnID.getMostSigBits());
        path = path.queryParam("leastSigBits", txnID.getLeastSigBits());
        final CompletableFuture<TransactionMetadata> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<TransactionMetadata>() {
                    @Override
                    public void completed(TransactionMetadata metadata) {
                        future.complete(metadata);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<TransactionBufferStats> getTransactionBufferStats(String topic) {
        WebTarget path = adminV3Transactions.path("transactionBufferStats");
        path = path.queryParam("topic", topic);
        final CompletableFuture<TransactionBufferStats> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<TransactionBufferStats>() {
                    @Override
                    public void completed(TransactionBufferStats stats) {
                        future.complete(stats);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<TransactionPendingAckStats> getPendingAckStats(String topic, String subName) {
        WebTarget path = adminV3Transactions.path("pendingAckStats");
        path = path.queryParam("topic", topic);
        path = path.queryParam("subName", subName);
        final CompletableFuture<TransactionPendingAckStats> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<TransactionPendingAckStats>() {
                    @Override
                    public void completed(TransactionPendingAckStats stats) {
                        future.complete(stats);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }


}
