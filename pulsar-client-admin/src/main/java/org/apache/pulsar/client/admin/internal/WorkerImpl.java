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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Worker;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.policies.data.WorkerFunctionInstanceStats;
import org.apache.pulsar.common.stats.Metrics;

@Slf4j
public class WorkerImpl extends BaseResource implements Worker {

    private final WebTarget workerStats;
    private final WebTarget worker;

    public WorkerImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        this.worker = web.path("/admin/v2/worker");
        this.workerStats = web.path("/admin/v2/worker-stats");
    }

    @Override
    public List<WorkerFunctionInstanceStats> getFunctionsStats() throws PulsarAdminException {
        return sync(() -> getFunctionsStatsAsync());
    }

    @Override
    public CompletableFuture<List<WorkerFunctionInstanceStats>> getFunctionsStatsAsync() {
        WebTarget path = workerStats.path("functionsmetrics");
        final CompletableFuture<List<WorkerFunctionInstanceStats>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            future.completeExceptionally(new ClientErrorException(response));
                        } else {
                            List<WorkerFunctionInstanceStats> metricsList =
                                    response.readEntity(new GenericType<List<WorkerFunctionInstanceStats>>() {});
                            future.complete(metricsList);
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public Collection<Metrics> getMetrics() throws PulsarAdminException {
        return sync(() -> getMetricsAsync());
    }

    @Override
    public CompletableFuture<Collection<Metrics>> getMetricsAsync() {
        WebTarget path = workerStats.path("metrics");
        final CompletableFuture<Collection<Metrics>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            future.completeExceptionally(new ClientErrorException(response));
                        } else {
                            future.complete(response.readEntity(
                                    new GenericType<List<Metrics>>() {}));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public List<WorkerInfo> getCluster() throws PulsarAdminException {
        return sync(() -> getClusterAsync());
    }

    @Override
    public CompletableFuture<List<WorkerInfo>> getClusterAsync() {
        WebTarget path = worker.path("cluster");
        final CompletableFuture<List<WorkerInfo>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {

                    @Override
                    public void completed(Response response) {
                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            future.completeExceptionally(new ClientErrorException(response));
                        } else {
                            future.complete(response.readEntity(new GenericType<List<WorkerInfo>>() {}));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public WorkerInfo getClusterLeader() throws PulsarAdminException {
        return sync(() -> getClusterLeaderAsync());
    }

    @Override
    public CompletableFuture<WorkerInfo> getClusterLeaderAsync() {
        WebTarget path = worker.path("cluster").path("leader");
        final CompletableFuture<WorkerInfo> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            future.completeExceptionally(new ClientErrorException(response));
                        } else {
                            future.complete(response.readEntity(new GenericType<WorkerInfo>(){}));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public Map<String, Collection<String>> getAssignments() throws PulsarAdminException {
        return sync(() -> getAssignmentsAsync());
    }

    @Override
    public CompletableFuture<Map<String, Collection<String>>> getAssignmentsAsync() {
        WebTarget path = worker.path("assignments");
        final CompletableFuture<Map<String, Collection<String>>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            future.completeExceptionally(new ClientErrorException(response));
                        } else {
                            future.complete(response.readEntity(
                                    new GenericType<Map<String, Collection<String>>>() {}));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void rebalance() throws PulsarAdminException {
        sync(this::rebalanceAsync);
    }

    @Override
    public CompletableFuture<Void> rebalanceAsync() {
        final WebTarget path = worker.path("rebalance");
        return asyncPutRequest(path,  Entity.entity("", MediaType.APPLICATION_JSON));
    }
}
