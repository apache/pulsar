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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.stats.AllocatorStats;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;

/**
 * Pulsar Admin API client.
 *
 */
public class BrokerStatsImpl extends BaseResource implements BrokerStats {

    private final WebTarget adminBrokerStats;
    private final WebTarget adminV2BrokerStats;

    public BrokerStatsImpl(WebTarget target, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminBrokerStats = target.path("/admin/broker-stats");
        adminV2BrokerStats = target.path("/admin/v2/broker-stats");
    }

    @Override
    public String getMetrics() throws PulsarAdminException {
        return sync(() -> getMetricsAsync());
    }

    @Override
    public CompletableFuture<String> getMetricsAsync() {
        WebTarget path = adminV2BrokerStats.path("/metrics");
        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String s) {
                        future.complete(s);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public AllocatorStats getAllocatorStats(String allocatorName) throws PulsarAdminException {
        return sync(() -> getAllocatorStatsAsync(allocatorName));
    }

    @Override
    public CompletableFuture<AllocatorStats> getAllocatorStatsAsync(String allocatorName) {
        WebTarget path = adminV2BrokerStats.path("/allocator-stats").path(allocatorName);
        final CompletableFuture<AllocatorStats> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<AllocatorStats>() {
                    @Override
                    public void completed(AllocatorStats allocatorStats) {
                        future.complete(allocatorStats);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public String getMBeans() throws PulsarAdminException {
        return sync(() -> getMBeansAsync());
    }

    @Override
    public CompletableFuture<String> getMBeansAsync() {
        WebTarget path = adminV2BrokerStats.path("/mbeans");
        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String s) {
                        future.complete(s);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public String getTopics() throws PulsarAdminException {
        return sync(() -> getTopicsAsync());
    }

    @Override
    public CompletableFuture<String> getTopicsAsync() {
        WebTarget path = adminV2BrokerStats.path("/topics");
        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String s) {
                        future.complete(s);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public LoadManagerReport getLoadReport() throws PulsarAdminException {
        return sync(() -> getLoadReportAsync());
    }

    @Override
    public CompletableFuture<LoadManagerReport> getLoadReportAsync() {
        WebTarget path = adminV2BrokerStats.path("/load-report");
        final CompletableFuture<LoadManagerReport> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<LoadManagerReport>() {
                    @Override
                    public void completed(LoadManagerReport loadManagerReport) {
                        future.complete(loadManagerReport);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public String getPendingBookieOpsStats() throws PulsarAdminException {
        return sync(() -> getPendingBookieOpsStatsAsync());
    }

    @Override
    public CompletableFuture<String> getPendingBookieOpsStatsAsync() {
        WebTarget path = adminV2BrokerStats.path("/bookieops");
        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String s) {
                        future.complete(s);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    public JsonObject getBrokerResourceAvailability(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget admin = ns.isV2() ? adminV2BrokerStats : adminBrokerStats;
            String json = request(admin.path("/broker-resource-availability").path(ns.toString())).get(String.class);
            return new Gson().fromJson(json, JsonObject.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
