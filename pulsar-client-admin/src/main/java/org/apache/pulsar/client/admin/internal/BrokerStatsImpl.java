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
package org.apache.pulsar.client.admin.internal;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import jakarta.ws.rs.client.WebTarget;
import java.util.concurrent.CompletableFuture;
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

    private final WebTarget adminV2BrokerStats;

    public BrokerStatsImpl(WebTarget target, Authentication auth, long requestTimeoutMs) {
        super(auth, requestTimeoutMs);
        adminV2BrokerStats = target.path("/admin/v2/broker-stats");
    }

    @Override
    public String getMetrics() throws PulsarAdminException {
        return getMetrics(null);
    }

    @Override
    public CompletableFuture<String> getMetricsAsync() {
        return getMetricsAsync(null);
    }

    @Override
    public String getMetrics(String broker) throws PulsarAdminException {
        return sync(() -> getMetricsAsync(broker));
    }

    @Override
    public CompletableFuture<String> getMetricsAsync(String broker) {
        WebTarget path = brokerStats(broker).path("/metrics");
        return asyncGetRequest(path, new FutureCallback<String>(){});
    }

    @Override
    public AllocatorStats getAllocatorStats(String allocatorName) throws PulsarAdminException {
        return getAllocatorStats(allocatorName, null);
    }

    @Override
    public CompletableFuture<AllocatorStats> getAllocatorStatsAsync(String allocatorName) {
        return getAllocatorStatsAsync(allocatorName, null);
    }

    @Override
    public AllocatorStats getAllocatorStats(String allocatorName, String broker) throws PulsarAdminException {
        return sync(() -> getAllocatorStatsAsync(allocatorName, broker));
    }

    @Override
    public CompletableFuture<AllocatorStats> getAllocatorStatsAsync(String allocatorName, String broker) {
        WebTarget path = brokerStats(broker).path("/allocator-stats").path(allocatorName);
        return asyncGetRequest(path, new FutureCallback<AllocatorStats>(){});
    }

    @Override
    public String getMBeans() throws PulsarAdminException {
        return getMBeans(null);
    }

    @Override
    public CompletableFuture<String> getMBeansAsync() {
        return getMBeansAsync(null);
    }

    @Override
    public String getMBeans(String broker) throws PulsarAdminException {
        return sync(() -> getMBeansAsync(broker));
    }

    @Override
    public CompletableFuture<String> getMBeansAsync(String broker) {
        WebTarget path = brokerStats(broker).path("/mbeans");
        return asyncGetRequest(path, new FutureCallback<String>(){});
    }

    @Override
    public String getTopics() throws PulsarAdminException {
        return getTopics(null);
    }

    @Override
    public CompletableFuture<String> getTopicsAsync() {
        return getTopicsAsync(null);
    }

    @Override
    public String getTopics(String broker) throws PulsarAdminException {
        return sync(() -> getTopicsAsync(broker));
    }

    @Override
    public CompletableFuture<String> getTopicsAsync(String broker) {
        WebTarget path = brokerStats(broker).path("/topics");
        return asyncGetRequest(path, new FutureCallback<String>(){});
    }

    @Override
    public LoadManagerReport getLoadReport() throws PulsarAdminException {
        return getLoadReport(null);
    }

    @Override
    public CompletableFuture<LoadManagerReport> getLoadReportAsync() {
        return getLoadReportAsync(null);
    }

    @Override
    public LoadManagerReport getLoadReport(String broker) throws PulsarAdminException {
        return sync(() -> getLoadReportAsync(broker));
    }

    @Override
    public CompletableFuture<LoadManagerReport> getLoadReportAsync(String broker) {
        WebTarget path = brokerStats(broker).path("/load-report");
        return asyncGetRequest(path, new FutureCallback<LoadManagerReport>(){});
    }

    @Override
    public String getPendingBookieOpsStats() throws PulsarAdminException {
        return getPendingBookieOpsStats(null);
    }

    @Override
    public CompletableFuture<String> getPendingBookieOpsStatsAsync() {
        return getPendingBookieOpsStatsAsync(null);
    }

    @Override
    public String getPendingBookieOpsStats(String broker) throws PulsarAdminException {
        return sync(() -> getPendingBookieOpsStatsAsync(broker));
    }

    @Override
    public CompletableFuture<String> getPendingBookieOpsStatsAsync(String broker) {
        WebTarget path = brokerStats(broker).path("/bookieops");
        return asyncGetRequest(path, new FutureCallback<String>(){});
    }

    /**
     * Explicit broker selection uses the broker-specific route, so a broker that doesn't support it fails
     * the request (404) instead of returning its own stats.
     */
    private WebTarget brokerStats(String broker) {
        return broker == null || broker.isEmpty()
                ? adminV2BrokerStats
                : adminV2BrokerStats.path("brokers").path(broker);
    }

    public JsonObject getBrokerResourceAvailability(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            String json = request(adminV2BrokerStats.path("/broker-resource-availability").path(ns.toString()))
                    .get(String.class);
            return new Gson().fromJson(json, JsonObject.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
