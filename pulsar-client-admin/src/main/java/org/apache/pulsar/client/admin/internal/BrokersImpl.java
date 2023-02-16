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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.client.admin.Brokers;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.naming.TopicVersion;
import org.apache.pulsar.common.policies.data.BrokerInfo;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.util.Codec;

public class BrokersImpl extends BaseResource implements Brokers {
    private final WebTarget adminBrokers;

    public BrokersImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminBrokers = web.path("admin/v2/brokers");
    }

    @Override
    public List<String> getActiveBrokers() throws PulsarAdminException {
        return sync(() -> getActiveBrokersAsync(null));
    }

    @Override
    public CompletableFuture<List<String>> getActiveBrokersAsync() {
        return getActiveBrokersAsync(null);
    }

    @Override
    public List<String> getActiveBrokers(String cluster) throws PulsarAdminException {
        return sync(() -> getActiveBrokersAsync(cluster));
    }

    @Override
    public CompletableFuture<List<String>> getActiveBrokersAsync(String cluster) {
        WebTarget path = cluster == null ? adminBrokers : adminBrokers.path(cluster);
        return asyncGetRequest(path, new FutureCallback<List<String>>(){});
    }

    @Override
    public BrokerInfo getLeaderBroker() throws PulsarAdminException {
        return sync(() -> getLeaderBrokerAsync());
    }

    @Override
    public CompletableFuture<BrokerInfo> getLeaderBrokerAsync() {
        WebTarget path = adminBrokers.path("leaderBroker");
        return asyncGetRequest(path, new FutureCallback<BrokerInfo>(){});
    }

    @Override
    public Map<String, NamespaceOwnershipStatus> getOwnedNamespaces(String cluster, String brokerUrl)
            throws PulsarAdminException {
        return sync(() -> getOwnedNamespacesAsync(cluster, brokerUrl));
    }

    @Override
    public CompletableFuture<Map<String, NamespaceOwnershipStatus>> getOwnedNamespacesAsync(
            String cluster, String brokerUrl) {
        WebTarget path = adminBrokers.path(cluster).path(brokerUrl).path("ownedNamespaces");
        return asyncGetRequest(path, new FutureCallback<Map<String, NamespaceOwnershipStatus>>(){});
    }

    @Override
    public void updateDynamicConfiguration(String configName, String configValue) throws PulsarAdminException {
        sync(() -> updateDynamicConfigurationAsync(configName, configValue));
    }

    @Override
    public CompletableFuture<Void> updateDynamicConfigurationAsync(String configName, String configValue) {
        String value = Codec.encode(configValue);
        WebTarget path = adminBrokers.path("configuration").path(configName).path(value);
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void deleteDynamicConfiguration(String configName) throws PulsarAdminException {
        sync(() -> deleteDynamicConfigurationAsync(configName));
    }

    @Override
    public CompletableFuture<Void> deleteDynamicConfigurationAsync(String configName) {
        WebTarget path = adminBrokers.path("configuration").path(configName);
        return asyncDeleteRequest(path);
    }

    @Override
    public Map<String, String> getAllDynamicConfigurations() throws PulsarAdminException {
        return sync(this::getAllDynamicConfigurationsAsync);
    }

    @Override
    public CompletableFuture<Map<String, String>> getAllDynamicConfigurationsAsync() {
        WebTarget path = adminBrokers.path("configuration").path("values");
        return asyncGetRequest(path, new FutureCallback<Map<String, String>>(){});
    }

    @Override
    public List<String> getDynamicConfigurationNames() throws PulsarAdminException {
        return sync(this::getDynamicConfigurationNamesAsync);
    }

    @Override
    public CompletableFuture<List<String>> getDynamicConfigurationNamesAsync() {
        WebTarget path = adminBrokers.path("configuration");
        return asyncGetRequest(path, new FutureCallback<List<String>>(){});
    }

    @Override
    public Map<String, String> getRuntimeConfigurations() throws PulsarAdminException {
        return sync(this::getRuntimeConfigurationsAsync);
    }

    @Override
    public CompletableFuture<Map<String, String>> getRuntimeConfigurationsAsync() {
        WebTarget path = adminBrokers.path("configuration").path("runtime");
        return asyncGetRequest(path, new FutureCallback<Map<String, String>>(){});
    }

    @Override
    public InternalConfigurationData getInternalConfigurationData() throws PulsarAdminException {
        return sync(this::getInternalConfigurationDataAsync);
    }

    @Override
    public CompletableFuture<InternalConfigurationData> getInternalConfigurationDataAsync() {
        WebTarget path = adminBrokers.path("internal-configuration");
        return asyncGetRequest(path, new FutureCallback<InternalConfigurationData>(){});
    }

    @Override
    public void backlogQuotaCheck() throws PulsarAdminException {
        sync(this::backlogQuotaCheckAsync);
    }

    @Override
    public CompletableFuture<Void> backlogQuotaCheckAsync() {
        WebTarget path = adminBrokers.path("backlogQuotaCheck");
        return asyncGetRequest(path, new FutureCallback<Void>() {});
    }

    @Override
    @Deprecated
    public void healthcheck() throws PulsarAdminException {
        healthcheck(TopicVersion.V1);
    }

    @Override
    @Deprecated
    public CompletableFuture<Void> healthcheckAsync() {
        return healthcheckAsync(TopicVersion.V1);
    }

    @Override
    public void healthcheck(TopicVersion topicVersion) throws PulsarAdminException {
        sync(() -> healthcheckAsync(topicVersion));
    }

    @Override
    public CompletableFuture<Void> healthcheckAsync(TopicVersion topicVersion) {
        WebTarget path = adminBrokers.path("health");
        if (topicVersion != null) {
            path = path.queryParam("topicVersion", topicVersion);
        }
        final CompletableFuture<Void> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String result) {
                        if (!"ok".equalsIgnoreCase(result.trim())) {
                            future.completeExceptionally(
                                    new PulsarAdminException("Healthcheck returned unexpected result: " + result));
                        } else {
                            future.complete(null);
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
    public CompletableFuture<Void> shutDownBrokerGracefully(int maxConcurrentUnloadPerSec,
                                                            boolean forcedTerminateTopic) {
        WebTarget path = adminBrokers.path("shutdown")
                .queryParam("maxConcurrentUnloadPerSec", maxConcurrentUnloadPerSec)
                .queryParam("forcedTerminateTopic", forcedTerminateTopic);
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public String getVersion() throws PulsarAdminException {
        return sync(this::getVersionAsync);
    }

    public CompletableFuture<String> getVersionAsync() {
        WebTarget path = adminBrokers.path("version");

        return asyncGetRequest(path, new FutureCallback<String>(){});
    }
}
