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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.client.admin.Brokers;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.util.Codec;

public class BrokersImpl extends BaseResource implements Brokers {
    private final WebTarget adminBrokers;

    public BrokersImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminBrokers = web.path("admin/v2/brokers");
    }

    @Override
    public List<String> getActiveBrokers(String cluster) throws PulsarAdminException {
        try {
            return getActiveBrokersAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> getActiveBrokersAsync(String cluster) {
        WebTarget path = adminBrokers.path(cluster);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> brokers) {
                        future.complete(brokers);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public Map<String, NamespaceOwnershipStatus> getOwnedNamespaces(String cluster, String brokerUrl)
            throws PulsarAdminException {
        try {
            return getOwnedNamespacesAsync(cluster, brokerUrl).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Map<String, NamespaceOwnershipStatus>> getOwnedNamespacesAsync(
            String cluster, String brokerUrl) {
        WebTarget path = adminBrokers.path(cluster).path(brokerUrl).path("ownedNamespaces");
        final CompletableFuture<Map<String, NamespaceOwnershipStatus>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, NamespaceOwnershipStatus>>() {
                    @Override
                    public void completed(Map<String, NamespaceOwnershipStatus> ownedNamespaces) {
                        future.complete(ownedNamespaces);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void updateDynamicConfiguration(String configName, String configValue) throws PulsarAdminException {
        try {
            updateDynamicConfigurationAsync(configName, configValue).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> updateDynamicConfigurationAsync(String configName, String configValue) {
        String value = Codec.encode(configValue);
        WebTarget path = adminBrokers.path("configuration").path(configName).path(value);
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void deleteDynamicConfiguration(String configName) throws PulsarAdminException {
        try {
            deleteDynamicConfigurationAsync(configName).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> deleteDynamicConfigurationAsync(String configName) {
        WebTarget path = adminBrokers.path("configuration").path(configName);
        return asyncDeleteRequest(path);
    }

    @Override
    public Map<String, String> getAllDynamicConfigurations() throws PulsarAdminException {
        try {
            return getAllDynamicConfigurationsAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Map<String, String>> getAllDynamicConfigurationsAsync() {
        WebTarget path = adminBrokers.path("configuration").path("values");
        final CompletableFuture<Map<String, String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, String>>() {
                    @Override
                    public void completed(Map<String, String> allConfs) {
                        future.complete(allConfs);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public List<String> getDynamicConfigurationNames() throws PulsarAdminException {
        try {
            return getDynamicConfigurationNamesAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> getDynamicConfigurationNamesAsync() {
        WebTarget path = adminBrokers.path("configuration");
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> confNames) {
                        future.complete(confNames);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public Map<String, String> getRuntimeConfigurations() throws PulsarAdminException {
        try {
            return getRuntimeConfigurationsAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Map<String, String>> getRuntimeConfigurationsAsync() {
        WebTarget path = adminBrokers.path("configuration").path("runtime");
        final CompletableFuture<Map<String, String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, String>>() {
                    @Override
                    public void completed(Map<String, String> runtimeConfs) {
                        future.complete(runtimeConfs);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public InternalConfigurationData getInternalConfigurationData() throws PulsarAdminException {
        try {
            return getInternalConfigurationDataAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<InternalConfigurationData> getInternalConfigurationDataAsync() {
        WebTarget path = adminBrokers.path("internal-configuration");
        final CompletableFuture<InternalConfigurationData> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<InternalConfigurationData>() {
                    @Override
                    public void completed(InternalConfigurationData internalConfigurationData) {
                        future.complete(internalConfigurationData);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void healthcheck() throws PulsarAdminException {
        try {
            healthcheckAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> healthcheckAsync() {
        WebTarget path = adminBrokers.path("health");
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
}
