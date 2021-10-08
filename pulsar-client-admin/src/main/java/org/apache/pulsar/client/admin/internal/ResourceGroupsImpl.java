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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.ResourceGroups;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.apache.pulsar.common.util.RestException;


public class ResourceGroupsImpl extends BaseResource implements ResourceGroups {
    private final WebTarget adminResourceGroups;

    public ResourceGroupsImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminResourceGroups = web.path("/admin/v2/resourcegroups");
    }

    @Override
    public List<String> getResourceGroups() throws PulsarAdminException {
        try {
            return getResourceGroupsAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<List<String>> getResourceGroupsAsync() {
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(adminResourceGroups,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> resourcegroups) {
                        future.complete(resourcegroups);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public ResourceGroup getResourceGroup(String resourcegroup) throws PulsarAdminException {
        try {
            return getResourceGroupAsync(resourcegroup).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<ResourceGroup> getResourceGroupAsync(String name) {
        WebTarget path = adminResourceGroups.path(name);
        final CompletableFuture<ResourceGroup> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<ResourceGroup>() {
                    @Override
                    public void completed(ResourceGroup resourcegroup) {
                        future.complete(resourcegroup);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void createResourceGroup(String name, ResourceGroup resourcegroup) throws PulsarAdminException {
        try {
            createResourceGroupAsync(name, resourcegroup)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> createResourceGroupAsync(String name, ResourceGroup resourcegroup) {
        WebTarget path = adminResourceGroups.path(name);
        return asyncPutRequest(path, Entity.entity(resourcegroup, MediaType.APPLICATION_JSON));
    }

    @Override
    public void updateResourceGroup(String name, ResourceGroup resourcegroup) throws PulsarAdminException {
        try {
            getResourceGroup(name);
            updateResourceGroupAsync(name, resourcegroup).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | RestException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> updateResourceGroupAsync(String name, ResourceGroup resourcegroup) {
        WebTarget path = adminResourceGroups.path(name);
        return asyncPutRequest(path, Entity.entity(resourcegroup, MediaType.APPLICATION_JSON));
    }

    @Override
    public void deleteResourceGroup(String name) throws PulsarAdminException {
        try {
            deleteResourceGroupAsync(name).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> deleteResourceGroupAsync(String name) {
        WebTarget path = adminResourceGroups.path(name);
        return asyncDeleteRequest(path);
    }
}
