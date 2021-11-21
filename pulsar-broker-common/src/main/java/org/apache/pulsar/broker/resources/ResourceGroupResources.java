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
package org.apache.pulsar.broker.resources;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;

public class ResourceGroupResources extends BaseResources<ResourceGroup> {

    private static final String BASE_PATH = "/admin/resourcegroups";

    public ResourceGroupResources(MetadataStore store, int operationTimeoutSec) {
        super(store, ResourceGroup.class, operationTimeoutSec);
    }

    public Optional<ResourceGroup> getResourceGroup(String resourceGroupName) throws MetadataStoreException {
        return get(joinPath(BASE_PATH, resourceGroupName));
    }

    public CompletableFuture<Optional<ResourceGroup>> getResourceGroupAsync(String resourceGroupName) {
        return getAsync(joinPath(BASE_PATH, resourceGroupName));
    }

    public boolean resourceGroupExists(String resourceGroupName) throws MetadataStoreException {
        return exists(joinPath(BASE_PATH, resourceGroupName));
    }

    public void createResourceGroup(String resourceGroupName, ResourceGroup rg) throws MetadataStoreException {
        create(joinPath(BASE_PATH, resourceGroupName), rg);
    }

    public void deleteResourceGroup(String resourceGroupName) throws MetadataStoreException {
        delete(joinPath(BASE_PATH, resourceGroupName));
    }

    public void updateResourceGroup(String resourceGroupName,
                                    Function<ResourceGroup, ResourceGroup> modifyFunction)
            throws MetadataStoreException {
        set(joinPath(BASE_PATH, resourceGroupName), modifyFunction);
    }

    public List<String> listResourceGroups() throws MetadataStoreException {
        return getChildren(BASE_PATH);
    }

    public CompletableFuture<List<String>> listResourceGroupsAsync(){
        return getChildrenAsync(BASE_PATH);
    }

    public static boolean isResourceGroupPath(String path) {
        return path.startsWith(BASE_PATH);
    }

    public static Optional<String> resourceGroupNameFromPath(String path) {
        if (path.length() > BASE_PATH.length() + 1) {
            return Optional.of(path.substring(BASE_PATH.length() + 1));
        } else {
            return Optional.empty();
        }
    }
}
