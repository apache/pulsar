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

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import java.util.concurrent.CompletableFuture;

public class LocalPoliciesResources extends BaseResources<LocalPolicies> {

    public static final String LOCAL_POLICIES_ROOT = "/admin/local-policies";

    public LocalPoliciesResources(MetadataStoreExtended configurationStore, int operationTimeoutSec) {
        super(configurationStore, LocalPolicies.class, operationTimeoutSec);
    }

    public CompletableFuture<Void> deleteLocalPoliciesAsync(NamespaceName ns) {
        CompletableFuture<Void> completableFuture = deleteIfExistsAsync(joinPath(LOCAL_POLICIES_ROOT, ns.toString()));
        // in order to delete the cluster for namespace v1
        if (ns.getCluster() != null) {
            String clusterPath = joinPath(LOCAL_POLICIES_ROOT, ns.getTenant(), ns.getCluster());
            return getChildrenAsync(clusterPath).thenCompose(nss -> {
                if (nss.isEmpty()) {
                    return deleteIfExistsAsync(clusterPath);
                }
                return completableFuture;
            });
        } else {
            return completableFuture;
        }
    }

    public CompletableFuture<Void> deleteLocalPoliciesTenantAsync(String tenant) {
        return deleteIfExistsAsync(joinPath(LOCAL_POLICIES_ROOT, tenant));
    }
}
