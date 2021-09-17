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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;

public class LocalPoliciesResources extends BaseResources<LocalPolicies> {

    private static final String LOCAL_POLICIES_ROOT = "/admin/local-policies";

    public LocalPoliciesResources(MetadataStore localStore, int operationTimeoutSec) {
        super(localStore, LocalPolicies.class, operationTimeoutSec);
    }

    public void setLocalPolicies(NamespaceName ns, Function<LocalPolicies, LocalPolicies> modifyFunction)
            throws MetadataStoreException {
        set(joinPath(LOCAL_POLICIES_ROOT, ns.toString()), modifyFunction);
    }

    public Optional<LocalPolicies> getLocalPolicies(NamespaceName ns) throws MetadataStoreException{
        return get(joinPath(LOCAL_POLICIES_ROOT, ns.toString()));
    }

    public CompletableFuture<Optional<LocalPolicies>> getLocalPoliciesAsync(NamespaceName ns) {
        return getCache().get(joinPath(LOCAL_POLICIES_ROOT, ns.toString()));
    }

    public void setLocalPoliciesWithCreate(NamespaceName ns, Function<Optional<LocalPolicies>, LocalPolicies> createFunction) throws MetadataStoreException {
        setWithCreate(joinPath(LOCAL_POLICIES_ROOT, ns.toString()), createFunction);
    }

    public CompletableFuture<Void> createLocalPoliciesAsync(NamespaceName ns, LocalPolicies policies) {
        return getCache().create(joinPath(LOCAL_POLICIES_ROOT, ns.toString()), policies);
    }

    public CompletableFuture<Optional<CacheGetResult<LocalPolicies>>> getLocalPoliciesWithVersion(NamespaceName ns) {
        return getCache().getWithStats(joinPath(LOCAL_POLICIES_ROOT, ns.toString()));
    }

    public CompletableFuture<Void> setLocalPoliciesWithVersion(NamespaceName ns, LocalPolicies policies,
                                                               Optional<Long> version) {
        try {
            byte[] content = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(policies);
            return getStore().put(joinPath(LOCAL_POLICIES_ROOT, ns.toString()), content, version)
                    .thenApply(__ -> null);
        } catch (JsonProcessingException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    public void deleteLocalPolicies(NamespaceName ns) throws MetadataStoreException {
        delete(joinPath(LOCAL_POLICIES_ROOT, ns.toString()));
    }

    public static boolean isLocalPoliciesPath(String path) {
        return path.startsWith(LOCAL_POLICIES_ROOT);
    }
}
