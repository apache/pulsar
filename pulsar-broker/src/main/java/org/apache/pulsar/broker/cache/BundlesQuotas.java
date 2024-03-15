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
package org.apache.pulsar.broker.cache;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.LoadBalanceResources;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.policies.data.ResourceQuota;

public class BundlesQuotas {
    LoadBalanceResources loadBalanceResources;

    // Default initial quota
    static final ResourceQuota INITIAL_QUOTA = new ResourceQuota();

    static {
        INITIAL_QUOTA.setMsgRateIn(40); // incoming msg / sec
        INITIAL_QUOTA.setMsgRateOut(120); // outgoing msg / sec
        INITIAL_QUOTA.setBandwidthIn(100000); // incoming bytes / sec
        INITIAL_QUOTA.setBandwidthOut(300000); // outgoing bytes / sec
        INITIAL_QUOTA.setMemory(80); // Mbytes
        INITIAL_QUOTA.setDynamic(true); // allow dynamically re-calculating
    }

    public BundlesQuotas(PulsarService pulsar) {
        loadBalanceResources = pulsar.getPulsarResources().getLoadBalanceResources();
    }

    public CompletableFuture<Void> setDefaultResourceQuota(ResourceQuota quota) {
        return loadBalanceResources.getQuotaResources().setWithCreateDefaultQuotaAsync(quota);
    }

    public CompletableFuture<ResourceQuota> getDefaultResourceQuota() {
        return loadBalanceResources.getQuotaResources().getDefaultQuota()
                .thenApply(optResourceQuota -> optResourceQuota.orElse(INITIAL_QUOTA));
    }

    public CompletableFuture<Void> setResourceQuota(String bundle, ResourceQuota quota) {
        return loadBalanceResources.getQuotaResources().setWithCreateQuotaAsync(bundle, quota);
    }

    public CompletableFuture<Void> setResourceQuota(NamespaceBundle bundle, ResourceQuota quota) {
        return setResourceQuota(bundle.toString(), quota);
    }

    public CompletableFuture<ResourceQuota> getResourceQuota(NamespaceBundle bundle) {
        return getResourceQuota(bundle.toString());
    }

    public CompletableFuture<ResourceQuota> getResourceQuota(String bundle) {
        return loadBalanceResources.getQuotaResources().getQuota(bundle)
                .thenCompose(optResourceQuota -> {
                    if (optResourceQuota.isPresent()) {
                        return CompletableFuture.completedFuture(optResourceQuota.get());
                    } else {
                        return getDefaultResourceQuota();
                    }
                });
    }

    public CompletableFuture<Void> resetResourceQuota(NamespaceBundle bundle) {
        return loadBalanceResources.getQuotaResources().deleteQuota(bundle.toString());
    }
}
