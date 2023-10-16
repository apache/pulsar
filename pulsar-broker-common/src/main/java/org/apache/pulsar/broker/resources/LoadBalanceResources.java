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
package org.apache.pulsar.broker.resources;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;

@Getter
public class LoadBalanceResources {
    public static final String BUNDLE_DATA_BASE_PATH = "/loadbalance/bundle-data";

    private final BundleDataResources bundleDataResources;

    public LoadBalanceResources(MetadataStore store, int operationTimeoutSec) {
        bundleDataResources = new BundleDataResources(store, operationTimeoutSec);
    }

    public static class BundleDataResources extends BaseResources<BundleData> {
        public BundleDataResources(MetadataStore store, int operationTimeoutSec) {
            super(store, BundleData.class, operationTimeoutSec);
        }

        public CompletableFuture<Optional<BundleData>> getBundleData(String bundle) {
            return getAsync(getBundleDataPath(bundle));
        }

        public CompletableFuture<Void> updateBundleData(String bundle, BundleData data) {
            return setWithCreateAsync(getBundleDataPath(bundle), __ -> data);
        }

        public CompletableFuture<Void> deleteBundleData(String bundle) {
            return deleteAsync(getBundleDataPath(bundle));
        }

        // clear resource of `/loadbalance/bundle-data/{tenant}/{namespace}/` in metadata-store
        public CompletableFuture<Void> deleteBundleDataAsync(NamespaceName ns) {
            final String namespaceBundlePath = joinPath(BUNDLE_DATA_BASE_PATH, ns.toString());
            return getStore().deleteRecursive(namespaceBundlePath);
        }

        // clear resource of `/loadbalance/bundle-data/{tenant}/` in metadata-store
        public CompletableFuture<Void> deleteBundleDataTenantAsync(String tenant) {
            final String tenantBundlePath = joinPath(BUNDLE_DATA_BASE_PATH, tenant);
            return getStore().deleteRecursive(tenantBundlePath);
        }

        // Get the metadata store path for the given bundle full name.
        private String getBundleDataPath(final String bundle) {
            return BUNDLE_DATA_BASE_PATH + "/" + bundle;
        }
    }
}
