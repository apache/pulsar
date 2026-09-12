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
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;

@Getter
public class LoadBalanceResources {
    public static final String BUNDLE_DATA_BASE_PATH = "/loadbalance/bundle-data";
    public static final String BROKER_TIME_AVERAGE_BASE_PATH = "/loadbalance/broker-time-average";
    public static final String RESOURCE_QUOTA_BASE_PATH = "/loadbalance/resource-quota";

    private final BundleDataResources bundleDataResources;
    private final BrokerTimeAverageDataResources brokerTimeAverageDataResources;
    private final QuotaResources quotaResources;

    public LoadBalanceResources(MetadataStore store, int operationTimeoutSec) {
        bundleDataResources = new BundleDataResources(store, operationTimeoutSec);
        brokerTimeAverageDataResources = new BrokerTimeAverageDataResources(store, operationTimeoutSec);
        quotaResources = new QuotaResources(store, operationTimeoutSec);
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

    public static class BrokerTimeAverageDataResources extends BaseResources<TimeAverageBrokerData> {
        public BrokerTimeAverageDataResources(MetadataStore store, int operationTimeoutSec) {
            super(store, TimeAverageBrokerData.class, operationTimeoutSec);
        }

        public CompletableFuture<Void> updateTimeAverageBrokerData(String brokerLookupAddress,
                TimeAverageBrokerData data) {
            return setWithCreateAsync(getTimeAverageBrokerDataPath(brokerLookupAddress), __ -> data);
        }

        public CompletableFuture<Void> deleteTimeAverageBrokerData(String brokerLookupAddress) {
            return deleteAsync(getTimeAverageBrokerDataPath(brokerLookupAddress));
        }

        private String getTimeAverageBrokerDataPath(final String brokerLookupAddress) {
            return BROKER_TIME_AVERAGE_BASE_PATH + "/" + brokerLookupAddress;
        }
    }

    public static class QuotaResources extends BaseResources<ResourceQuota> {
        public QuotaResources(MetadataStore store, int operationTimeoutSec) {
            super(store, ResourceQuota.class, operationTimeoutSec);
        }

        public CompletableFuture<Optional<ResourceQuota>> getQuota(String bundle) {
            return getAsync(getBundleQuotaPath(bundle));
        }

        public CompletableFuture<Optional<ResourceQuota>> getDefaultQuota() {
            return getAsync(getDefaultBundleQuotaPath());
        }

        public CompletableFuture<Void> setWithCreateQuotaAsync(String bundle, ResourceQuota quota) {
            return setWithCreateAsync(getBundleQuotaPath(bundle), __ -> quota);
        }

        public CompletableFuture<Void> setWithCreateDefaultQuotaAsync(ResourceQuota quota) {
            return setWithCreateAsync(getDefaultBundleQuotaPath(), __ -> quota);
        }

        public CompletableFuture<Void> deleteQuota(String bundle) {
            return deleteAsync(getBundleQuotaPath(bundle));
        }

        private String getBundleQuotaPath(String bundle) {
            return String.format("%s/%s", RESOURCE_QUOTA_BASE_PATH, bundle);
        }

        private String getDefaultBundleQuotaPath() {
            return getBundleQuotaPath("default");
        }
    }
}
