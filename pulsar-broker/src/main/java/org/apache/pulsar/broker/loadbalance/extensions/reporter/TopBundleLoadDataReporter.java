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
package org.apache.pulsar.broker.loadbalance.extensions.reporter;

import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.models.TopKBundles;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;

/**
 * The top k highest-loaded bundles' load data reporter.
 */
@Slf4j
public class TopBundleLoadDataReporter implements LoadDataReporter<TopBundlesLoadData> {

    private final PulsarService pulsar;

    private final String lookupServiceAddress;

    private final LoadDataStore<TopBundlesLoadData> bundleLoadDataStore;

    private final TopKBundles topKBundles;

    private long lastBundleStatsUpdatedAt;

    public TopBundleLoadDataReporter(PulsarService pulsar,
                                     String lookupServiceAddress,
                                     LoadDataStore<TopBundlesLoadData> bundleLoadDataStore) {
        this.pulsar = pulsar;
        this.lookupServiceAddress = lookupServiceAddress;
        this.bundleLoadDataStore = bundleLoadDataStore;
        this.lastBundleStatsUpdatedAt = 0;
        this.topKBundles = new TopKBundles();
    }

    @Override
    public TopBundlesLoadData generateLoadData() {

        var pulsarStats = pulsar.getBrokerService().getPulsarStats();
        TopBundlesLoadData result = null;
        synchronized (pulsarStats) {
            var pulsarStatsUpdatedAt = pulsarStats.getUpdatedAt();
            if (pulsarStatsUpdatedAt > lastBundleStatsUpdatedAt) {
                var bundleStats = pulsar.getBrokerService().getBundleStats();
                double percentage = pulsar.getConfiguration().getLoadBalancerBundleLoadReportPercentage();
                int topk = Math.max(1, (int) (bundleStats.size() * percentage / 100.0));
                topKBundles.update(bundleStats, topk);
                lastBundleStatsUpdatedAt = pulsarStatsUpdatedAt;
                result = topKBundles.getLoadData();
            }
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> reportAsync(boolean force) {
        var topBundlesLoadData = generateLoadData();
        if (topBundlesLoadData != null || force) {
            return this.bundleLoadDataStore.pushAsync(lookupServiceAddress, topKBundles.getLoadData())
                    .exceptionally(e -> {
                        log.error("Failed to report top-bundles load data.", e);
                        return null;
                    });
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }
}
