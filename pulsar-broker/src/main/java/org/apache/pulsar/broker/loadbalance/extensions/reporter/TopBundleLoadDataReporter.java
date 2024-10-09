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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.manager.StateChangeListener;
import org.apache.pulsar.broker.loadbalance.extensions.models.TopKBundles;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;

/**
 * The top k highest-loaded bundles' load data reporter.
 */
@Slf4j
public class TopBundleLoadDataReporter implements LoadDataReporter<TopBundlesLoadData>, StateChangeListener {

    private static final long TOMBSTONE_DELAY_IN_MILLIS = 1000 * 10;

    private final PulsarService pulsar;

    private final String brokerId;

    private final LoadDataStore<TopBundlesLoadData> bundleLoadDataStore;

    private final TopKBundles topKBundles;

    private long lastBundleStatsUpdatedAt;

    private volatile long lastTombstonedAt;
    private long tombstoneDelayInMillis;

    public TopBundleLoadDataReporter(PulsarService pulsar,
                                     String brokerId,
                                     LoadDataStore<TopBundlesLoadData> bundleLoadDataStore) {
        this.pulsar = pulsar;
        this.brokerId = brokerId;
        this.bundleLoadDataStore = bundleLoadDataStore;
        this.lastBundleStatsUpdatedAt = 0;
        this.topKBundles = new TopKBundles(pulsar);
        this.tombstoneDelayInMillis = TOMBSTONE_DELAY_IN_MILLIS;
    }

    @Override
    public TopBundlesLoadData generateLoadData() {

        var pulsarStats = pulsar.getBrokerService().getPulsarStats();
        TopBundlesLoadData result = null;
        synchronized (pulsarStats) {
            var pulsarStatsUpdatedAt = pulsarStats.getUpdatedAt();
            if (pulsarStatsUpdatedAt > lastBundleStatsUpdatedAt) {
                var bundleStats = pulsar.getBrokerService().getBundleStats();
                int topk = pulsar.getConfiguration().getLoadBalancerMaxNumberOfBundlesInBundleLoadReport();
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
            if (ExtensibleLoadManagerImpl.debug(pulsar.getConfiguration(), log)) {
                log.info("Reporting TopBundlesLoadData:{}", topKBundles.getLoadData());
            }
            return this.bundleLoadDataStore.pushAsync(brokerId, topKBundles.getLoadData())
                    .exceptionally(e -> {
                        log.error("Failed to report top-bundles load data.", e);
                        return null;
                    });
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @VisibleForTesting
    protected void tombstone() {
        var now = System.currentTimeMillis();
        if (now - lastTombstonedAt < tombstoneDelayInMillis) {
            return;
        }
        var lastSuccessfulTombstonedAt = lastTombstonedAt;
        lastTombstonedAt = now; // dedup first
        bundleLoadDataStore.removeAsync(brokerId)
                .whenComplete((__, e) -> {
                            if (e != null) {
                                log.error("Failed to clean broker load data.", e);
                                lastTombstonedAt = lastSuccessfulTombstonedAt;
                            } else {
                                boolean debug = ExtensibleLoadManagerImpl.debug(pulsar.getConfiguration(), log);
                                if (debug) {
                                    log.info("Cleaned broker load data.");
                                }
                            }
                        }
                );
    }

    @Override
    public void handleEvent(String serviceUnit, ServiceUnitStateData data, Throwable t) {
        if (t != null) {
            return;
        }
        ServiceUnitState state = ServiceUnitStateData.state(data);
        switch (state) {
            case Releasing, Splitting -> {
                if (StringUtils.equals(data.sourceBroker(), brokerId)) {
                    tombstone();
                }
            }
            case Owned -> {
                if (StringUtils.equals(data.dstBroker(), brokerId)) {
                    tombstone();
                }
            }
        }
    }
}
