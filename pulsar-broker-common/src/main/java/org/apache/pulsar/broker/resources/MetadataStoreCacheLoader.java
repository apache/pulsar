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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connects with MetadataStore and sets watch to listen changes for active broker list.
 *
 */
public class MetadataStoreCacheLoader implements Closeable {

    private final LoadManagerReportResources loadReportResources;
    private final int operationTimeoutMs;

    private volatile List<LoadManagerReport> availableBrokers;

    private final OrderedScheduler orderedExecutor = OrderedScheduler.newSchedulerBuilder().numThreads(8)
            .name("pulsar-metadata-cache-loader-ordered-cache").build();

    public static final String LOADBALANCE_BROKERS_ROOT = "/loadbalance/brokers";

    public MetadataStoreCacheLoader(PulsarResources pulsarResources, int operationTimeoutMs) throws Exception {
        this.loadReportResources = pulsarResources.getLoadReportResources();
        this.operationTimeoutMs = operationTimeoutMs;
        init();
    }

    /**
     * Initialize ZooKeeper session and creates broker cache list.
     *
     * @throws Exception
     */
    public void init() throws Exception {
        loadReportResources.getStore().registerListener((n) -> {
            if (LOADBALANCE_BROKERS_ROOT.equals(n.getPath()) && NotificationType.ChildrenChanged.equals(n.getType())) {
                loadReportResources.getChildrenAsync(LOADBALANCE_BROKERS_ROOT).thenApplyAsync((brokerNodes)->{
                    updateBrokerList(brokerNodes).thenRun(() -> {
                        log.info("Successfully updated broker info {}", brokerNodes);
                    }).exceptionally(ex -> {
                        log.warn("Error updating broker info after broker list changed", ex);
                        return null;
                    });
                    return null;
                }).exceptionally(ex -> {
                    log.warn("Error updating broker info after broker list changed", ex);
                    return null;
                });
            }
        });

        // Do initial fetch of brokers list
        updateBrokerList(loadReportResources.getChildren(LOADBALANCE_BROKERS_ROOT)).get(operationTimeoutMs,
                TimeUnit.SECONDS);
    }

    public List<LoadManagerReport> getAvailableBrokers() {
        if (CollectionUtils.isEmpty(availableBrokers)) {
            try {
                updateBrokerList(loadReportResources.getChildren(LOADBALANCE_BROKERS_ROOT));
            } catch (Exception e) {
                log.warn("Error updating broker from zookeeper.", e);
            }
        }
        return availableBrokers;
    }

    @Override
    public void close() throws IOException {
        orderedExecutor.shutdown();
    }

    private CompletableFuture<Void> updateBrokerList(List<String> brokerNodes) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (brokerNodes.isEmpty()) {
            availableBrokers = new ArrayList<>();
            future.complete(null);
            return future;
        }

        List<CompletableFuture<Optional<LoadManagerReport>>> loadReportFutureList = new ArrayList<>();
        for (String broker : brokerNodes) {
            loadReportFutureList.add(loadReportResources.getAsync(LOADBALANCE_BROKERS_ROOT + '/' + broker));
        }

        FutureUtil.waitForAll(loadReportFutureList).thenRun(() -> {
            List<LoadManagerReport> newAvailableBrokers = new ArrayList<>(brokerNodes.size());

            for (CompletableFuture<Optional<LoadManagerReport>> loadReportFuture : loadReportFutureList) {
                try {
                    Optional<LoadManagerReport> loadReport = loadReportFuture.get();
                    if (loadReport.isPresent()) {
                        newAvailableBrokers.add(loadReport.get());
                    }
                } catch (Exception e) {
                    future.completeExceptionally(e);
                    return;
                }
            }

            availableBrokers = newAvailableBrokers;
            future.complete(null);
        }).exceptionally(ex -> {
            future.completeExceptionally(ex);
            return null;
        });

        return future;
    }

    private static final Logger log = LoggerFactory.getLogger(MetadataStoreCacheLoader.class);

}