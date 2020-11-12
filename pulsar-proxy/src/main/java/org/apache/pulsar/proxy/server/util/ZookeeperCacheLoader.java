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
package org.apache.pulsar.proxy.server.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.zookeeper.LocalZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperChildrenCache;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connects with ZooKeeper and sets watch to listen changes for active broker list.
 *
 */
public class ZookeeperCacheLoader implements Closeable {

    private final ZooKeeper zkClient;
    private final ZooKeeperCache localZkCache;
    private final ZooKeeperDataCache<LoadManagerReport> brokerInfo;
    private final ZooKeeperChildrenCache availableBrokersCache;

    private volatile List<LoadManagerReport> availableBrokers;

    private final OrderedScheduler orderedExecutor = OrderedScheduler.newSchedulerBuilder().numThreads(8)
            .name("pulsar-proxy-ordered-cache").build();

    public static final String LOADBALANCE_BROKERS_ROOT = "/loadbalance/brokers";

    /**
     * Initialize ZooKeeper session and creates broker cache list
     *
     * @param zookeeperServers
     * @throws Exception
     */
    public ZookeeperCacheLoader(ZooKeeperClientFactory factory, String zookeeperServers, int zookeeperSessionTimeoutMs) throws Exception {
        this.zkClient = factory.create(zookeeperServers, SessionType.AllowReadOnly, zookeeperSessionTimeoutMs).get();
        int zkOperationTimeoutSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(zookeeperSessionTimeoutMs);
        this.localZkCache = new LocalZooKeeperCache(zkClient, zkOperationTimeoutSeconds, this.orderedExecutor);

        this.brokerInfo = new ZooKeeperDataCache<LoadManagerReport>(localZkCache) {
            @Override
            public LoadManagerReport deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, LoadManagerReport.class);
            }
        };

        this.availableBrokersCache = new ZooKeeperChildrenCache(getLocalZkCache(), LOADBALANCE_BROKERS_ROOT);
        this.availableBrokersCache.registerListener((path, brokerNodes, stat) -> {
            updateBrokerList(brokerNodes).thenRun(() -> {
                log.info("Successfully updated broker info {}", brokerNodes);
            }).exceptionally(ex -> {
                log.warn("Error updating broker info after broker list changed", ex);
                return null;
            });
        });

        // Do initial fetch of brokers list
        try {
            updateBrokerList(availableBrokersCache.get()).get(zkOperationTimeoutSeconds, TimeUnit.SECONDS);
        } catch (NoNodeException nne) { // can happen if no broker started yet
            updateBrokerList(Collections.emptySet()).get(zkOperationTimeoutSeconds, TimeUnit.SECONDS);
        }
    }

    public List<LoadManagerReport> getAvailableBrokers() {
        return availableBrokers;
    }

    public ZooKeeperCache getLocalZkCache() {
        return localZkCache;
    }

    @Override
    public void close() throws IOException {
        try {
            zkClient.close();
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new IOException(e);
        }
        orderedExecutor.shutdown();
    }

    private CompletableFuture<Void> updateBrokerList(Set<String> brokerNodes) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (brokerNodes.isEmpty()) {
            availableBrokers = new ArrayList<>();
            future.complete(null);
            return future;
        }

        List<CompletableFuture<Optional<LoadManagerReport>>> loadReportFutureList = new ArrayList<>();
        for (String broker : brokerNodes) {
            loadReportFutureList.add(brokerInfo.getAsync(LOADBALANCE_BROKERS_ROOT + '/' + broker));
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

    private static final Logger log = LoggerFactory.getLogger(ZookeeperCacheLoader.class);

}
