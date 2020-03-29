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
package org.apache.pulsar.discovery.service.web;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.zookeeper.LocalZooKeeperCache;
import org.apache.pulsar.zookeeper.LocalZooKeeperConnectionService;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperChildrenCache;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connects with ZooKeeper and sets watch to listen changes for active broker list.
 *
 */
public class ZookeeperCacheLoader implements Closeable {

    private final ZooKeeperCache localZkCache;
    private final LocalZooKeeperConnectionService localZkConnectionSvc;

    private final ZooKeeperDataCache<LoadManagerReport> brokerInfo;
    private final ZooKeeperChildrenCache availableBrokersCache;

    private volatile List<LoadManagerReport> availableBrokers;

    private final OrderedScheduler orderedExecutor = OrderedScheduler.newSchedulerBuilder().numThreads(8)
            .name("pulsar-discovery-ordered-cache").build();

    public static final String LOADBALANCE_BROKERS_ROOT = "/loadbalance/brokers";

    /**
     * Initialize ZooKeeper session and creates broker cache list
     *
     * @param zookeeperServers
     * @throws Exception
     */
    public ZookeeperCacheLoader(ZooKeeperClientFactory zkClientFactory, String zookeeperServers,
            int zookeeperSessionTimeoutMs) throws Exception {
        localZkConnectionSvc = new LocalZooKeeperConnectionService(zkClientFactory, zookeeperServers,
                zookeeperSessionTimeoutMs);
        localZkConnectionSvc.start(exitCode -> {
            log.error("Shutting down ZK sessions: {}", exitCode);
        });

        this.localZkCache = new LocalZooKeeperCache(localZkConnectionSvc.getLocalZooKeeper(),
                (int) TimeUnit.MILLISECONDS.toSeconds(zookeeperSessionTimeoutMs), this.orderedExecutor);
        localZkConnectionSvc.start(exitCode -> {
            try {
                localZkCache.getZooKeeper().close();
            } catch (InterruptedException e) {
                log.warn("Failed to shutdown ZooKeeper gracefully {}", e.getMessage(), e);
            }
        });

        this.brokerInfo = new ZooKeeperDataCache<LoadManagerReport>(localZkCache) {
            @Override
            public LoadManagerReport deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, LoadManagerReport.class);
            }
        };

        this.availableBrokersCache = new ZooKeeperChildrenCache(getLocalZkCache(), LOADBALANCE_BROKERS_ROOT);
        this.availableBrokersCache.registerListener((path, brokerNodes, stat) -> {
            try {
                updateBrokerList(brokerNodes);
            } catch (Exception e) {
                log.warn("Error updating broker info after broker list changed.", e);
            }
        });

        // Do initial fetch of brokers list
        updateBrokerList(availableBrokersCache.get());
    }

    public List<LoadManagerReport> getAvailableBrokers() {
        return availableBrokers;
    }

    public ZooKeeperCache getLocalZkCache() {
        return localZkCache;
    }

    @Override
    public void close() throws IOException {
        localZkCache.stop();
        localZkConnectionSvc.close();
        orderedExecutor.shutdown();
    }

    private void updateBrokerList(Set<String> brokerNodes) throws Exception {
        List<LoadManagerReport> availableBrokers = new ArrayList<>(brokerNodes.size());
        for (String broker : brokerNodes) {
            availableBrokers.add(brokerInfo.get(LOADBALANCE_BROKERS_ROOT + '/' + broker).get());
        }

        this.availableBrokers = availableBrokers;
    }

    private static final Logger log = LoggerFactory.getLogger(ZookeeperCacheLoader.class);

}
