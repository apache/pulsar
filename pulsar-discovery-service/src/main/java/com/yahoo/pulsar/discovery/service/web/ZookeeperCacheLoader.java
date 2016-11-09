/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.discovery.service.web;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.common.policies.data.loadbalancer.LoadReport;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.zookeeper.LocalZooKeeperCache;
import com.yahoo.pulsar.zookeeper.LocalZooKeeperConnectionService;
import com.yahoo.pulsar.zookeeper.ZooKeeperCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperChildrenCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory;
import com.yahoo.pulsar.zookeeper.ZooKeeperDataCache;

/**
 * Connects with ZooKeeper and sets watch to listen changes for active broker list.
 *
 */
public class ZookeeperCacheLoader implements Closeable {

    private final ZooKeeperCache localZkCache;
    private final LocalZooKeeperConnectionService localZkConnectionSvc;

    private final ZooKeeperDataCache<LoadReport> brokerInfo;
    private final ZooKeeperChildrenCache availableBrokersCache;

    private volatile List<LoadReport> availableBrokers;

    private final OrderedSafeExecutor orderedExecutor = new OrderedSafeExecutor(8, "pulsar-discovery");

    public static final String LOADBALANCE_BROKERS_ROOT = "/loadbalance/brokers";

    private static final int zooKeeperSessionTimeoutMillis = 30_000;

    /**
     * Initialize ZooKeeper session and creates broker cache list
     *
     * @param zookeeperServers
     * @throws Exception
     */
    public ZookeeperCacheLoader(ZooKeeperClientFactory zkClientFactory, String zookeeperServers) throws Exception {
        localZkConnectionSvc = new LocalZooKeeperConnectionService(zkClientFactory, zookeeperServers,
                zooKeeperSessionTimeoutMillis);
        localZkConnectionSvc.start(exitCode -> {
            log.error("Shutting down ZK sessions: {}", exitCode);
        });

        this.localZkCache = new LocalZooKeeperCache(localZkConnectionSvc.getLocalZooKeeper(), this.orderedExecutor);
        localZkConnectionSvc.start(exitCode -> {
            try {
                localZkCache.getZooKeeper().close();
            } catch (InterruptedException e) {
                log.warn("Failed to shutdown ZooKeeper gracefully {}", e.getMessage(), e);
            }
        });

        this.brokerInfo = new ZooKeeperDataCache<LoadReport>(localZkCache) {
            @Override
            public LoadReport deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, LoadReport.class);
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

    public List<LoadReport> getAvailableBrokers() {
        return availableBrokers;
    }

    public ZooKeeperCache getLocalZkCache() {
        return localZkCache;
    }

    @Override
    public void close() {
        orderedExecutor.shutdown();
    }

    private void updateBrokerList(Set<String> brokerNodes) throws Exception {
        List<LoadReport> availableBrokers = new ArrayList<>(brokerNodes.size());
        for (String broker : brokerNodes) {
            availableBrokers.add(brokerInfo.get(LOADBALANCE_BROKERS_ROOT + '/' + broker).get());
        }

        this.availableBrokers = availableBrokers;
    }

    private static final Logger log = LoggerFactory.getLogger(ZookeeperCacheLoader.class);

}
