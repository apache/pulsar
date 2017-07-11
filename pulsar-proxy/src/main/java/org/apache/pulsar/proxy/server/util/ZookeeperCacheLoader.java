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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.apache.pulsar.zookeeper.LocalZooKeeperCache;
import org.apache.pulsar.zookeeper.LocalZooKeeperConnectionService;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperChildrenCache;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Connects with ZooKeeper and sets watch to listen changes for active broker list.
 *
 */
public class ZookeeperCacheLoader implements Closeable {

    private final ZooKeeperCache localZkCache;
    private final LocalZooKeeperConnectionService localZkConnectionSvc;

    private final ZooKeeperDataCache<LoadReport> brokerInfo;
    private final ZooKeeperChildrenCache availableBrokersCache;

    private volatile Set<String> availableBrokersSet;
    private volatile List<ServiceLookupData> availableBrokers;

    private final OrderedSafeExecutor orderedExecutor = new OrderedSafeExecutor(8, "pulsar-discovery-ordered-cache");
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(8,
            new DefaultThreadFactory("pulsar-discovery-cache"));

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

        this.localZkCache = new LocalZooKeeperCache(localZkConnectionSvc.getLocalZooKeeper(), this.orderedExecutor,
                executor);
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
        availableBrokersSet = availableBrokersCache.get();
        updateBrokerList(availableBrokersSet);
    }

    public List<ServiceLookupData> getAvailableBrokers() {
        return availableBrokers;
    }

    public Set<String> getAvailableBrokersSet() {
        return availableBrokersSet;
    }

    public ZooKeeperCache getLocalZkCache() {
        return localZkCache;
    }

    @Override
    public void close() {
        orderedExecutor.shutdown();
        executor.shutdown();
    }

    private void updateBrokerList(Set<String> brokerNodes) throws Exception {
        List<ServiceLookupData> availableBrokers = new ArrayList<>(brokerNodes.size());
        for (String broker : brokerNodes) {
            availableBrokers.add(brokerInfo.get(LOADBALANCE_BROKERS_ROOT + '/' + broker).get());
        }

        this.availableBrokers = availableBrokers;
    }

    private static final Logger log = LoggerFactory.getLogger(ZookeeperCacheLoader.class);

}
