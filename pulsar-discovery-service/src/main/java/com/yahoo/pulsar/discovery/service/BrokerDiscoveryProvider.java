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
package com.yahoo.pulsar.discovery.service;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.common.policies.data.loadbalancer.LoadReport;
import com.yahoo.pulsar.discovery.service.server.ServiceConfig;
import com.yahoo.pulsar.discovery.service.web.ZookeeperCacheLoader;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory;
import static org.apache.bookkeeper.util.MathUtils.signSafeMod;

/**
 * Maintains available active broker list and returns next active broker in round-robin for discovery service.
 *
 */
public class BrokerDiscoveryProvider {

    private ZookeeperCacheLoader zkCache;
    private final String zookeeperServers;
    private final AtomicInteger counter = new AtomicInteger();

    public BrokerDiscoveryProvider(ServiceConfig config) {
        this.zookeeperServers = config.getZookeeperServers();
    }

    /**
     * starts {@link ZookeeperCacheLoader} to maintain active-broker list
     * 
     * @param zkClientFactory
     * @throws PulsarServerException
     */
    public void start(ZooKeeperClientFactory zkClientFactory) throws PulsarServerException {
        try {
            zkCache = new ZookeeperCacheLoader(zkClientFactory, zookeeperServers);
        } catch (Exception e) {
            LOG.error("Failed to start Zookkeeper {}", e.getMessage(), e);
            throw new PulsarServerException("Failed to start zookeeper :" + e.getMessage(), e);
        }
    }

    /**
     * Find next broke {@link LoadReport} in round-robin fashion.
     *
     * @return
     * @throws PulsarServerException
     */
    LoadReport nextBroker() throws PulsarServerException {
        List<LoadReport> availableBrokers = zkCache.getAvailableBrokers();

        if (availableBrokers.isEmpty()) {
            throw new PulsarServerException("No active broker is available");
        } else {
            int brokersCount = availableBrokers.size();
            int nextIdx = signSafeMod(counter.getAndIncrement(), brokersCount);
            return availableBrokers.get(nextIdx);
        }
    }

    public void close() {
        zkCache.close();
    }

    private static final Logger LOG = LoggerFactory.getLogger(BrokerDiscoveryProvider.class);
    
}
