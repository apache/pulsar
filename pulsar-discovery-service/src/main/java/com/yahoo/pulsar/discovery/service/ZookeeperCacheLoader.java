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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.yahoo.pulsar.zookeeper.LocalZooKeeperCache;
import com.yahoo.pulsar.zookeeper.LocalZooKeeperConnectionService;
import com.yahoo.pulsar.zookeeper.ZooKeeperCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperCacheListener;
import com.yahoo.pulsar.zookeeper.ZooKeeperChildrenCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory;
import com.yahoo.pulsar.zookeeper.ZooKeeperSessionWatcher.ShutdownService;
import com.yahoo.pulsar.zookeeper.ZookeeperClientFactoryImpl;

/**
 * Connects with ZooKeeper and sets watch to listen changes for active broker list.
 * 
 */
public class ZookeeperCacheLoader {

    // Zookeeper quorum connection string
    private String zookeeperServers;
    // Zookeeper session timeout in milliseconds
    private long zooKeeperSessionTimeoutMillis = 30000;
    private ZooKeeperCache localZkCache;
    private LocalZooKeeperConnectionService localZkConnectionSvc;
    private ZooKeeperClientFactory zkClientFactory = null;
    private ZooKeeperChildrenCache availableActiveBrokerCache;
    private volatile List<String> availableActiveBrokers;
    public static final String LOADBALANCE_BROKERS_ROOT = "/loadbalance/brokers";
    private final OrderedSafeExecutor orderedExecutor = new OrderedSafeExecutor(8, "pulsar-discovery");

    /**
     * Initialize ZooKeeper session and creates broker cache list
     * 
     * @param zookeeperServers
     * @throws InterruptedException
     *             : when failed to fetch broker list from cache
     * @throws IOException
     *             : when failed create ZooKeeper session
     */
    public ZookeeperCacheLoader(String zookeeperServers) throws InterruptedException, IOException {
        this.zookeeperServers = zookeeperServers;
        start();
    }

    /**
     * starts ZooKeeper session
     * 
     * @throws InterruptedException
     *             : when failed to fetch broker list from cache
     * @throws IOException
     *             : when failed create zk session
     */
    public void start() throws InterruptedException, IOException {

        localZkConnectionSvc = new LocalZooKeeperConnectionService(getZooKeeperClientFactory(), zookeeperServers,
                zooKeeperSessionTimeoutMillis);

        localZkConnectionSvc.start(new ShutdownService() {
            @Override
            public void shutdown(int exitCode) {
                try {
                    localZkCache.getZooKeeper().close();
                } catch (InterruptedException e) {
                    log.warn("Failed to shutdown ZooKeeper gracefully {}", e.getMessage(), e);
                }
            }
        });

        this.localZkCache = new LocalZooKeeperCache(localZkConnectionSvc.getLocalZooKeeper(), this.orderedExecutor);
        // attach ZooKeeperChildrenCache listener
        initializeBrokerList();
    }

    /**
     * 1. creates ZooKeeper Children cache on path {@value LOADBALANCE_BROKERS_ROOT}, 2. sets watch on the path and 3.
     * maintain list of available brokers at availableActiveBrokers
     * 
     * @throws InterruptedException
     * @throws IOException
     * 
     */
    private void initializeBrokerList() throws InterruptedException, IOException {
        this.availableActiveBrokerCache = new ZooKeeperChildrenCache(getLocalZkCache(), LOADBALANCE_BROKERS_ROOT);
        this.availableActiveBrokerCache.registerListener(new ZooKeeperCacheListener<Set<String>>() {
            @Override
            public void onUpdate(String path, Set<String> data, Stat stat) {
                if (log.isDebugEnabled()) {
                    log.debug("Update Received for path {}", path);
                }
                availableActiveBrokers = Lists.newArrayList(data);
            }
        });
        // initialize available broker list
        try {
            this.availableActiveBrokers = Lists.newArrayList(availableActiveBrokerCache.get());
        } catch (KeeperException e) {
            log.warn("Failed to find broker znode children under {}", LOADBALANCE_BROKERS_ROOT, e);
            throw new IOException(String.format("Failed to find broker list in zk at %s with %s ",
                    LOADBALANCE_BROKERS_ROOT, e.getMessage()), e);
        }
    }

    private ZooKeeperClientFactory getZooKeeperClientFactory() {
        if (zkClientFactory == null) {
            zkClientFactory = new ZookeeperClientFactoryImpl();
        }
        // Return default factory
        return zkClientFactory;
    }

    public List<String> getAvailableActiveBrokers() {
        return this.availableActiveBrokers;
    }

    public ZooKeeperCache getLocalZkCache() {
        return localZkCache;
    }

    private static final Logger log = LoggerFactory.getLogger(ZookeeperCacheLoader.class);

}
