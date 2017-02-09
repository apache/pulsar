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
package com.yahoo.pulsar.broker.loadbalance.impl;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.common.policies.data.loadbalancer.LoadReport;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.zookeeper.ZooKeeperCacheListener;
import com.yahoo.pulsar.zookeeper.ZooKeeperChildrenCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperDataCache;

/**
 * <p>
 * Sometimes if cluster has brokers which are running on different zk-quorum configuration. <br/>
 * One set of brokers which are using single zk-quorom(data_zk) to store cluster-management and ledger-data. And other
 * set of brokers are using separate zk: local_zk for cluster-management and data_zk for ledger-data.
 * <p>
 * Therefore, loadBalancer requires loadReport + list_of_active brokers from both the cluster to decide assignment of
 * new bundle-ownership.
 * <p>
 * It helps to aggregate active brokers from local_zk and data_zk, also helps to get their load report by performing
 * dual-read from both the cluster.
 * <p>
 * As brokers which are running with one data_zk configuration, is not reading local_zk: So, this one also performs
 * dual-write while broker registration and load-report generation so, it can be available at data_zk.
 * 
 */
public class SimpleLoadManagerDualZkImpl extends SimpleLoadManagerImpl {

    private final ZooKeeperChildrenCache _availableActiveBrokers;
    private final ZooKeeperDataCache<LoadReport> _loadReportCacheZk;
    private final boolean shouldWriteOnDataZk;

    public SimpleLoadManagerDualZkImpl(PulsarService pulsar) {
        super(pulsar);
        _availableActiveBrokers = new ZooKeeperChildrenCache(pulsar.getDataZkCache(), LOADBALANCE_BROKERS_ROOT);
        _availableActiveBrokers.registerListener(new ZooKeeperCacheListener<Set<String>>() {
            @Override
            public void onUpdate(String path, Set<String> data, Stat stat) {
                if (log.isDebugEnabled()) {
                    log.debug("Update Received for path {}", path);
                }
                scheduler.submit(SimpleLoadManagerDualZkImpl.super::updateRanking);
            }
        });
        _loadReportCacheZk = new ZooKeeperDataCache<LoadReport>(pulsar.getDataZkCache()) {
            @Override
            public LoadReport deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, LoadReport.class);
            }
        };
        _loadReportCacheZk.registerListener(this);
        shouldWriteOnDataZk = !pulsar.equalsDataAndLocalZk();
    }

    @Override
    public Set<String> getActiveBrokers() throws Exception {
        Set<String> availableBrokers = Sets.newHashSet(super.getActiveBrokers());
        availableBrokers.addAll(_availableActiveBrokers.get());
        return availableBrokers;
    }

    @Override
    public CompletableFuture<LoadReport> getLoadReportAsync(String broker) {
        CompletableFuture<LoadReport> loadReportFuture = new CompletableFuture<>();
        super.getLoadReportAsync(broker).thenAccept(loadReport -> loadReportFuture.complete(loadReport))
                .exceptionally(ex -> {
                    String key = String.format("%s/%s", LOADBALANCE_BROKERS_ROOT, broker);
                    _loadReportCacheZk.getAsync(key).thenAccept(loadReport -> {
                        if (loadReport.isPresent()) {
                            loadReportFuture.complete(loadReport.get());
                        } else {
                            loadReportFuture.completeExceptionally(new KeeperException.NoNodeException(key));
                        }
                    }).exceptionally(e -> {
                        loadReportFuture.completeExceptionally(e);
                        return null;
                    });
                    return null;
                });
        return loadReportFuture;
    }

    @Override
    protected void registerBroker(String loadReportJson, String brokerZnodePath)
            throws KeeperException, InterruptedException {
        super.registerBroker(loadReportJson, brokerZnodePath);
        if (shouldWriteOnDataZk) {
            if (pulsar.getDataZkClient().exists(LOADBALANCE_BROKERS_ROOT, false) == null) {
                try {
                    ZkUtils.createFullPathOptimistic(pulsar.getDataZkClient(), LOADBALANCE_BROKERS_ROOT, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {
                    // ignore the exception, node might be present already
                }
            }
            try {
                ZkUtils.createFullPathOptimistic(pulsar.getDataZkClient(), brokerZnodePath,
                        loadReportJson.getBytes(Charsets.UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (Exception e) {
                // Catching excption here to print the right error message
                log.error("Unable to create znode - [{}] for load balance on zookeeper ", brokerZnodePath, e);
                throw e;
            }
        }
    }

    @Override
    public void disableBroker() throws Exception {
        super.disableBroker();
        if (isNotEmpty(brokerZnodePath) && shouldWriteOnDataZk) {
            pulsar.getDataZkClient().delete(brokerZnodePath, -1);
        }
    }

    @Override
    protected void writeLoadReportOnZk(LoadReport lr) throws Exception {
        super.writeLoadReportOnZk(lr);
        if (shouldWriteOnDataZk) {
            pulsar.getDataZkClient().setData(brokerZnodePath,
                    ObjectMapperFactory.getThreadLocal().writeValueAsBytes(lr), -1);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(SimpleLoadManagerDualZkImpl.class);
}
