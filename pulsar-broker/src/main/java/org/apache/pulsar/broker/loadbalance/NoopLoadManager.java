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
package org.apache.pulsar.broker.loadbalance;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.impl.PulsarResourceDescription;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceUnit;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.apache.pulsar.zookeeper.ZooKeeperCache.Deserializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class NoopLoadManager implements LoadManager {

    private PulsarService pulsar;
    private String lookupServiceAddress;
    private ResourceUnit localResourceUnit;
    private ZooKeeper zkClient;

    LocalBrokerData localData;

    private static final Deserializer<LocalBrokerData> loadReportDeserializer = (key, content) -> ObjectMapperFactory
            .getThreadLocal()
            .readValue(content, LocalBrokerData.class);

    @Override
    public void initialize(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    @Override
    public void start() throws PulsarServerException {
        lookupServiceAddress = pulsar.getAdvertisedAddress() + ":" + pulsar.getConfiguration().getWebServicePort().get();
        localResourceUnit = new SimpleResourceUnit(String.format("http://%s", lookupServiceAddress),
                new PulsarResourceDescription());
        zkClient = pulsar.getZkClient();

        localData = new LocalBrokerData(pulsar.getSafeWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                pulsar.getSafeBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls());
        localData.setProtocols(pulsar.getProtocolDataToAdvertise());
        String brokerZnodePath = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + lookupServiceAddress;

        try {
            // When running in standalone, this error can happen when killing the "standalone" process
            // ungracefully since the ZK session will not be closed and it will take some time for ZK server
            // to prune the expired sessions after startup.
            // Since there's a single broker instance running, it's safe, in this mode, to remove the old lock

            // Delete and recreate z-node
            try {
                if (zkClient.exists(brokerZnodePath, null) != null) {
                    zkClient.delete(brokerZnodePath, -1);
                }
            } catch (NoNodeException nne) {
                // Ignore if z-node was just expired
            }

            ZkUtils.createFullPathOptimistic(zkClient, brokerZnodePath, localData.getJsonBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        } catch (Exception e) {
            throw new PulsarServerException(e);
        }
    }

    @Override
    public boolean isCentralized() {
        return false;
    }

    @Override
    public Optional<ResourceUnit> getLeastLoaded(ServiceUnitId su) throws Exception {
        return Optional.of(localResourceUnit);
    }

    @Override
    public LoadManagerReport generateLoadReport() throws Exception {
        return null;
    }

    @Override
    public Deserializer<? extends ServiceLookupData> getLoadReportDeserializer() {
        return loadReportDeserializer;
    }

    @Override
    public void setLoadReportForceUpdateFlag() {
        // do nothing
    }

    @Override
    public void writeLoadReportOnZookeeper() throws Exception {
        // do nothing
    }

    @Override
    public void writeResourceQuotasToZooKeeper() throws Exception {
        // do nothing
    }

    @Override
    public List<Metrics> getLoadBalancingMetrics() {
        return Collections.emptyList();
    }

    @Override
    public void doLoadShedding() {
        // do nothing
    }

    @Override
    public void doNamespaceBundleSplit() throws Exception {
        // do nothing
    }

    @Override
    public void disableBroker() throws Exception {
        // do nothing
    }

    @Override
    public Set<String> getAvailableBrokers() throws Exception {
        return Collections.singleton(lookupServiceAddress);
    }

    @Override
    public void stop() throws PulsarServerException {
        // do nothing
    }

}
