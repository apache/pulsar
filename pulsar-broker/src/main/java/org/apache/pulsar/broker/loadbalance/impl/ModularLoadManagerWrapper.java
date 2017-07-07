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
package org.apache.pulsar.broker.loadbalance.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.ModularLoadManager;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.apache.pulsar.zookeeper.ZooKeeperCache.Deserializer;

/**
 * Wrapper class allowing classes of instance ModularLoadManager to be compatible with the interface LoadManager.
 */
public class ModularLoadManagerWrapper implements LoadManager {
    private ModularLoadManager loadManager;

    public ModularLoadManagerWrapper(final ModularLoadManager loadManager) {
        this.loadManager = loadManager;
    }

    @Override
    public void disableBroker() throws Exception {
        loadManager.disableBroker();
    }

    @Override
    public void doLoadShedding() {
        loadManager.doLoadShedding();
    }

    @Override
    public void doNamespaceBundleSplit() {
        loadManager.doNamespaceBundleSplit();
    }

    @Override
    public LoadReport generateLoadReport() {
        loadManager.updateLocalBrokerData();
        return null;
    }

    @Override
    public ResourceUnit getLeastLoaded(final ServiceUnitId serviceUnit) {
        return new SimpleResourceUnit(String.format("http://%s", loadManager.selectBrokerForAssignment(serviceUnit)),
                new PulsarResourceDescription());
    }

    @Override
    public List<Metrics> getLoadBalancingMetrics() {
        return Collections.emptyList();
    }

    @Override
    public void initialize(final PulsarService pulsar) {
        loadManager.initialize(pulsar);
    }

    @Override
    public boolean isCentralized() {
        return true;
    }

    @Override
    public void setLoadReportForceUpdateFlag() {

    }

    @Override
    public void start() throws PulsarServerException {
        loadManager.start();
    }

    @Override
    public void stop() throws PulsarServerException {
        loadManager.stop();
    }

    @Override
    public void writeLoadReportOnZookeeper() {
        loadManager.writeBrokerDataOnZooKeeper();
    }

    @Override
    public void writeResourceQuotasToZooKeeper() {
        loadManager.writeBundleDataOnZooKeeper();
    }

    @Override
    public Deserializer<? extends ServiceLookupData> getLoadReportDeserializer() {
        return loadManager.getLoadReportDeserializer();
    }
}
