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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.yahoo.pulsar.broker.PulsarServerException;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.loadbalance.LoadManager;
import com.yahoo.pulsar.broker.loadbalance.ModularLoadManager;
import com.yahoo.pulsar.broker.loadbalance.ResourceUnit;
import com.yahoo.pulsar.common.naming.ServiceUnitId;
import com.yahoo.pulsar.common.policies.data.loadbalancer.LoadReport;
import com.yahoo.pulsar.common.policies.data.loadbalancer.ServiceLookupData;
import com.yahoo.pulsar.common.policies.data.loadbalancer.SystemResourceUsage;
import com.yahoo.pulsar.common.stats.Metrics;
import com.yahoo.pulsar.zookeeper.ZooKeeperCache.Deserializer;

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
