/*
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
package org.apache.pulsar.broker.loadbalance.extensions;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;

public class ExtensibleLoadManagerWrapper implements LoadManager {

    private PulsarService pulsar;

    private final ExtensibleLoadManagerImpl loadManager;

    public ExtensibleLoadManagerWrapper(ExtensibleLoadManagerImpl loadManager) {
        this.loadManager = loadManager;
    }

    @Override
    public void start() throws PulsarServerException {
        loadManager.start();
    }

    @Override
    public void initialize(PulsarService pulsar) {
        loadManager.initialize(pulsar);
        this.pulsar = pulsar;
    }

    @Override
    public boolean isCentralized() {
        return true;
    }

    @Override
    public CompletableFuture<Optional<LookupResult>> findBrokerServiceUrl(
            Optional<ServiceUnitId> topic, ServiceUnitId bundle) {
        return loadManager.assign(topic, bundle)
                .thenApply(lookupData -> lookupData.map(BrokerLookupData::toLookupResult));
    }

    @Override
    public CompletableFuture<Boolean> checkOwnershipAsync(Optional<ServiceUnitId> topic, ServiceUnitId bundle) {
        return loadManager.checkOwnershipAsync(topic, bundle);
    }

    @Override
    public void disableBroker() throws Exception {
        this.loadManager.getBrokerRegistry().unregister();
    }

    @Override
    public Set<String> getAvailableBrokers() throws Exception {
        return getAvailableBrokersAsync()
                .get(pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public CompletableFuture<Set<String>> getAvailableBrokersAsync() {
        return this.loadManager.getBrokerRegistry().getAvailableBrokersAsync().thenApply(HashSet::new);
    }

    @Override
    public String setNamespaceBundleAffinity(String bundle, String broker) {
        // TODO: Add namespace bundle affinity support.
        return null;
    }

    @Override
    public void stop() throws PulsarServerException {
        this.loadManager.close();
    }


    @Override
    public Optional<ResourceUnit> getLeastLoaded(ServiceUnitId su) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public LoadManagerReport generateLoadReport() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLoadReportForceUpdateFlag() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLoadReportOnZookeeper() throws Exception {
        // No-op, this operation is not useful, the load data reporter will automatically write.
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeResourceQuotasToZooKeeper() throws Exception {
        // No-op, this operation is not useful, the load data reporter will automatically write.
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Metrics> getLoadBalancingMetrics() {
        return loadManager.getMetrics();
    }

    @Override
    public void doLoadShedding() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void doNamespaceBundleSplit() {
        throw new UnsupportedOperationException();
    }

}
