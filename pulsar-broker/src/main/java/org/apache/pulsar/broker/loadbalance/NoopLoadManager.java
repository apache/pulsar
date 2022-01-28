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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.impl.PulsarResourceDescription;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceUnit;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;

@Slf4j
public class NoopLoadManager implements LoadManager {

    private PulsarService pulsar;
    private String lookupServiceAddress;
    private ResourceUnit localResourceUnit;
    private LockManager<LocalBrokerData> lockManager;

    @Override
    public void initialize(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.lockManager = pulsar.getCoordinationService().getLockManager(LocalBrokerData.class);
    }

    @Override
    public void start() throws PulsarServerException {
        lookupServiceAddress = getBrokerAddress();
        localResourceUnit = new SimpleResourceUnit(String.format("http://%s", lookupServiceAddress),
                new PulsarResourceDescription());

        LocalBrokerData localData = new LocalBrokerData(pulsar.getSafeWebServiceAddress(),
                pulsar.getWebServiceAddressTls(),
                pulsar.getBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls(), pulsar.getAdvertisedListeners());
        localData.setProtocols(pulsar.getProtocolDataToAdvertise());
        String brokerReportPath = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + lookupServiceAddress;

        try {
            log.info("Acquiring broker resource lock on {}", brokerReportPath);
            lockManager.acquireLock(brokerReportPath, localData).join();
            log.info("Acquired broker resource lock on {}", brokerReportPath);
        } catch (CompletionException ce) {
            throw new PulsarServerException(MetadataStoreException.unwrap(ce));
        }
    }

    private String getBrokerAddress() {
        return String.format("%s:%s", pulsar.getAdvertisedAddress(),
                pulsar.getConfiguration().getWebServicePort().isPresent()
                        ? pulsar.getConfiguration().getWebServicePort().get()
                        : pulsar.getConfiguration().getWebServicePortTls().get());
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
    public CompletableFuture<Set<String>> getAvailableBrokersAsync() {
        return CompletableFuture.completedFuture(Collections.singleton(lookupServiceAddress));
    }

    @Override
    public void stop() throws PulsarServerException {
        if (lockManager != null) {
            try {
                lockManager.close();
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }
        }
    }

}
