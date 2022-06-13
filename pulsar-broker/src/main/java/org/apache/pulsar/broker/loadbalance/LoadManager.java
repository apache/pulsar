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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LoadManager runs through set of load reports collected from different brokers and generates a recommendation of
 * namespace/ServiceUnit placement on machines/ResourceUnit. Each Concrete Load Manager will use different algorithms to
 * generate this mapping.
 *
 * Concrete Load Manager is also return the least loaded broker that should own the new namespace.
 */
public interface LoadManager {
    Logger LOG = LoggerFactory.getLogger(LoadManager.class);

    String LOADBALANCE_BROKERS_ROOT = "/loadbalance/brokers";

    void start() throws PulsarServerException;

    /**
     * Is centralized decision making to assign a new bundle.
     */
    boolean isCentralized();

    /**
     * Returns the Least Loaded Resource Unit decided by some algorithm or criteria which is implementation specific.
     */
    Optional<ResourceUnit> getLeastLoaded(ServiceUnitId su) throws Exception;

    /**
     * Generate the load report.
     */
    LoadManagerReport generateLoadReport() throws Exception;

    /**
     * Set flag to force load report update.
     */
    void setLoadReportForceUpdateFlag();

    /**
     * Publish the current load report on ZK.
     */
    void writeLoadReportOnZookeeper() throws Exception;

    /**
     * Publish the current load report on ZK, forced or not.
     * By default rely on method writeLoadReportOnZookeeper().
     */
    default void writeLoadReportOnZookeeper(boolean force) throws Exception {
        writeLoadReportOnZookeeper();
    }

    /**
     * Update namespace bundle resource quota on ZK.
     */
    void writeResourceQuotasToZooKeeper() throws Exception;

    /**
     * Generate load balancing stats metrics.
     */
    List<Metrics> getLoadBalancingMetrics();

    /**
     * Unload a candidate service unit to balance the load.
     */
    void doLoadShedding();

    /**
     * Namespace bundle split.
     */
    void doNamespaceBundleSplit() throws Exception;

    /**
     * Removes visibility of current broker from loadbalancer list so, other brokers can't redirect any request to this
     * broker and this broker won't accept new connection requests.
     *
     * @throws Exception
     */
    void disableBroker() throws Exception;

    /**
     * Get list of available brokers in cluster.
     *
     * @return
     * @throws Exception
     */
    Set<String> getAvailableBrokers() throws Exception;

    CompletableFuture<Set<String>> getAvailableBrokersAsync();

    void stop() throws PulsarServerException;

    /**
     * Initialize this LoadManager.
     *
     * @param pulsar
     *            The service to initialize this with.
     */
    void initialize(PulsarService pulsar);

    static LoadManager create(final PulsarService pulsar) {
        try {
            final ServiceConfiguration conf = pulsar.getConfiguration();
            final Class<?> loadManagerClass = Class.forName(conf.getLoadManagerClassName());
            // Assume there is a constructor with one argument of PulsarService.
            final Object loadManagerInstance = loadManagerClass.getDeclaredConstructor().newInstance();
            if (loadManagerInstance instanceof LoadManager) {
                final LoadManager casted = (LoadManager) loadManagerInstance;
                casted.initialize(pulsar);
                return casted;
            } else if (loadManagerInstance instanceof ModularLoadManager) {
                final LoadManager casted = new ModularLoadManagerWrapper((ModularLoadManager) loadManagerInstance);
                casted.initialize(pulsar);
                return casted;
            }
        } catch (Exception e) {
            LOG.warn("Error when trying to create load manager: ", e);
        }
        // If we failed to create a load manager, default to SimpleLoadManagerImpl.
        return new SimpleLoadManagerImpl(pulsar);
    }

}
