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
package com.yahoo.pulsar.broker.loadbalance;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.PulsarServerException;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import com.yahoo.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import com.yahoo.pulsar.broker.stats.Metrics;
import com.yahoo.pulsar.common.naming.ServiceUnitId;
import com.yahoo.pulsar.common.policies.data.loadbalancer.LoadReport;

/**
 * LoadManager runs though set of load reports collected from different brokers and generates a recommendation of
 * namespace/ServiceUnit placement on machines/ResourceUnit. Each Concrete Load Manager will use different algorithms to
 * generate this mapping.
 *
 * Concrete Load Manager is also return the least loaded broker that should own the new namespace.
 */
public interface LoadManager {
    Logger log = LoggerFactory.getLogger(LoadManager.class);

    String LOADBALANCE_BROKERS_ROOT = "/loadbalance/brokers";

    public void start() throws PulsarServerException;

    /**
     * Is centralized decision making to assign a new bundle.
     */
    boolean isCentralized();

    /**
     * Returns the Least Loaded Resource Unit decided by some algorithm or criteria which is implementation specific
     */
    ResourceUnit getLeastLoaded(ServiceUnitId su) throws Exception;

    /**
     * Generate the load report
     */
    LoadReport generateLoadReport() throws Exception;

    /**
     * Set flag to force load report update
     */
    void setLoadReportForceUpdateFlag();

    /**
     * Publish the current load report on ZK
     */
    void writeLoadReportOnZookeeper() throws Exception;

    /**
     * Update namespace bundle resource quota on ZK
     */
    void writeResourceQuotasToZooKeeper() throws Exception;

    /**
     * Generate load balancing stats metrics
     */
    List<Metrics> getLoadBalancingMetrics();

    /**
     * Unload a candidate service unit to balance the load
     */
    void doLoadShedding();

    /**
     * Namespace bundle split
     */
    void doNamespaceBundleSplit() throws Exception;

    /**
     * Removes visibility of current broker from loadbalancer list so, other brokers can't redirect any request to this
     * broker and this broker won't accept new connection requests.
     *
     * @throws Exception
     */
    public void disableBroker() throws Exception;

    public void stop() throws PulsarServerException;

    /**
     * Initialize this LoadManager.
     * 
     * @param pulsar
     *            The service to initialize this with.
     */
    public void initialize(PulsarService pulsar);

    static LoadManager create(final PulsarService pulsar) {
        try {
            final ServiceConfiguration conf = pulsar.getConfiguration();
            final Class<?> loadManagerClass = Class.forName(conf.getLoadManagerClassName());
            // Assume there is a constructor with one argument of PulsarService.
            final Object loadManagerInstance = loadManagerClass.newInstance();
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
            log.warn("Error when trying to create load manager: {}");
        }
        // If we failed to create a load manager, default to SimpleLoadManagerImpl.
        return new SimpleLoadManagerImpl(pulsar);
    }
}
