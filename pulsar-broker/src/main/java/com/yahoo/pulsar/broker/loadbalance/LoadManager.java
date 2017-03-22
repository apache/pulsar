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

import com.yahoo.pulsar.broker.PulsarServerException;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import com.yahoo.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import com.yahoo.pulsar.broker.stats.Metrics;
import com.yahoo.pulsar.common.naming.ServiceUnitId;
import com.yahoo.pulsar.common.policies.data.loadbalancer.LoadReport;

/**
 * LoadManager runs though set of load reports collected from different brokers
 * and generates a recommendation of namespace/ServiceUnit placement on
 * machines/ResourceUnit. Each Concrete Load Manager will use different
 * algorithms to generate this mapping.
 *
 * Concrete Load Manager is also return the least loaded broker that should own
 * the new namespace.
 */
public interface LoadManager {

	public void start() throws PulsarServerException;

	/**
	 * Is centralized decision making to assign a new bundle.
	 */
	boolean isCentralized();

	/**
	 * Returns the Least Loaded Resource Unit decided by some algorithm or
	 * criteria which is implementation specific
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
	 * Determine the broker root.
	 */
	String getBrokerRoot();

	/**
	 * Removes visibility of current broker from loadbalancer list so, other
	 * brokers can't redirect any request to this broker and this broker won't
	 * accept new connection requests.
	 *
	 * @throws Exception
	 */
	public void disableBroker() throws Exception;

	public void stop() throws PulsarServerException;

	static LoadManager create(final PulsarService pulsar) {
		try {
			final ServiceConfiguration conf = pulsar.getConfiguration();
			final Class<?> loadManagerClass = Class.forName(conf.getLoadManagerClassName());
			// Assume there is a constructor with one argument of PulsarService.
			final Object loadManagerInstance = loadManagerClass.getConstructor(PulsarService.class).newInstance(pulsar);
			if (loadManagerInstance instanceof LoadManager) {
				return (LoadManager) loadManagerInstance;
			} else if (loadManagerInstance instanceof ModularLoadManager) {
				return new ModularLoadManagerWrapper((ModularLoadManager) loadManagerInstance);
			}
		} catch (Exception e) {
			// Ignore
		}
		// If we failed to create a load manager, default to
		// SimpleLoadManagerImpl.
		return new SimpleLoadManagerImpl(pulsar);
	}
}
