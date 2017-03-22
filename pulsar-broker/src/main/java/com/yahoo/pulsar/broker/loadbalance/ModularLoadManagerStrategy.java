package com.yahoo.pulsar.broker.loadbalance;

import java.util.Set;

import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate;

/**
 * Interface which serves as a component for ModularLoadManagerImpl, flexibly
 * allowing the injection of potentially complex strategies.
 */
public interface ModularLoadManagerStrategy {

	/**
	 * Find a suitable broker to assign the given bundle to.
	 * 
	 * @param candidates
	 *            The candidates for which the bundle may be assigned.
	 * @param bundleToAssign
	 *            The data for the bundle to assign.
	 * @param loadData
	 *            The load data from the leader broker.
	 * @param conf
	 *            The service configuration.
	 * @return The name of the selected broker as it appears on ZooKeeper.
	 */
	String selectBroker(Set<String> candidates, BundleData bundleToAssign, LoadData loadData,
			ServiceConfiguration conf);

	/**
	 * Create a placement strategy using the configuration.
	 * 
	 * @param conf
	 *            ServiceConfiguration to use.
	 * @return A placement strategy from the given configurations.
	 */
	static ModularLoadManagerStrategy create(final ServiceConfiguration conf) {
		try {
			final Class<?> placementStrategyClass = Class.forName(conf.getModularPlacementStrategyClassName());

			// Assume there is a constructor of one argument of
			// ServiceConfiguration.
			return (ModularLoadManagerStrategy) placementStrategyClass.getConstructor(ServiceConfiguration.class)
					.newInstance(conf);
		} catch (Exception e) {
			// Ignore
		}
		return new LeastLongTermMessageRate(conf);
	}
}
