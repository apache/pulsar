package com.yahoo.pulsar.broker.loadbalance;

import java.util.Map;

import com.yahoo.pulsar.broker.ServiceConfiguration;

/**
 * Load management component which determines the criteria for unloading
 * bundles.
 */
public interface LoadSheddingStrategy {

	/**
	 * Recommend that all of the returned bundles be unloaded.
	 * 
	 * @param loadData
	 *            The load data to used to make the unloading decision.
	 * @param conf
	 *            The service configuration.
	 * @return A map from all selected bundles to the brokers on which they
	 *         reside.
	 */
	Map<String, String> selectBundlesForUnloading(LoadData loadData, ServiceConfiguration conf);
}
