package com.yahoo.pulsar.broker.loadbalance;

import com.yahoo.pulsar.broker.PulsarServerException;

/**
 * New proposal for a load manager interface which attempts to use more
 * intuitive method names and provide a starting place for new load manager
 * proposals.
 */
public interface ModularLoadManager {

	/**
	 * As any broker, disable the broker this manager is running on.
	 * 
	 * @throws PulsarServerException
	 *             If ZooKeeper failed to disable the broker.
	 */
	void disableBroker() throws PulsarServerException;

	/**
	 * As the leader broker, select bundles for the namespace service to unload
	 * so that they may be reassigned to new brokers.
	 */
	void doLoadShedding();

	/**
	 * As the leader broker, attempt to automatically detect and split hot
	 * namespace bundles.
	 */
	void doNamespaceBundleSplit();

	/**
	 * Get the broker root ZooKeeper path.
	 */
	String getBrokerRoot();

	/**
	 * As the leader broker, find a suitable broker for the assignment of the
	 * given bundle.
	 * 
	 * @param bundleToAssign
	 *            Full name of the bundle to assign.
	 * @return The name of the selected broker, as it appears on ZooKeeper.
	 */
	String selectBrokerForAssignment(String bundleToAssign);

	/**
	 * As any broker, retrieve the namespace bundle stats and system resource
	 * usage to update data local to this broker.
	 */
	void updateLocalBrokerData();

	/**
	 * As any broker, start the load manager.
	 * 
	 * @throws PulsarServerException
	 *             If an unexpected error prevented the load manager from being
	 *             started.
	 */
	void start() throws PulsarServerException;

	/**
	 * As any broker, stop the load manager.
	 * 
	 * @throws PulsarServerException
	 *             If an unexpected error occurred when attempting to stop the
	 *             load manager.
	 */
	void stop() throws PulsarServerException;

	/**
	 * As any broker, write the local broker data to ZooKeeper.
	 */
	void writeBrokerDataOnZooKeeper();

	/**
	 * As the leader broker, write bundle data aggregated from all brokers to
	 * ZooKeeper.
	 */
	void writeBundleDataOnZooKeeper();
}
