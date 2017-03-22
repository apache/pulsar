package com.yahoo.pulsar.broker.loadbalance;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.BundleData;

/**
 * This class represents all data that could be relevant when making a load
 * management decision.
 */
public class LoadData {
	/**
	 * Map from broker names to their available data.
	 */
	private final Map<String, BrokerData> brokerData;

	/**
	 * Map from bundle names to their time-sensitive aggregated data.
	 */
	private final Map<String, BundleData> bundleData;

	/**
	 * Initialize a LoadData.
	 */
	public LoadData() {
		this.brokerData = new ConcurrentHashMap<>();
		this.bundleData = new ConcurrentHashMap<>();
	}

	public Map<String, BrokerData> getBrokerData() {
		return brokerData;
	}

	public Map<String, BundleData> getBundleData() {
		return bundleData;
	}
}
