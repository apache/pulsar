package com.yahoo.pulsar.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Data class containing three components comprising all the data available for
 * the leader broker about other brokers: - The local broker data which is
 * written to ZooKeeper by each individual broker (LocalBrokerData). - The time
 * average bundle data which is written to ZooKeeper by the leader broker
 * (TimeAverageBrokerData). - The preallocated bundles which are not written to
 * ZooKeeper but are maintained by the leader broker (Map<String, BundleData>).
 */
public class BrokerData {
	private LocalBrokerData localData;
	private TimeAverageBrokerData timeAverageData;
	private Map<String, BundleData> preallocatedBundleData;

	/**
	 * Initialize this BrokerData using the most recent local data.
	 * 
	 * @param localData
	 *            The data local for the broker.
	 */
	public BrokerData(final LocalBrokerData localData) {
		this.localData = localData;
		timeAverageData = new TimeAverageBrokerData();
		preallocatedBundleData = new ConcurrentHashMap<>();
	}

	public LocalBrokerData getLocalData() {
		return localData;
	}

	public void setLocalData(LocalBrokerData localData) {
		this.localData = localData;
	}

	public TimeAverageBrokerData getTimeAverageData() {
		return timeAverageData;
	}

	public void setTimeAverageData(TimeAverageBrokerData timeAverageData) {
		this.timeAverageData = timeAverageData;
	}

	public Map<String, BundleData> getPreallocatedBundleData() {
		return preallocatedBundleData;
	}

	public void setPreallocatedBundleData(Map<String, BundleData> preallocatedBundleData) {
		this.preallocatedBundleData = preallocatedBundleData;
	}
}
