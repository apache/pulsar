package com.yahoo.pulsar.broker.loadbalance.impl;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.TimeAverageBrokerData;
import com.yahoo.pulsar.broker.TimeAverageMessageData;
import com.yahoo.pulsar.broker.loadbalance.LoadData;
import com.yahoo.pulsar.broker.loadbalance.ModularLoadManagerStrategy;

/**
 * Placement strategy which selects a broker based on which one has the least
 * long term message rate.
 */
public class LeastLongTermMessageRate implements ModularLoadManagerStrategy {
	private static Logger log = LoggerFactory.getLogger(LeastLongTermMessageRate.class);

	// Maintain this list to reduce object creation.
	private ArrayList<String> bestBrokers;

	public LeastLongTermMessageRate(final ServiceConfiguration conf) {
		bestBrokers = new ArrayList<>();
	}

	// Form a score for a broker using its preallocated bundle data and time
	// average data.
	private static double getScore(final BrokerData brokerData) {
		double totalMessageRate = 0;
		for (BundleData bundleData : brokerData.getPreallocatedBundleData().values()) {
			final TimeAverageMessageData longTermData = bundleData.getLongTermData();
			totalMessageRate += longTermData.getMsgRateIn() + longTermData.getMsgRateOut();
		}
		final TimeAverageBrokerData timeAverageData = brokerData.getTimeAverageData();
		return totalMessageRate + timeAverageData.getLongTermMsgRateIn() + timeAverageData.getLongTermMsgRateOut();
	}

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
	@Override
	public String selectBroker(final Set<String> candidates, final BundleData bundleToAssign, final LoadData loadData,
			final ServiceConfiguration conf) {
		bestBrokers.clear();
		double minScore = Double.POSITIVE_INFINITY;
		// Maintain of list of all the best scoring brokers and then randomly
		// select one of them at the end.
		for (String broker : candidates) {
			final double score = getScore(loadData.getBrokerData().get(broker));
			log.info("{} got score {}", broker, score);
			if (score < minScore) {
				// Clear best brokers since this score beats the other brokers.
				bestBrokers.clear();
				bestBrokers.add(broker);
				minScore = score;
			} else if (score == minScore) {
				// Add this broker to best brokers since it ties with the best
				// score.
				bestBrokers.add(broker);
			}
		}
		return bestBrokers.get(ThreadLocalRandom.current().nextInt(bestBrokers.size()));
	}
}
