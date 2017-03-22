package com.yahoo.pulsar.broker.loadbalance.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.loadbalance.LoadData;
import com.yahoo.pulsar.broker.loadbalance.LoadSheddingStrategy;

/**
 * An abstract class which makes a LoadSheddingStrategy which makes decisions
 * based on standard deviation easier to implement.
 */
public abstract class DeviationShedder implements LoadSheddingStrategy {
	// A Set of pairs is used in favor of a Multimap for simplicity.
	protected TreeSet<Pair<Double, String>> metricTreeSetCache;
	protected TreeSet<Pair<Double, String>> bundleTreeSetCache;

	/**
	 * Initialize this DeviationShedder.
	 */
	public DeviationShedder() {
		bundleTreeSetCache = new TreeSet<>();
		metricTreeSetCache = new TreeSet<>();
	}

	// Measure the load incurred by a bundle.
	protected abstract double bundleValue(String bundle, BrokerData brokerData, ServiceConfiguration conf);

	// Measure the load suffered by a broker.
	protected abstract double brokerValue(BrokerData brokerData, ServiceConfiguration conf);

	// Get the threshold above which the standard deviation of a broker is large
	// enough to warrant unloading bundles.
	protected abstract double getDeviationThreshold(ServiceConfiguration conf);

	/**
	 * Recommend that all of the returned bundles be unloaded based on observing
	 * obsessive standard deviations according to some metric.
	 * 
	 * @param loadData
	 *            The load data to used to make the unloading decision.
	 * @param conf
	 *            The service configuration.
	 * @return A map from all selected bundles to the brokers on which they
	 *         reside.
	 */
	@Override
	public Map<String, String> selectBundlesForUnloading(final LoadData loadData, final ServiceConfiguration conf) {
		final Map<String, String> result = new HashMap<>();
		bundleTreeSetCache.clear();
		metricTreeSetCache.clear();
		double sum = 0;
		double squareSum = 0;
		final Map<String, BrokerData> brokerDataMap = loadData.getBrokerData();

		// Treating each broker as a data point, calculate the sum and squared
		// sum of the evaluated broker metrics.
		// These may be used to calculate the standard deviation.
		for (Map.Entry<String, BrokerData> entry : brokerDataMap.entrySet()) {
			final double value = brokerValue(entry.getValue(), conf);
			sum += value;
			squareSum += value * value;
			metricTreeSetCache.add(new ImmutablePair<>(value, entry.getKey()));
		}
		// Mean cannot change by just moving around bundles.
		final double mean = sum / brokerDataMap.size();
		double standardDeviation = Math.sqrt(squareSum / brokerDataMap.size() - mean * mean);
		final double deviationThreshold = getDeviationThreshold(conf);
		String lastMostOverloaded = null;
		// While the most loaded broker is above the standard deviation
		// threshold, continue to move bundles.
		while ((metricTreeSetCache.last().getKey() - mean) / standardDeviation > deviationThreshold) {
			final Pair<Double, String> mostLoadedPair = metricTreeSetCache.last();
			final double highestValue = mostLoadedPair.getKey();
			final String mostLoaded = mostLoadedPair.getValue();

			final Pair<Double, String> leastLoadedPair = metricTreeSetCache.first();
			final double leastValue = leastLoadedPair.getKey();
			final String leastLoaded = metricTreeSetCache.first().getValue();

			if (!mostLoaded.equals(lastMostOverloaded)) {
				// Reset the bundle tree set now that a different broker is
				// being considered.
				bundleTreeSetCache.clear();
				for (String bundle : brokerDataMap.get(mostLoaded).getLocalData().getBundles()) {
					if (!result.containsKey(bundle)) {
						// Don't consider bundles that are already going to be
						// moved.
						bundleTreeSetCache.add(
								new ImmutablePair<>(bundleValue(bundle, brokerDataMap.get(mostLoaded), conf), bundle));
					}
				}
				lastMostOverloaded = mostLoaded;
			}
			boolean selected = false;
			while (!(bundleTreeSetCache.isEmpty() || selected)) {
				Pair<Double, String> mostExpensivePair = bundleTreeSetCache.pollLast();
				double loadIncurred = mostExpensivePair.getKey();
				// When the bundle is moved, we want the now least loaded server
				// to have lower overall load than the
				// most loaded server does not. Thus, we will only consider
				// moving the bundle if this condition
				// holds, and otherwise we will try the next bundle.
				if (loadIncurred + leastValue < highestValue) {
					// Update the standard deviation and replace the old load
					// values in the broker tree set with the
					// load values assuming this move took place.
					final String bundleToMove = mostExpensivePair.getValue();
					result.put(bundleToMove, mostLoaded);
					metricTreeSetCache.remove(mostLoadedPair);
					metricTreeSetCache.remove(leastLoadedPair);
					final double newHighLoad = highestValue - loadIncurred;
					final double newLowLoad = leastValue - loadIncurred;
					squareSum -= highestValue * highestValue + leastValue * leastValue;
					squareSum += newHighLoad * newHighLoad + newLowLoad * newLowLoad;
					standardDeviation = Math.sqrt(squareSum / brokerDataMap.size() - mean * mean);
					metricTreeSetCache.add(new ImmutablePair<>(newLowLoad, leastLoaded));
					metricTreeSetCache.add(new ImmutablePair<>(newHighLoad, mostLoaded));
					selected = true;
				}
			}
			if (!selected) {
				// Move on to the next broker if no bundle could be moved.
				metricTreeSetCache.pollLast();
			}
		}
		return result;
	}
}
