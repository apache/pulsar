package com.yahoo.pulsar.broker.loadbalance.impl;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.broker.LocalBrokerData;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.TimeAverageMessageData;
import com.yahoo.pulsar.broker.loadbalance.LoadData;
import com.yahoo.pulsar.broker.loadbalance.LoadSheddingStrategy;

/**
 * Load shedding strategy which will attempt to shed exactly one bundle on brokers which are overloaded, that is, whose
 * maximum system resource usage exceeds loadBalancerBrokerOverloadedThresholdPercentage. A bundle will be recommended
 * for unloading off that broker if and only if the following conditions hold: The broker has at least two bundles
 * assigned and the broker has at least one bundle that has not been unloaded recently according to
 * LoadBalancerSheddingGracePeriodMinutes. The unloaded bundle will be the most expensive bundle in terms of message
 * rate that has not been recently unloaded.
 */
public class OverloadShedder implements LoadSheddingStrategy {
    private static final Logger log = LoggerFactory.getLogger(OverloadShedder.class);
    private Map<String, String> selectedBundlesCache;

    /**
     * Create an OverloadShedder with the service configuration.
     * 
     * @param conf
     *            Service configuration to create from.
     */
    public OverloadShedder(final ServiceConfiguration conf) {
        selectedBundlesCache = new HashMap<>();
    }

    /**
     * Attempt to shed one bundle off every broker which is overloaded.
     * 
     * @param loadData
     *            The load data to used to make the unloading decision.
     * @param conf
     *            The service configuration.
     * @return A map from bundles to unload to the brokers on which they are loaded.
     */
    public Map<String, String> findBundlesForUnloading(final LoadData loadData, final ServiceConfiguration conf) {
        selectedBundlesCache.clear();
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        final Map<String, Long> recentlyUnloadedBundles = loadData.getRecentlyUnloadedBundles();
        for (final Map.Entry<String, BrokerData> entry : loadData.getBrokerData().entrySet()) {
            final String broker = entry.getKey();
            final BrokerData brokerData = entry.getValue();
            final LocalBrokerData localData = brokerData.getLocalData();
            final double maxUsage = localData.getMaxResourceUsage();
            if (maxUsage >= overloadThreshold) {
                log.info("Attempting to shed load on {}, which has max resource usage {}%", broker, maxUsage);
                double maxMessageRate = Double.NEGATIVE_INFINITY;
                String mostTaxingBundle = null;
                if (localData.getBundles().size() > 1) {
                    for (final String bundle : localData.getBundles()) {
                        final BundleData bundleData = loadData.getBundleData().get(bundle);
                        // Consider short-term message rate to address system resource burden
                        final TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                        final double messageRate = shortTermData.getMsgRateIn() + shortTermData.getMsgRateOut();
                        // The burden of checking the timestamp is for the load manager, not the strategy.
                        if (messageRate > maxMessageRate && !recentlyUnloadedBundles.containsKey(bundle)) {
                            maxMessageRate = messageRate;
                            mostTaxingBundle = bundle;
                        }
                    }
                    if (mostTaxingBundle != null) {
                        selectedBundlesCache.put(broker, mostTaxingBundle);
                    } else {
                        log.warn("Load shedding could not be performed on broker {} because all bundles assigned to it "
                                + "have recently been unloaded");
                    }
                } else if (localData.getBundles().size() == 1) {
                    log.warn(
                            "HIGH USAGE WARNING : Sole namespace bundle {} is overloading broker {}. "
                                    + "No Load Shedding will be done on this broker",
                            localData.getBundles().iterator().next(), broker);
                } else {
                    log.warn("Broker {} is overloaded despite having no bundles", broker);
                }
            }
        }
        return selectedBundlesCache;
    }
}
