package com.yahoo.pulsar.broker.loadbalance;

import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.broker.ServiceConfiguration;

import java.util.Set;

/**
 * Load management component which determines what brokers should not be considered for topic placement by the placement
 * strategy. For example, the placement strategy may determine that the broker with the least msg/s should get the
 * bundle assignment, but we may not want to consider brokers whose CPU usage is very high. Thus, we could use a filter
 * to blacklist brokers with high CPU usage.
 */
public interface BrokerFilter {

    /**
     * From the given set of available broker candidates, filter those using the load data.
     * @param brokers The currently available brokers that have not already been filtered. This set may be modified
     *                by filter.
     * @param bundleToAssign The data for the bundle to assign.
     * @param loadData The load data from the leader broker.
     * @param conf The service configuration.
     */
    void filter(Set<String> brokers, BundleData bundleToAssign, LoadData loadData, ServiceConfiguration conf);

    static BrokerFilter create(final ServiceConfiguration conf) {
        // TODO
        return null;
    }
}
