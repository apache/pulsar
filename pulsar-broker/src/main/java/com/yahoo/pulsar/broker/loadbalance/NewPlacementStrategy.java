package com.yahoo.pulsar.broker.loadbalance;

import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate;

import java.util.Set;

/**
 * Interface which serves as a component for NewLoadManagerImpl, flexibly allowing the injection of potentially
 * complex strategies.
 */
public interface NewPlacementStrategy {

    /**
     * Find a suitable broker to assign the given bundle to.
     * @param candidates The candidates for which the bundle may be assigned.
     * @param bundleToAssign The data for the bundle to assign.
     * @param loadData The load data from the leader broker.
     * @param conf The service configuration.
     * @return The name of the selected broker as it appears on ZooKeeper.
     */
    String selectBroker(Set<String> candidates, BundleData bundleToAssign, LoadData loadData,
                        ServiceConfiguration conf);

    /**
     * Create a placement strategy using the configuration.
     * @param conf ServiceConfiguration to use.
     * @return A placement strategy from the given configurations.
     */
    static NewPlacementStrategy create(final ServiceConfiguration conf) {
        switch (conf.getNewPlacementStrategyName()) {
            case "LeastLongTermMessageRate":
            default:
                return new LeastLongTermMessageRate();
        }
    }
}
