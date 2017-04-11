package com.yahoo.pulsar.broker.loadbalance;

import com.yahoo.pulsar.broker.LocalBrokerData;
import com.yahoo.pulsar.broker.ServiceConfiguration;

/**
 * DataUpdateCondition instances are injecting into ModularLoadManagers to determine when local broker data should be
 * written to ZooKeeper or not.
 */
public interface DataUpdateCondition {
    /**
     * Determine if the LocalBrokerData should be written to ZooKeeper by comparing the old data to the new data.
     * 
     * @param oldData
     *            Data available before the most recent local update.
     * @param newData
     *            Most recently available data.
     * @param conf
     *            Configuration to use to determine whether the new data should be written.
     * @return true if the new data should be written to ZooKeeper, false otherwise.
     */
    boolean shouldUpdate(LocalBrokerData oldData, LocalBrokerData newData, ServiceConfiguration conf);
}
