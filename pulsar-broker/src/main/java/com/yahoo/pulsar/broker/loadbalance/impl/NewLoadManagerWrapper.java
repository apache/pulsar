package com.yahoo.pulsar.broker.loadbalance.impl;

import com.yahoo.pulsar.broker.PulsarServerException;
import com.yahoo.pulsar.common.policies.data.loadbalancer.LoadReport;
import com.yahoo.pulsar.broker.loadbalance.NewLoadManager;
import com.yahoo.pulsar.broker.loadbalance.LoadManager;
import com.yahoo.pulsar.broker.loadbalance.ResourceUnit;
import com.yahoo.pulsar.broker.stats.Metrics;
import com.yahoo.pulsar.common.naming.ServiceUnitId;

import java.util.Collections;
import java.util.List;

/**
 * Wrapper class allowing classes of instance NewLoadManager to be compatible with the interface LoadManager.
 */
public class NewLoadManagerWrapper implements LoadManager {
    private NewLoadManager loadManager;

    public NewLoadManagerWrapper(final NewLoadManager loadManager) {
        this.loadManager = loadManager;
    }

    @Override
    public void disableBroker() throws Exception {
        loadManager.disableBroker();
    }

    @Override
    public void doLoadShedding() {
        loadManager.doLoadShedding();
    }

    @Override
    public void doNamespaceBundleSplit() {
        loadManager.doNamespaceBundleSplit();
    }

    @Override
    public LoadReport generateLoadReport() {
        loadManager.updateLocalBrokerData();
        return null;
    }

    @Override
    public String getBrokerRoot() {
        return loadManager.getBrokerRoot();
    }

    @Override
    public ResourceUnit getLeastLoaded(final ServiceUnitId serviceUnit) {
        return new SimpleResourceUnit(String.format("http://%s",
                loadManager.selectBrokerForAssignment(serviceUnit.toString())),
                new PulsarResourceDescription());
    }

    @Override
    public List<Metrics> getLoadBalancingMetrics() {
        return Collections.emptyList();
    }

    @Override
    public boolean isCentralized() {
        return true;
    }

    @Override
    public void setLoadReportForceUpdateFlag() {

    }

    @Override
    public void start() throws PulsarServerException {
        loadManager.start();
    }

    @Override
    public void stop() throws PulsarServerException {
        loadManager.stop();
    }

    @Override
    public void writeLoadReportOnZookeeper() {
        loadManager.writeBrokerDataOnZooKeeper();
    }

    @Override
    public void writeResourceQuotasToZooKeeper() {
        loadManager.writeBundleDataOnZooKeeper();
    }
}
