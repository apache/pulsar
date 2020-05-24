package org.apache.pulsar.broker.loadbalance.impl;

import lombok.val;
import org.apache.pulsar.broker.OverallBandwidthBrokerData;
import org.apache.pulsar.broker.loadbalance.BillingData;

public class ModularLoadManagerWithBillingImpl extends ModularLoadManagerImpl {
    private final BillingData billingData;

    // Path to ZNode containing BillingData jsons for each broker.
    public static final String BILLING_DATA_BROKER_ZPATH = "/loadbalance/billing-data";

    public ModularLoadManagerWithBillingImpl() {
        super();
        this.billingData = new BillingData();
    }

    private void updateBillingData() {
        val bandwidthData = billingData.getBandwidthData();

        // Iterate over the broker data and update the bandwidth counters for the billing.
        loadData.getBrokerData().forEach((broker, value) -> {
                val overallBandWidthForBroker = bandwidthData.getOrDefault(broker, new OverallBandwidthBrokerData());
                overallBandWidthForBroker.update(value.getLocalData());
                bandwidthData.put(broker, overallBandWidthForBroker);
        });
    }

    /**
     * Override to write additional data for billing
     */
    @Override
    public void writeBundleDataOnZooKeeper() {
        super.writeBundleDataOnZooKeeper();
        updateBillingData();
        billingData.getBandwidthData().forEach((broker, data) -> {
            try {
                final String zooKeeperPath = BILLING_DATA_BROKER_ZPATH + "/" + broker;
                createZPathIfNotExists(zkClient, zooKeeperPath);
                zkClient.setData(zooKeeperPath, data.getJsonBytes(), -1);
                if (log.isDebugEnabled()) {
                    log.debug("Writing billing data report {}", data);
                }
            } catch (Exception e) {
                log.warn("Error when writing billing data for {} to ZooKeeper: {}", broker, e);
            }
        });

    }
}
