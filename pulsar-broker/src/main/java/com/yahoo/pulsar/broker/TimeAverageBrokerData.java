package com.yahoo.pulsar.broker;

import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;

import java.util.Map;
import java.util.Set;

/**
 * Data class aggregating the short term and long term data across all bundles belonging to a broker.
 */
public class TimeAverageBrokerData extends JSONWritable {
    private double shortTermMsgThroughputIn;
    private double shortTermMsgThroughputOut;
    private double shortTermMsgRateIn;
    private double shortTermMsgRateOut;
    private double longTermMsgThroughputIn;
    private double longTermMsgThroughputOut;
    private double longTermMsgRateIn;
    private double longTermMsgRateOut;

    public TimeAverageBrokerData() {}

    /**
     * Initialize a TimeAverageBrokerData.
     * @param bundles The bundles belonging to the broker.
     * @param data Map from bundle names to the data for that bundle.
     * @param defaultStats The stats to use when a bundle belonging to this broker is not found in the bundle data map.
     */
    public TimeAverageBrokerData(final Set<String> bundles, final Map<String, BundleData> data,
                                 final NamespaceBundleStats defaultStats) {
        reset(bundles, data, defaultStats);
    }

    /**
     * Reuse this TimeAverageBrokerData using new data.
     * @param bundles The bundles belonging to the broker.
     * @param data Map from bundle names to the data for that bundle.
     * @param defaultStats The stats to use when a bundle belonging to this broker is not found in the bundle data map.
     */
    public void reset(final Set<String> bundles, final Map<String, BundleData> data,
                      final NamespaceBundleStats defaultStats) {
        shortTermMsgThroughputIn = 0;
        shortTermMsgThroughputOut = 0;
        shortTermMsgRateIn = 0;
        shortTermMsgRateOut = 0;

        longTermMsgThroughputIn = 0;
        longTermMsgThroughputOut = 0;
        longTermMsgRateIn = 0;
        longTermMsgRateOut = 0;

        for (String bundle: bundles) {
            final BundleData bundleData = data.get(bundle);
            if (bundleData == null) {
                shortTermMsgThroughputIn += defaultStats.msgThroughputIn;
                shortTermMsgThroughputOut += defaultStats.msgThroughputOut;
                shortTermMsgRateIn += defaultStats.msgRateIn;
                shortTermMsgRateOut += defaultStats.msgRateOut;

                longTermMsgThroughputIn += defaultStats.msgThroughputIn;
                longTermMsgThroughputOut += defaultStats.msgThroughputOut;
                longTermMsgRateIn += defaultStats.msgRateIn;
                longTermMsgRateOut += defaultStats.msgRateOut;
            } else {
                final TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                final TimeAverageMessageData longTermData = bundleData.getLongTermData();

                shortTermMsgThroughputIn += shortTermData.getMsgThroughputIn();
                shortTermMsgThroughputOut += shortTermData.getMsgThroughputOut();
                shortTermMsgRateIn += shortTermData.getMsgRateIn();
                shortTermMsgRateOut += shortTermData.getMsgRateOut();

                longTermMsgThroughputIn += longTermData.getMsgThroughputIn();
                longTermMsgThroughputOut += longTermData.getMsgThroughputOut();
                longTermMsgRateIn += longTermData.getMsgRateIn();
                longTermMsgRateOut += longTermData.getMsgRateOut();
            }
        }
    }

    public double getShortTermMsgThroughputIn() {
        return shortTermMsgThroughputIn;
    }

    public void setShortTermMsgThroughputIn(double shortTermMsgThroughputIn) {
        this.shortTermMsgThroughputIn = shortTermMsgThroughputIn;
    }

    public double getShortTermMsgThroughputOut() {
        return shortTermMsgThroughputOut;
    }

    public void setShortTermMsgThroughputOut(double shortTermMsgThroughputOut) {
        this.shortTermMsgThroughputOut = shortTermMsgThroughputOut;
    }

    public double getShortTermMsgRateIn() {
        return shortTermMsgRateIn;
    }

    public void setShortTermMsgRateIn(double shortTermMsgRateIn) {
        this.shortTermMsgRateIn = shortTermMsgRateIn;
    }

    public double getShortTermMsgRateOut() {
        return shortTermMsgRateOut;
    }

    public void setShortTermMsgRateOut(double shortTermMsgRateOut) {
        this.shortTermMsgRateOut = shortTermMsgRateOut;
    }

    public double getLongTermMsgThroughputIn() {
        return longTermMsgThroughputIn;
    }

    public void setLongTermMsgThroughputIn(double longTermMsgThroughputIn) {
        this.longTermMsgThroughputIn = longTermMsgThroughputIn;
    }

    public double getLongTermMsgThroughputOut() {
        return longTermMsgThroughputOut;
    }

    public void setLongTermMsgThroughputOut(double longTermMsgThroughputOut) {
        this.longTermMsgThroughputOut = longTermMsgThroughputOut;
    }

    public double getLongTermMsgRateIn() {
        return longTermMsgRateIn;
    }

    public void setLongTermMsgRateIn(double longTermMsgRateIn) {
        this.longTermMsgRateIn = longTermMsgRateIn;
    }

    public double getLongTermMsgRateOut() {
        return longTermMsgRateOut;
    }

    public void setLongTermMsgRateOut(double longTermMsgRateOut) {
        this.longTermMsgRateOut = longTermMsgRateOut;
    }
}
