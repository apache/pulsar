package com.yahoo.pulsar.broker;

import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;

/**
 * Data class comprising the short term and long term historical data for this bundle.
 */
public class BundleData extends JSONWritable {
    // Short term data for this bundle. The time frame of this data is determined by the number of short term samples
    // and the bundle update period.
    private TimeAverageMessageData shortTermData;

    // Long term data for this bundle. The time frame of this data is determined by the number of long term samples
    // and the bundle update period.
    private TimeAverageMessageData longTermData;

    // For JSON only.
    public BundleData(){}

    /**
     * Initialize the bundle data.
     * @param numShortSamples Number of short term samples to use.
     * @param numLongSamples Number of long term samples to use.
     */
    public BundleData(final int numShortSamples, final int numLongSamples) {
        shortTermData = new TimeAverageMessageData(numShortSamples);
        longTermData = new TimeAverageMessageData(numLongSamples);
    }

    /**
     * Initialize this bundle data and have its histories default to the given stats before the first sample is
     * received.
     * @param numShortSamples Number of short term samples to use.
     * @param numLongSamples Number of long term samples to use.
     * @param defaultStats The stats to default to before the first sample is received.
     */
    public BundleData(final int numShortSamples, final int numLongSamples, final NamespaceBundleStats defaultStats) {
        shortTermData = new TimeAverageMessageData(numShortSamples, defaultStats);
        longTermData = new TimeAverageMessageData(numLongSamples, defaultStats);
    }

    /**
     * Update the historical data for this bundle.
     * @param newSample The bundle stats to update this data with.
     */
    public void update(final NamespaceBundleStats newSample) {
        shortTermData.update(newSample);
        longTermData.update(newSample);
    }

    public TimeAverageMessageData getShortTermData() {
        return shortTermData;
    }

    public void setShortTermData(TimeAverageMessageData shortTermData) {
        this.shortTermData = shortTermData;
    }

    public TimeAverageMessageData getLongTermData() {
        return longTermData;
    }

    public void setLongTermData(TimeAverageMessageData longTermData) {
        this.longTermData = longTermData;
    }
}
