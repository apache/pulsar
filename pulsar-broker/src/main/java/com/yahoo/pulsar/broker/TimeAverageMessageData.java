/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker;

import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;

/**
 * Data class comprising the average message data over a fixed period of time.
 */
public class TimeAverageMessageData {
    // The maximum number of samples this data will consider.
    private int maxSamples;

    // The number of samples that are currently available for this data. Always
    // at most maxSamples.
    private int numSamples;

    // Message data that is being time-averaged.
    private MessageData messageData;

    // For JSON only.
    public TimeAverageMessageData() {
        this.messageData = new MessageData();
    }

    /**
     * Initialize this TimeAverageData to 0 values.
     * 
     * @param maxSamples
     *            The maximum number of samples with which to maintain the average.
     */
    public TimeAverageMessageData(final int maxSamples) {
        this();
        this.maxSamples = maxSamples;
    }

    /**
     * Initialize this TimeAverageData using default stats.
     * 
     * @param maxSamples
     *            The maximum number of samples with which to maintain the average.
     * @param defaultStats
     *            The stats to default to. These are overwritten after the first update.
     */
    public TimeAverageMessageData(final int maxSamples, final NamespaceBundleStats defaultStats) {
        this();
        this.maxSamples = maxSamples;
        messageData.add(defaultStats);
    }

    /**
     * Update using a new bundle sample.
     * 
     * @param newSample
     *            Most recently observed bundle stats.
     */
    public void update(final NamespaceBundleStats newSample) {
        // Limit number of samples to maxSamples
        numSamples = Math.min(maxSamples, numSamples + 1);
        messageData.setMsgRateIn(getUpdatedValue(messageData.getMsgRateIn(), newSample.msgRateIn));
        messageData.setMsgRateOut(getUpdatedValue(messageData.getMsgRateOut(), newSample.msgRateOut));
        messageData.setMsgThroughputIn(getUpdatedValue(messageData.getMsgThroughputIn(), newSample.msgThroughputIn));
        messageData.setMsgThroughputOut(getUpdatedValue(messageData.getMsgThroughputOut(), newSample.msgThroughputOut));
    }

    // Update the average of a sample using the number of samples, the previous
    // average, and a new sample.
    private double getUpdatedValue(final double oldAverage, final double newSample) {
        // Note that for numSamples == 1, this returns newSample.
        // This ensures that default stats get overwritten after the first
        // update.
        return ((numSamples - 1) * oldAverage + newSample) / numSamples;
    }

    public int getMaxSamples() {
        return maxSamples;
    }

    public void setMaxSamples(int maxSamples) {
        this.maxSamples = maxSamples;
    }

    public int getNumSamples() {
        return numSamples;
    }

    public void setNumSamples(int numSamples) {
        this.numSamples = numSamples;
    }

    public MessageData getMessageData() {
        return messageData;
    }

    public void setMessageData(MessageData messageData) {
        this.messageData = messageData;
    }
}
