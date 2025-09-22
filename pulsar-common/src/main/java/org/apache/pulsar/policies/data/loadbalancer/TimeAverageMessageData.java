/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.policies.data.loadbalancer;

import static org.apache.pulsar.common.util.CompareUtil.compareDoubleWithResolution;
import lombok.EqualsAndHashCode;

/**
 * Data class comprising the average message data over a fixed period of time.
 */
@EqualsAndHashCode
public class TimeAverageMessageData implements Comparable<TimeAverageMessageData> {
    // The maximum number of samples this data will consider.
    private int maxSamples;

    // The number of samples that are currently available for this data. Always
    // at most maxSamples.
    private int numSamples;

    // The average throughput-in in bytes per second.
    private double msgThroughputIn;

    // The average throughput-out in bytes per second.
    private double msgThroughputOut;

    // The average message rate in per second.
    private double msgRateIn;

    // The average message rate out per second.
    private double msgRateOut;

    // When comparing throughput, uses a resolution of 100 KB/s, effectively rounding values before comparison
    private static final double throughputComparisonResolution = 1e5;
    // When comparing message rate, uses a resolution of 100, effectively rounding values before comparison
    private static final double msgRateComparisonResolution = 100;

    // For JSON only.
    public TimeAverageMessageData() {
    }

    /**
     * Initialize this TimeAverageData to 0 values.
     *
     * @param maxSamples
     *            The maximum number of samples with which to maintain the average.
     */
    public TimeAverageMessageData(final int maxSamples) {
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
        this.maxSamples = maxSamples;
        msgThroughputIn = defaultStats.msgThroughputIn;
        msgThroughputOut = defaultStats.msgThroughputOut;
        msgRateIn = defaultStats.msgRateIn;
        msgRateOut = defaultStats.msgRateOut;
    }

    /**
     * Update using new samples for the message data.
     *
     * @param newMsgThroughputIn
     *            Most recently observed throughput in.
     * @param newMsgThroughputOut
     *            Most recently observed throughput out.
     * @param newMsgRateIn
     *            Most recently observed message rate in.
     * @param newMsgRateOut
     *            Most recently observed message rate out.
     */
    public void update(final double newMsgThroughputIn, final double newMsgThroughputOut, final double newMsgRateIn,
                       final double newMsgRateOut) {
        // If max samples has been reached, don't increase numSamples.
        numSamples = Math.min(numSamples + 1, maxSamples);
        msgThroughputIn = getUpdatedValue(msgThroughputIn, newMsgThroughputIn);
        msgThroughputOut = getUpdatedValue(msgThroughputOut, newMsgThroughputOut);
        msgRateIn = getUpdatedValue(msgRateIn, newMsgRateIn);
        msgRateOut = getUpdatedValue(msgRateOut, newMsgRateOut);
    }

    /**
     * Update using a new bundle sample.
     *
     * @param newSample
     *            Most recently observed bundle stats.
     */
    public void update(final NamespaceBundleStats newSample) {
        update(newSample.msgThroughputIn, newSample.msgThroughputOut, newSample.msgRateIn, newSample.msgRateOut);
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

    public double getMsgThroughputIn() {
        return msgThroughputIn;
    }

    public void setMsgThroughputIn(double msgThroughputIn) {
        this.msgThroughputIn = msgThroughputIn;
    }

    public double getMsgThroughputOut() {
        return msgThroughputOut;
    }

    public void setMsgThroughputOut(double msgThroughputOut) {
        this.msgThroughputOut = msgThroughputOut;
    }

    public double getMsgRateIn() {
        return msgRateIn;
    }

    public void setMsgRateIn(double msgRateIn) {
        this.msgRateIn = msgRateIn;
    }

    public double getMsgRateOut() {
        return msgRateOut;
    }

    public void setMsgRateOut(double msgRateOut) {
        this.msgRateOut = msgRateOut;
    }

    /**
     * Get the total message rate.
     *
     * @return Message rate in + message rate out.
     */
    public double totalMsgRate() {
        return msgRateIn + msgRateOut;
    }

    /**
     * Get the total message throughput.
     *
     * @return Message throughput in + message throughput out.
     */
    public double totalMsgThroughput() {
        return msgThroughputIn + msgThroughputOut;
    }

    @Override
    public int compareTo(TimeAverageMessageData other) {
        int result = this.compareByBandwidthIn(other);

        if (result == 0) {
            result = this.compareByBandwidthOut(other);
        }
        if (result == 0) {
            result = this.compareByMsgRate(other);
        }
        return result;
    }

    public int compareByMsgRate(TimeAverageMessageData other) {
        double thisMsgRate = this.msgRateIn + this.msgRateOut;
        double otherMsgRate = other.msgRateIn + other.msgRateOut;
        return compareDoubleWithResolution(thisMsgRate, otherMsgRate, msgRateComparisonResolution);
    }

    public int compareByBandwidthIn(TimeAverageMessageData other) {
        return compareDoubleWithResolution(msgThroughputIn, other.msgThroughputIn, throughputComparisonResolution);
    }

    public int compareByBandwidthOut(TimeAverageMessageData other) {
        return compareDoubleWithResolution(msgThroughputOut, other.msgThroughputOut, throughputComparisonResolution);
    }
}
