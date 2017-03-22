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

	// The average throughput-in in bytes per second.
	private double msgThroughputIn;

	// The average throughput-out in bytes per second.
	private double msgThroughputOut;

	// The average message rate in per second.
	private double msgRateIn;

	// The average message rate out per second.
	private double msgRateOut;

	// For JSON only.
	public TimeAverageMessageData() {
	}

	/**
	 * Initialize this TimeAverageData to 0 values.
	 * 
	 * @param maxSamples
	 *            The maximum number of samples with which to maintain the
	 *            average.
	 */
	public TimeAverageMessageData(final int maxSamples) {
		this.maxSamples = maxSamples;
	}

	/**
	 * Initialize this TimeAverageData using default stats.
	 * 
	 * @param maxSamples
	 *            The maximum number of samples with which to maintain the
	 *            average.
	 * @param defaultStats
	 *            The stats to default to. These are overwritten after the first
	 *            update.
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
}
