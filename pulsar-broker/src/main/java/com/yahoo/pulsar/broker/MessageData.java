/*
 * Copyright 2016 Yahoo Inc.
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
 *
 */

package com.yahoo.pulsar.broker;

import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;

/**
 * Class containing message rate and throughput fields.
 */
public class MessageData extends JSONWritable {
    private double msgRateIn;
    private double msgRateOut;
    private double msgThroughputIn;
    private double msgThroughputOut;

    /**
     * Construct a MessageData object.
     */
    public MessageData() {
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

    /**
     * Add the message data of the given bundle stats
     * 
     * @param stats
     *            Bundle stats to add from.
     */
    public void add(final NamespaceBundleStats stats) {
        msgRateIn += stats.msgRateIn;
        msgRateOut += stats.msgRateOut;
        msgThroughputIn += stats.msgThroughputIn;
        msgThroughputOut += stats.msgThroughputOut;
    }

    /**
     * Combine the message data of that and this into this.
     * 
     * @param that
     *            Message data to combine with.
     */
    public void add(final MessageData that) {
        msgRateIn += that.msgRateIn;
        msgRateOut += that.msgRateOut;
        msgThroughputIn += that.msgThroughputIn;
        msgThroughputOut += that.msgThroughputOut;
    }

    /**
     * Sets all rates and throughputs to 0.
     */
    public void reset() {
        msgRateIn = msgRateOut = msgThroughputIn = msgThroughputOut = 0;
    }

    /**
     * Remove the message data of that from this.
     * 
     * @param that
     *            Message data to remove.
     */
    public void subtract(final MessageData that) {
        msgRateIn -= that.msgRateIn;
        msgRateOut -= that.msgRateOut;
        msgThroughputIn -= that.msgThroughputIn;
        msgThroughputOut -= that.msgThroughputOut;
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

}
