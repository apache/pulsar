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

import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 */
@EqualsAndHashCode
@ToString
public class NamespaceBundleStats implements Comparable<NamespaceBundleStats>, Serializable {

    public double msgRateIn;
    public double msgThroughputIn;
    public double msgRateOut;
    public double msgThroughputOut;
    public int consumerCount;
    public int producerCount;
    public long topics;
    public long cacheSize;

    // When comparing throughput, uses a resolution of 100 KB/s, effectively rounding values before comparison
    private static final double throughputComparisonResolution = 1e5;
    // When comparing message rate, uses a resolution of 100, effectively rounding values before comparison
    private static final double msgRateComparisonResolution = 100;
    // When comparing total topics/producers/consumers, uses a resolution/rounding of 500
    private static final long topicConnectionComparisonResolution = 500;
    // When comparing cache size, uses a resolution/rounding of 100kB
    private static final long cacheSizeComparisonResolution = 100000;

    public NamespaceBundleStats() {
        reset();
    }

    public void reset() {
        this.msgRateIn = 0;
        this.msgThroughputIn = 0;
        this.msgRateOut = 0;
        this.msgThroughputOut = 0;
        this.consumerCount = 0;
        this.producerCount = 0;
        this.topics = 0;
        this.cacheSize = 0;
    }

    // compare 2 bundles in below aspects:
    // 1. Inbound bandwidth
    // 2. Outbound bandwidth
    // 3. Total megRate (both in and out)
    // 4. Total topics and producers/consumers
    // 5. Total cache size
    public int compareTo(NamespaceBundleStats other) {
        int result = this.compareByBandwidthIn(other);

        if (result == 0) {
            result = this.compareByBandwidthOut(other);
        }
        if (result == 0) {
            result = this.compareByMsgRate(other);
        }
        if (result == 0) {
            result = this.compareByTopicConnections(other);
        }
        if (result == 0) {
            result = this.compareByCacheSize(other);
        }

        return result;
    }

    public int compareByMsgRate(NamespaceBundleStats other) {
        double thisMsgRate = this.msgRateIn + this.msgRateOut;
        double otherMsgRate = other.msgRateIn + other.msgRateOut;
        return compareDoubleWithResolution(thisMsgRate, otherMsgRate, msgRateComparisonResolution);
    }

    private static int compareDoubleWithResolution(double v1, double v2, double resolution) {
        return Long.compare(Math.round(v1 / resolution), Math.round(v2 / resolution));
    }

    private static int compareLongWithResolution(long v1, long v2, long resolution) {
        return Long.compare(v1 / resolution, v2 / resolution);
    }

    public int compareByTopicConnections(NamespaceBundleStats other) {
        long thisTopicsAndConnections = this.topics + this.consumerCount + this.producerCount;
        long otherTopicsAndConnections = other.topics + other.consumerCount + other.producerCount;
        return compareLongWithResolution(thisTopicsAndConnections, otherTopicsAndConnections,
                topicConnectionComparisonResolution);
    }

    public int compareByCacheSize(NamespaceBundleStats other) {
        return compareLongWithResolution(cacheSize, other.cacheSize, cacheSizeComparisonResolution);
    }

    public int compareByBandwidthIn(NamespaceBundleStats other) {
        return compareDoubleWithResolution(msgThroughputIn, other.msgThroughputIn, throughputComparisonResolution);
    }

    public int compareByBandwidthOut(NamespaceBundleStats other) {
        return compareDoubleWithResolution(msgThroughputOut, other.msgThroughputOut, throughputComparisonResolution);
    }
}
