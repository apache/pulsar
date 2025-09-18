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
package org.apache.pulsar.broker.loadbalance.extensions.models;

import java.util.Comparator;
import java.util.Map;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;

/**
 * A strict comparator for NamespaceBundleStats that provides deterministic ordering
 * without threshold-based comparisons to avoid transitivity violations.
 */
public class StrictNamespaceBundleStatsComparator implements Comparator<NamespaceBundleStats> {

    /**
     * Compares two bundle entries
     * Comparison hierarchy:
     * 1. Inbound bandwidth (msgThroughputIn)
     * 2. Outbound bandwidth (msgThroughputOut)
     * 3. Total message rate (msgRateIn + msgRateOut)
     * 4. Total topics and connections (topics + consumerCount + producerCount)
     * 5. Cache size (cacheSize)
     *
     * @param bundleA first bundle entry
     * @param bundleB second bundle entry
     * @return negative if a < b, positive if a > b, zero if equal
     */
    @Override
    public int compare(NamespaceBundleStats bundleA, NamespaceBundleStats bundleB) {
        int result = compareByBandwidthIn(bundleA, bundleB);
        if (result != 0) {
            return result;
        }

        result = compareByBandwidthOut(bundleA, bundleB);
        if (result != 0) {
            return result;
        }

        result = compareByMsgRate(bundleA, bundleB);
        if (result != 0) {
            return result;
        }

        result = compareByTopicConnections(bundleA, bundleB);
        if (result != 0) {
            return result;
        }

        result = compareByCacheSize(bundleA, bundleB);
        if (result != 0) {
            return result;
        }

        // If all metrics are equal
        return 0;
    }

    private int compareByBandwidthIn(NamespaceBundleStats bundleA, NamespaceBundleStats bundleB) {
        return Double.compare(bundleA.msgThroughputIn, bundleB.msgThroughputIn);
    }

    private int compareByBandwidthOut(NamespaceBundleStats bundleA, NamespaceBundleStats bundleB) {
        return Double.compare(bundleA.msgThroughputOut, bundleB.msgThroughputOut);
    }

    private int compareByMsgRate(NamespaceBundleStats bundleA, NamespaceBundleStats bundleB) {
        double totalMsgRateA = bundleA.msgRateIn + bundleA.msgRateOut;
        double totalMsgRateB = bundleB.msgRateIn + bundleB.msgRateOut;
        return Double.compare(totalMsgRateA, totalMsgRateB);
    }

    private int compareByTopicConnections(NamespaceBundleStats bundleA, NamespaceBundleStats bundleB) {
        long totalConnectionsA = bundleA.topics + bundleA.consumerCount + bundleA.producerCount;
        long totalConnectionsB = bundleB.topics + bundleB.consumerCount + bundleB.producerCount;
        return Long.compare(totalConnectionsA, totalConnectionsB);
    }

    private int compareByCacheSize(NamespaceBundleStats bundleA, NamespaceBundleStats bundleB) {
        return Long.compare(bundleA.cacheSize, bundleB.cacheSize);
    }
}
