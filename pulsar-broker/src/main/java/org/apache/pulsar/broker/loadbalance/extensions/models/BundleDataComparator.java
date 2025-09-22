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
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;

/**
 * A strict comparator for BundleData that provides deterministic ordering
 * without threshold-based comparisons to avoid transitivity violations.
 */
public class BundleDataComparator implements Comparator<BundleData> {

    /**
     * Compares two bundle entries using BundleData
     * 1. Short-term data comparison (inbound bandwidth, outbound bandwidth, message rate)
     * 2. Long-term data comparison (same hierarchy as short-term)
     *
     * @param bundleA first bundle entry
     * @param bundleB second bundle entry
     * @return negative if a < b, positive if a > b, zero if equal
     */
    @Override
    public int compare(BundleData bundleA, BundleData bundleB) {

        // First compare short-term data (same hierarchy as TimeAverageMessageData.compareTo() but strict)
        int result = compareShortTermData(bundleA, bundleB);
        if (result != 0) {
            return result;
        }

        // If short-term data is equal, compare long-term data
        result = compareLongTermData(bundleA, bundleB);
        if (result != 0) {
            return result;
        }

        // If all metrics are equal
        return 0;
    }

    /**
     * Compare short-term data using the same hierarchy as TimeAverageMessageData.compareTo() but with strict comparisons.
     */
    private int compareShortTermData(BundleData bundleA, BundleData bundleB) {
        TimeAverageMessageData shortTermA = bundleA.getShortTermData();
        TimeAverageMessageData shortTermB = bundleB.getShortTermData();

        // 1. Inbound bandwidth (strict comparison)
        int result = Double.compare(shortTermA.getMsgThroughputIn(), shortTermB.getMsgThroughputIn());
        if (result != 0) {
            return result;
        }

        // 2. Outbound bandwidth (strict comparison)
        result = Double.compare(shortTermA.getMsgThroughputOut(), shortTermB.getMsgThroughputOut());
        if (result != 0) {
            return result;
        }

        // 3. Total message rate (strict comparison)
        double totalMsgRateA = shortTermA.getMsgRateIn() + shortTermA.getMsgRateOut();
        double totalMsgRateB = shortTermB.getMsgRateIn() + shortTermB.getMsgRateOut();
        return Double.compare(totalMsgRateA, totalMsgRateB);
    }

    /**
     * Compare long-term data using the same hierarchy as TimeAverageMessageData.compareTo() but with strict comparisons.
     */
    private int compareLongTermData(BundleData bundleA, BundleData bundleB) {
        TimeAverageMessageData longTermA = bundleA.getLongTermData();
        TimeAverageMessageData longTermB = bundleB.getLongTermData();

        // 1. Inbound bandwidth (strict comparison)
        int result = Double.compare(longTermA.getMsgThroughputIn(), longTermB.getMsgThroughputIn());
        if (result != 0) {
            return result;
        }

        // 2. Outbound bandwidth (strict comparison)
        result = Double.compare(longTermA.getMsgThroughputOut(), longTermB.getMsgThroughputOut());
        if (result != 0) {
            return result;
        }

        // 3. Total message rate (strict comparison)
        double totalMsgRateA = longTermA.getMsgRateIn() + longTermA.getMsgRateOut();
        double totalMsgRateB = longTermB.getMsgRateIn() + longTermB.getMsgRateOut();
        return Double.compare(totalMsgRateA, totalMsgRateB);
    }
}
