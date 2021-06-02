/**
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
package org.apache.pulsar.broker;

import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;

/**
 * Data class aggregating the short term and long term data across all bundles belonging to a broker.
 */
@Data
@NoArgsConstructor
public class TimeAverageBrokerData {
    private double shortTermMsgThroughputIn;
    private double shortTermMsgThroughputOut;
    private double shortTermMsgRateIn;
    private double shortTermMsgRateOut;
    private double longTermMsgThroughputIn;
    private double longTermMsgThroughputOut;
    private double longTermMsgRateIn;
    private double longTermMsgRateOut;

    /**
     * Initialize a TimeAverageBrokerData.
     *
     * @param bundles
     *            The bundles belonging to the broker.
     * @param data
     *            Map from bundle names to the data for that bundle.
     * @param defaultStats
     *            The stats to use when a bundle belonging to this broker is not found in the bundle data map.
     */
    public TimeAverageBrokerData(final Set<String> bundles, final Map<String, BundleData> data,
            final NamespaceBundleStats defaultStats) {
        reset(bundles, data, defaultStats);
    }

    /**
     * Reuse this TimeAverageBrokerData using new data.
     *
     * @param bundles
     *            The bundles belonging to the broker.
     * @param data
     *            Map from bundle names to the data for that bundle.
     * @param defaultStats
     *            The stats to use when a bundle belonging to this broker is not found in the bundle data map.
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

        for (String bundle : bundles) {
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
}
