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

import java.util.Map;
import java.util.Set;

import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;

/**
 * Data class aggregating the short term and long term data across all bundles belonging to a broker.
 */
public class TimeAverageBrokerData extends JSONWritable {
    private MessageData shortTermData;
    private MessageData longTermData;

    public TimeAverageBrokerData() {
        shortTermData = new MessageData();
        longTermData = new MessageData();
    }

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
    public TimeAverageBrokerData(final Set<String> bundles, final Map<String, TimeAverageBundleData> data,
            final NamespaceBundleStats defaultStats) {
        this();
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
    public void reset(final Set<String> bundles, final Map<String, TimeAverageBundleData> data,
            final NamespaceBundleStats defaultStats) {
        shortTermData.reset();
        longTermData.reset();

        for (String bundle : bundles) {
            final TimeAverageBundleData bundleData = data.get(bundle);
            if (bundleData == null) {
                shortTermData.add(defaultStats);
                longTermData.add(defaultStats);
            } else {
                final MessageData bundleShortTermData = bundleData.getShortTermData().getMessageData();
                final MessageData bundleLongTermData = bundleData.getLongTermData().getMessageData();
                shortTermData.add(bundleShortTermData);
                longTermData.add(bundleLongTermData);
            }
        }
    }

    public MessageData getShortTermData() {
        return shortTermData;
    }

    public void setShortTermData(MessageData shortTermData) {
        this.shortTermData = shortTermData;
    }

    public MessageData getLongTermData() {
        return longTermData;
    }

    public void setLongTermData(MessageData longTermData) {
        this.longTermData = longTermData;
    }
}
