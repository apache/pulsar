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
package com.yahoo.pulsar.broker.loadbalance;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.TimeAverageBundleData;

/**
 * This class represents all data that could be relevant when making a load management decision.
 */
public class LoadData {
    /**
     * Map from broker names to their available data.
     */
    private final Map<String, BrokerData> brokerData;

    /**
     * Map from bundle names to their time-sensitive aggregated data.
     */
    private final Map<String, TimeAverageBundleData> bundleData;

    /**
     * Initialize a LoadData.
     */
    public LoadData() {
        this.brokerData = new ConcurrentHashMap<>();
        this.bundleData = new ConcurrentHashMap<>();
    }

    public Map<String, BrokerData> getBrokerData() {
        return brokerData;
    }

    public Map<String, TimeAverageBundleData> getBundleData() {
        return bundleData;
    }
}
