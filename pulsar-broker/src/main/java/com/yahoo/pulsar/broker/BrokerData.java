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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Data class containing three components comprising all the data available for the leader broker about other brokers: -
 * The local broker data which is written to ZooKeeper by each individual broker (LocalBrokerData). - The time average
 * bundle data which is written to ZooKeeper by the leader broker (TimeAverageBrokerData). - The preallocated bundles
 * which are not written to ZooKeeper but are maintained by the leader broker (Map<String, TimeAverageBundleData>).
 */
public class BrokerData {
    private LocalBrokerData localData;
    private TimeAverageBrokerData timeAverageData;
    private Map<String, TimeAverageBundleData> preallocatedBundleData;

    /**
     * Initialize this BrokerData using the most recent local data.
     * 
     * @param localData
     *            The data local for the broker.
     */
    public BrokerData(final LocalBrokerData localData) {
        this.localData = localData;
        timeAverageData = new TimeAverageBrokerData();
        preallocatedBundleData = new ConcurrentHashMap<>();
    }

    public LocalBrokerData getLocalData() {
        return localData;
    }

    public void setLocalData(LocalBrokerData localData) {
        this.localData = localData;
    }

    public TimeAverageBrokerData getTimeAverageData() {
        return timeAverageData;
    }

    public void setTimeAverageData(TimeAverageBrokerData timeAverageData) {
        this.timeAverageData = timeAverageData;
    }

    public Map<String, TimeAverageBundleData> getPreallocatedBundleData() {
        return preallocatedBundleData;
    }

    public void setPreallocatedBundleData(Map<String, TimeAverageBundleData> preallocatedBundleData) {
        this.preallocatedBundleData = preallocatedBundleData;
    }
}
