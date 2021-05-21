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
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;

/**
 * Data class containing three components comprising all the data available for the leader broker about other brokers: -
 * The local broker data which is written to ZooKeeper by each individual broker (LocalBrokerData). - The time average
 * bundle data which is written to ZooKeeper by the leader broker (TimeAverageBrokerData). - The preallocated bundles
 * which are not written to ZooKeeper but are maintained by the leader broker (Map<String, BundleData>).
 */
@Data
public class BrokerData {
    private LocalBrokerData localData;
    private TimeAverageBrokerData timeAverageData;
    private Map<String, BundleData> preallocatedBundleData;

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
}
