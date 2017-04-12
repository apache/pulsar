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

import com.yahoo.pulsar.broker.LocalBrokerData;
import com.yahoo.pulsar.broker.ServiceConfiguration;

/**
 * DataUpdateCondition instances are injecting into ModularLoadManagers to determine when local broker data should be
 * written to ZooKeeper or not.
 */
public interface DataUpdateCondition {
    /**
     * Determine if the LocalBrokerData should be written to ZooKeeper by comparing the old data to the new data.
     * 
     * @param oldData
     *            Data available before the most recent local update.
     * @param newData
     *            Most recently available data.
     * @param conf
     *            Configuration to use to determine whether the new data should be written.
     * @return true if the new data should be written to ZooKeeper, false otherwise.
     */
    boolean shouldUpdate(LocalBrokerData oldData, LocalBrokerData newData, ServiceConfiguration conf);
}
