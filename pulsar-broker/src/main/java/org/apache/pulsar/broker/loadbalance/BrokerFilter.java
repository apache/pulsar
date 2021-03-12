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
package org.apache.pulsar.broker.loadbalance;

import java.util.Set;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.ServiceConfiguration;

/**
 * Load management component which determines what brokers should not be considered for topic placement by the placement
 * strategy. For example, the placement strategy may determine that the broker with the least msg/s should get the
 * bundle assignment, but we may not want to consider brokers whose CPU usage is very high. Thus, we could use a filter
 * to blacklist brokers with high CPU usage.
 */
public interface BrokerFilter {

    /**
     * From the given set of available broker candidates, filter those using the load data.
     *
     * @param brokers
     *            The currently available brokers that have not already been filtered. This set may be modified by
     *            filter.
     * @param bundleToAssign
     *            The data for the bundle to assign.
     * @param loadData
     *            The load data from the leader broker.
     * @param conf
     *            The service configuration.
     * @throws BrokerFilterException
     *            There was an error in the pipeline and the brokers should be reset to their original value
     */
    void filter(Set<String> brokers, BundleData bundleToAssign, LoadData loadData, ServiceConfiguration conf)
            throws BrokerFilterException;
}
