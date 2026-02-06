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
package org.apache.pulsar.broker.loadbalance.impl;

import java.util.Objects;
import java.util.Set;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;

public class BrokerLoadManagerClassFilter implements BrokerFilter {

    @Override
    public void filter(Set<String> brokers, BundleData bundleToAssign,
                       LoadData loadData,
                       ServiceConfiguration conf) throws BrokerFilterException {
        loadData.getBrokerData().forEach((key, value) -> {
            // The load manager class name can be null if the cluster has old version of broker.
            if (!Objects.equals(value.getLocalData().getLoadManagerClassName(), conf.getLoadManagerClassName())) {
                brokers.remove(key);
            }
        });
    }
}
