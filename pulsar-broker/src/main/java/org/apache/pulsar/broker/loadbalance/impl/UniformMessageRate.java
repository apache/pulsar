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

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class UniformMessageRate implements ModularLoadManagerStrategy {
    @Override
    public Optional<String> selectBroker(Set<String> candidates, BundleData bundleToAssign, LoadData loadData, ServiceConfiguration conf) {
        Map<String, BrokerData> brokersData = loadData.getBrokerData();
        MutableObject<String> underloadedBroker = new MutableObject<>();
        MutableDouble minMsgRate = new MutableDouble(Integer.MAX_VALUE);
        MutableDouble minThroughputRate = new MutableDouble(Integer.MAX_VALUE);
        brokersData.forEach((broker, data) -> {
            TimeAverageBrokerData timeAverageData = data.getTimeAverageData();
            double msgRate = timeAverageData.getShortTermMsgRateIn()
                    + timeAverageData.getShortTermMsgRateOut();
            double throughputRate = timeAverageData.getShortTermMsgThroughputIn()
                    + timeAverageData.getShortTermMsgThroughputOut();

            if (((conf.getLoadBalancerMsgRateDifferenceShedderThreshold() > 0) && (msgRate < minMsgRate.getValue()))
                    || ((conf.getLoadBalancerMsgThroughputMultiplierDifferenceShedderThreshold() > 0)
                    && (throughputRate < minThroughputRate.getValue()))) {
                underloadedBroker.setValue(broker);
                minMsgRate.setValue(msgRate);
                minThroughputRate.setValue(throughputRate);
            }
        });

        return Optional.of(underloadedBroker.getValue());
    }
}
