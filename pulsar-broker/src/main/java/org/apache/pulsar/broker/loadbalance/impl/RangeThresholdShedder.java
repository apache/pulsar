/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.loadbalance.impl;

import com.google.common.collect.Multimap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;

/**
 * On the basis of ThresholdShedder, RangeThresholdShedder adds the lower boundary judgment of the load.
 * When 【current usage < average usage - threshold】, the broker with the highest load will be triggered to unload,
 * avoiding the following scenarios:
 * There are 11 Brokers, of which 10 are loaded at 80% and 1 is loaded at 0%.
 * The average load is 80 * 10 / 11 = 72.73, and the threshold to unload is 72.73 + 10 = 82.73.
 * Since 80 < 82.73, unload will not be trigger, and there is one idle Broker with load of 0%.
 */
@Slf4j
public class RangeThresholdShedder extends ThresholdShedder {

    @Override
    public Multimap<String, String> findBundlesForUnloading(LoadData loadData, ServiceConfiguration conf) {
        super.findBundlesForUnloading(loadData, conf);
        // Return if the bundle to unload has already been selected.
        if (!selectedBundlesCache.isEmpty()) {
            return selectedBundlesCache;
        }
        // Select the broker with the most resource usage.
        final double threshold = conf.getLoadBalancerBrokerThresholdShedderPercentage() / 100.0;
        final double avgUsage = getBrokerAvgUsage(loadData, conf);
        Pair<Boolean, String> result = getMaxUsageBroker(loadData, threshold, avgUsage);
        boolean hasBrokerBelowLowerBound = result.getLeft();
        String maxUsageBroker = result.getRight();
        BrokerData brokerData = loadData.getBrokerData().get(maxUsageBroker);
        if (brokerData == null || brokerData.getLocalData() == null ||
                brokerData.getLocalData().getBundles().size() <= 1) {
            log.info("Load data is null or bundle <=1, broker name is {}, skipping bundle unload.", maxUsageBroker);
            return selectedBundlesCache;
        }
        if (!hasBrokerBelowLowerBound) {
            log.info("No broker is below the lower bound, threshold is {}, "
                            + "avgUsage usage is {}, max usage of Broker {} is {}",
                    threshold, avgUsage, maxUsageBroker,
                    brokerAvgResourceUsage.getOrDefault(maxUsageBroker, 0.0));
            return selectedBundlesCache;
        }
        LocalBrokerData localData = brokerData.getLocalData();
        double minimumThroughputToOffload = getMinimumThroughputToOffload(threshold, localData);
        final double minThroughputThreshold = conf.getLoadBalancerBundleUnloadMinThroughputThreshold() * MB;
        if (minThroughputThreshold > minimumThroughputToOffload) {
            log.info("broker {} in RangeThresholdShedder is planning to shed throughput {} MByte/s less than "
                            + "minimumThroughputThreshold {} MByte/s, skipping bundle unload.",
                    maxUsageBroker, minimumThroughputToOffload / MB, minThroughputThreshold / MB);
            return selectedBundlesCache;
        }
        super.filterAndSelectBundle(loadData, loadData.getRecentlyUnloadedBundles(), maxUsageBroker, localData,
                minimumThroughputToOffload);
        return selectedBundlesCache;
    }

    private Pair<Boolean, String> getMaxUsageBroker(
            LoadData loadData, double threshold, double avgUsage) {
        String maxUsageBrokerName = "";
        double maxUsage = -1;
        boolean hasBrokerBelowLowerBound = false;
        for (Map.Entry<String, BrokerData> entry : loadData.getBrokerData().entrySet()) {
            String broker = entry.getKey();
            double currentUsage = brokerAvgResourceUsage.getOrDefault(broker, 0.0);
            // Select the broker with the most resource usage.
            if (currentUsage > maxUsage) {
                maxUsage = currentUsage;
                maxUsageBrokerName = broker;
            }
            // Whether any brokers with low usage in the cluster.
            if (currentUsage < avgUsage - threshold) {
                hasBrokerBelowLowerBound = true;
            }
        }
        return Pair.of(hasBrokerBelowLowerBound, maxUsageBrokerName);
    }

    private double getMinimumThroughputToOffload(double threshold, LocalBrokerData localData) {
        double brokerCurrentThroughput = localData.getMsgThroughputIn() + localData.getMsgThroughputOut();
        return brokerCurrentThroughput * threshold;
    }

}
