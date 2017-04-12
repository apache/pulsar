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
package com.yahoo.pulsar.broker.loadbalance.impl;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.LocalBrokerData;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.loadbalance.DataUpdateCondition;

/**
 * HighDeltaCondition tells the load manager that we should update the local data when the differences in the message
 * rates, system resource usage, or bundle count is sufficiently large.
 */
public class HighDeltaCondition implements DataUpdateCondition {
    private final static Logger log = LoggerFactory.getLogger(HighDeltaCondition.class);

    private double percentChange(final double oldValue, final double newValue) {
        if (oldValue == 0) {
            if (newValue == 0) {
                // Avoid NaN
                return 0;
            }
            return Double.POSITIVE_INFINITY;
        }
        return 100 * Math.abs((oldValue - newValue) / oldValue);
    }

    /**
     * Decide whether to update based on if the difference in any metric exceeds some percentage.
     * 
     * @param oldData
     *            Data available before the most recent local update.
     * @param newData
     *            Most recently available data.
     * @param conf
     *            Configuration to use to determine whether the new data should be written.
     * @return true if an update should occur, false otherwise.
     */
    @Override
    public boolean shouldUpdate(final LocalBrokerData oldData, final LocalBrokerData newData,
            final ServiceConfiguration conf) {
        final long updateMaxIntervalMillis = TimeUnit.MINUTES
                .toMillis(conf.getLoadBalancerReportUpdateMaxIntervalMinutes());
        if (System.currentTimeMillis() - newData.getLastUpdate() > updateMaxIntervalMillis) {
            log.info("Writing local data to ZooKeeper because time since last update exceeded threshold of {} minutes",
                    conf.getLoadBalancerReportUpdateMaxIntervalMinutes());
            // Always update after surpassing the maximum interval.
            return true;
        }
        final double maxChange = Math.max(percentChange(oldData.getMaxResourceUsage(), newData.getMaxResourceUsage()),
                Math.max(
                        percentChange(oldData.getMsgRateIn() + oldData.getMsgRateOut(),
                                newData.getMsgRateIn() + newData.getMsgRateOut()),
                        Math.max(
                                percentChange(oldData.getMsgThroughputIn() + oldData.getMsgThroughputOut(),
                                        newData.getMsgThroughputIn() + newData.getMsgThroughputOut()),
                                percentChange(oldData.getNumBundles(), newData.getNumBundles()))));
        if (maxChange > conf.getLoadBalancerReportUpdateThresholdPercentage()) {
            log.info("Writing local data to ZooKeeper because maximum change {}% exceeded threshold {}%", maxChange,
                    conf.getLoadBalancerReportUpdateThresholdPercentage());
            return true;
        }
        return false;
    }
}
