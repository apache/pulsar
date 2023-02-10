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
package org.apache.pulsar.broker.loadbalance.extensions.reporter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.impl.GenericBrokerHostUsageImpl;
import org.apache.pulsar.broker.loadbalance.impl.LinuxBrokerHostUsageImpl;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;

/**
 * The broker load data reporter.
 */
@Slf4j
public class BrokerLoadDataReporter implements LoadDataReporter<BrokerLoadData> {

    private final PulsarService pulsar;

    private final ServiceConfiguration conf;

    private final LoadDataStore<BrokerLoadData> brokerLoadDataStore;

    private final BrokerHostUsage brokerHostUsage;

    private final String lookupServiceAddress;

    @Getter
    private final BrokerLoadData localData;

    private final BrokerLoadData lastData;

    public BrokerLoadDataReporter(PulsarService pulsar,
                                  String lookupServiceAddress,
                                  LoadDataStore<BrokerLoadData> brokerLoadDataStore) {
        this.brokerLoadDataStore = brokerLoadDataStore;
        this.lookupServiceAddress = lookupServiceAddress;
        this.pulsar = pulsar;
        this.conf = this.pulsar.getConfiguration();
        if (SystemUtils.IS_OS_LINUX) {
            brokerHostUsage = new LinuxBrokerHostUsageImpl(pulsar);
        } else {
            brokerHostUsage = new GenericBrokerHostUsageImpl(pulsar);
        }
        this.localData = new BrokerLoadData();
        this.lastData = new BrokerLoadData();

    }

    @Override
    public BrokerLoadData generateLoadData() {
        final SystemResourceUsage systemResourceUsage = LoadManagerShared.getSystemResourceUsage(brokerHostUsage);
        final var pulsarStats = pulsar.getBrokerService().getPulsarStats();
        synchronized (pulsarStats) {
            var brokerStats = pulsarStats.getBrokerStats();
            localData.update(systemResourceUsage,
                    brokerStats.msgThroughputIn,
                    brokerStats.msgThroughputOut,
                    brokerStats.msgRateIn,
                    brokerStats.msgRateOut,
                    brokerStats.bundleCount,
                    pulsar.getConfiguration());

        }
        return this.localData;
    }

    @Override
    public CompletableFuture<Void> reportAsync(boolean force) {
        BrokerLoadData newLoadData = this.generateLoadData();
        if (force || needBrokerDataUpdate()) {
            log.info("publishing load report:{}", localData.toString(conf));
            CompletableFuture<Void> future =
                    this.brokerLoadDataStore.pushAsync(this.lookupServiceAddress, newLoadData);
            future.whenComplete((__, ex) -> {
                if (ex == null) {
                    localData.setReportedAt(System.currentTimeMillis());
                    lastData.update(localData);
                } else {
                    log.error("Failed to report the broker load data.", ex);
                }
            });
            return future;
        } else {
            log.info("skipping load report:{}", localData.toString(conf));
        }
        return CompletableFuture.completedFuture(null);
    }

    private boolean needBrokerDataUpdate() {
        int loadBalancerReportUpdateMaxIntervalMinutes = conf.getLoadBalancerReportUpdateMaxIntervalMinutes();
        int loadBalancerReportUpdateThresholdPercentage = conf.getLoadBalancerReportUpdateThresholdPercentage();
        final long updateMaxIntervalMillis = TimeUnit.MINUTES
                .toMillis(loadBalancerReportUpdateMaxIntervalMinutes);
        long timeSinceLastReportWrittenToStore = System.currentTimeMillis() - localData.getReportedAt();
        if (timeSinceLastReportWrittenToStore > updateMaxIntervalMillis) {
            log.info("Writing local data to metadata store because time since last"
                            + " update exceeded threshold of {} minutes",
                    loadBalancerReportUpdateMaxIntervalMinutes);
            // Always update after surpassing the maximum interval.
            return true;
        }
        final double maxChange = Math
                .max(100.0 * (Math.abs(lastData.getMaxResourceUsage() - localData.getMaxResourceUsage())),
                        Math.max(percentChange(lastData.getMsgRateIn() + lastData.getMsgRateOut(),
                                        localData.getMsgRateIn() + localData.getMsgRateOut()),
                            Math.max(
                                percentChange(lastData.getMsgThroughputIn() + lastData.getMsgThroughputOut(),
                                        localData.getMsgThroughputIn() + localData.getMsgThroughputOut()),
                                percentChange(lastData.getBundleCount(), localData.getBundleCount()))));
        if (maxChange > loadBalancerReportUpdateThresholdPercentage) {
            log.info("Writing local data to metadata store because maximum change {}% exceeded threshold {}%; "
                            + "time since last report written is {} seconds", maxChange,
                    loadBalancerReportUpdateThresholdPercentage,
                    timeSinceLastReportWrittenToStore / 1000.0);
            return true;
        }
        return false;
    }

    protected double percentChange(final double oldValue, final double newValue) {
        if (oldValue == 0) {
            if (newValue == 0) {
                // Avoid NaN
                return 0;
            }
            return Double.POSITIVE_INFINITY;
        }
        return 100 * Math.abs((oldValue - newValue) / oldValue);
    }
}
