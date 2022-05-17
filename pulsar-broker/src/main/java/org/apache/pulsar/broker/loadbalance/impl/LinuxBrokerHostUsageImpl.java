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
package org.apache.pulsar.broker.loadbalance.impl;

import static org.apache.pulsar.broker.loadbalance.LinuxInfoUtils.NICUsageType;
import static org.apache.pulsar.broker.loadbalance.LinuxInfoUtils.getCpuUsageForCGroup;
import static org.apache.pulsar.broker.loadbalance.LinuxInfoUtils.getCpuUsageForEntireHost;
import static org.apache.pulsar.broker.loadbalance.LinuxInfoUtils.getPhysicalNICs;
import static org.apache.pulsar.broker.loadbalance.LinuxInfoUtils.getTotalCpuLimit;
import static org.apache.pulsar.broker.loadbalance.LinuxInfoUtils.getTotalNicLimit;
import static org.apache.pulsar.broker.loadbalance.LinuxInfoUtils.getTotalNicUsage;
import static org.apache.pulsar.broker.loadbalance.LinuxInfoUtils.isCGroupEnabled;
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BitRateUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.broker.loadbalance.LinuxInfoUtils;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;


/**
 * Class that will return the broker host usage.
 */
@Slf4j
public class LinuxBrokerHostUsageImpl implements BrokerHostUsage {
    private long lastCollection;
    private double lastTotalNicUsageTx;
    private double lastTotalNicUsageRx;
    private double lastCpuUsage;
    private double lastCpuTotalTime;
    private OperatingSystemMXBean systemBean;
    private SystemResourceUsage usage;
    private final Optional<Double> overrideBrokerNicSpeedGbps;
    private final boolean isCGroupsEnabled;

    public LinuxBrokerHostUsageImpl(PulsarService pulsar) {
        this(
            pulsar.getConfiguration().getLoadBalancerHostUsageCheckIntervalMinutes(),
            pulsar.getConfiguration().getLoadBalancerOverrideBrokerNicSpeedGbps(),
            pulsar.getLoadManagerExecutor()
        );
    }

    public LinuxBrokerHostUsageImpl(int hostUsageCheckIntervalMin,
                                    Optional<Double> overrideBrokerNicSpeedGbps,
                                    ScheduledExecutorService executorService) {
        this.systemBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        this.lastCollection = 0L;
        this.usage = new SystemResourceUsage();
        this.overrideBrokerNicSpeedGbps = overrideBrokerNicSpeedGbps;
        this.isCGroupsEnabled = isCGroupEnabled();
        // Call now to initialize values before the constructor returns
        calculateBrokerHostUsage();
        executorService.scheduleWithFixedDelay(catchingAndLoggingThrowables(this::calculateBrokerHostUsage),
                hostUsageCheckIntervalMin,
                hostUsageCheckIntervalMin, TimeUnit.MINUTES);
    }

    @Override
    public SystemResourceUsage getBrokerHostUsage() {
        return usage;
    }

    @Override
    public void calculateBrokerHostUsage() {
        List<String> nics = getPhysicalNICs();
        double totalNicLimit = getTotalNicLimitWithConfiguration(nics);
        double totalNicUsageTx = getTotalNicUsage(nics, NICUsageType.TX, BitRateUnit.Kilobit);
        double totalNicUsageRx = getTotalNicUsage(nics, NICUsageType.RX, BitRateUnit.Kilobit);
        double totalCpuLimit = getTotalCpuLimit(isCGroupsEnabled);
        long now = System.currentTimeMillis();
        double elapsedSeconds = (now - lastCollection) / 1000d;
        if (elapsedSeconds <= 0) {
            log.warn("elapsedSeconds {} is not expected, skip this round of calculateBrokerHostUsage", elapsedSeconds);
            return;
        }
        SystemResourceUsage usage = new SystemResourceUsage();
        double cpuUsage = getTotalCpuUsage(elapsedSeconds);

        if (lastCollection == 0L) {
            usage.setMemory(getMemUsage());
            usage.setBandwidthIn(new ResourceUsage(0d, totalNicLimit));
            usage.setBandwidthOut(new ResourceUsage(0d, totalNicLimit));
        } else {
            double nicUsageTx = (totalNicUsageTx - lastTotalNicUsageTx) / elapsedSeconds;
            double nicUsageRx = (totalNicUsageRx - lastTotalNicUsageRx) / elapsedSeconds;

            usage.setMemory(getMemUsage());
            usage.setBandwidthIn(new ResourceUsage(nicUsageRx, totalNicLimit));
            usage.setBandwidthOut(new ResourceUsage(nicUsageTx, totalNicLimit));
        }

        lastTotalNicUsageTx = totalNicUsageTx;
        lastTotalNicUsageRx = totalNicUsageRx;
        lastCollection = System.currentTimeMillis();
        this.usage = usage;
        usage.setCpu(new ResourceUsage(cpuUsage, totalCpuLimit));
    }

    private double getTotalNicLimitWithConfiguration(List<String> nics) {
        // Use the override value as configured. Return the total max speed across all available NICs, converted
        // from Gbps into Kbps
        return overrideBrokerNicSpeedGbps.map(BitRateUnit.Gigabit::toKilobit)
                .orElseGet(() -> getTotalNicLimit(nics, BitRateUnit.Kilobit));
    }

    private double getTotalCpuUsage(double elapsedTimeSeconds) {
        if (isCGroupsEnabled) {
            return getTotalCpuUsageForCGroup(elapsedTimeSeconds);
        } else {
            return getTotalCpuUsageForEntireHost();
        }
    }

    private double getTotalCpuUsageForCGroup(double elapsedTimeSeconds) {
        double usage = getCpuUsageForCGroup();
        double currentUsage = usage - lastCpuUsage;
        lastCpuUsage = usage;
        return 100 * currentUsage / elapsedTimeSeconds / TimeUnit.SECONDS.toNanos(1);
    }

    /**
     * Reads first line of /proc/stat to get total cpu usage.
     *
     * <pre>
     *     cpu  user   nice system idle    iowait irq softirq steal guest guest_nice
     *     cpu  317808 128  58637  2503692 7634   0   13472   0     0     0
     * </pre>
     *
     * Line is split in "words", filtering the first. The sum of all numbers give the amount of cpu cycles used this
     * far. Real CPU usage should equal the sum substracting the idle cycles, this would include iowait, irq and steal.
     */
    private double getTotalCpuUsageForEntireHost() {
        LinuxInfoUtils.ResourceUsage cpuUsageForEntireHost = getCpuUsageForEntireHost();
        if (cpuUsageForEntireHost.isEmpty()) {
            return -1;
        }
        double currentUsage = (cpuUsageForEntireHost.getUsage() - lastCpuUsage)
                / (cpuUsageForEntireHost.getTotal() - lastCpuTotalTime) * getTotalCpuLimit(isCGroupsEnabled);
        lastCpuUsage = cpuUsageForEntireHost.getUsage();
        lastCpuTotalTime = cpuUsageForEntireHost.getTotal();
        return currentUsage;
    }

    private ResourceUsage getMemUsage() {
        double total = ((double) systemBean.getTotalPhysicalMemorySize()) / (1024 * 1024);
        double free = ((double) systemBean.getFreePhysicalMemorySize()) / (1024 * 1024);
        return new ResourceUsage(total - free, total);
    }

}
