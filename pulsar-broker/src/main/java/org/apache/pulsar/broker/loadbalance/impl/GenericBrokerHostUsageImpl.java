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

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;

/**
 * Class that will return the broker host usage.
 */
public class GenericBrokerHostUsageImpl implements BrokerHostUsage {
    // The interval for host usage check command
    private static final int CPU_CHECK_MILLIS = 1000;
    private double totalCpuLimit;
    private double cpuUsageSum = 0d;
    private int cpuUsageCount = 0;
    private OperatingSystemMXBean systemBean;
    private SystemResourceUsage usage;

    public GenericBrokerHostUsageImpl(PulsarService pulsar) {
        this(
            pulsar.getConfiguration().getLoadBalancerHostUsageCheckIntervalMinutes(),
            pulsar.getLoadManagerExecutor()
        );
    }

    public GenericBrokerHostUsageImpl(int hostUsageCheckIntervalMin,
                                      ScheduledExecutorService executorService) {
        this.systemBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        this.usage = new SystemResourceUsage();
        this.totalCpuLimit = getTotalCpuLimit();
        // Call now to initialize values before the constructor returns
        calculateBrokerHostUsage();
        executorService.scheduleAtFixedRate(this::checkCpuLoad, CPU_CHECK_MILLIS,
                CPU_CHECK_MILLIS, TimeUnit.MILLISECONDS);
        executorService.scheduleAtFixedRate(this::doCalculateBrokerHostUsage, hostUsageCheckIntervalMin,
                hostUsageCheckIntervalMin, TimeUnit.MINUTES);
    }

    @Override
    public SystemResourceUsage getBrokerHostUsage() {
        return usage;
    }

    private synchronized void checkCpuLoad() {
        cpuUsageSum += systemBean.getSystemCpuLoad();
        cpuUsageCount++;
    }

    @Override
    public void calculateBrokerHostUsage() {
        checkCpuLoad();
        doCalculateBrokerHostUsage();
    }

    void doCalculateBrokerHostUsage() {
        SystemResourceUsage usage = new SystemResourceUsage();
        usage.setCpu(getCpuUsage());
        usage.setMemory(getMemUsage());

        this.usage = usage;
    }

    private double getTotalCpuLimit() {
        return 100 * Runtime.getRuntime().availableProcessors();
    }

    private synchronized double getTotalCpuUsage() {
        if (cpuUsageCount == 0) {
            return 0;
        }
        double cpuUsage = cpuUsageSum / cpuUsageCount;
        cpuUsageSum = 0d;
        cpuUsageCount = 0;
        return cpuUsage;
    }

    private ResourceUsage getCpuUsage() {
        return new ResourceUsage(getTotalCpuUsage() * totalCpuLimit, totalCpuLimit);
    }

    private ResourceUsage getMemUsage() {
        double total = ((double) systemBean.getTotalPhysicalMemorySize()) / (1024 * 1024);
        double free = ((double) systemBean.getFreePhysicalMemorySize()) / (1024 * 1024);
        return new ResourceUsage(total - free, total);
    }
}
