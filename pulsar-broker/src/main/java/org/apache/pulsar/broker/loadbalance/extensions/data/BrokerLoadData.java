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
package org.apache.pulsar.broker.loadbalance.extensions.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;

/**
 * Contains all the data that is maintained locally on each broker.
 *
 * Migrate from {@link org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData}.
 * And removed the lookup data, see {@link BrokerLookupData}
 */
@Getter
@EqualsAndHashCode
@ToString
public class BrokerLoadData {

    private static final double DEFAULT_RESOURCE_USAGE = 1.0d;

    // Most recently available system resource usage.
    private ResourceUsage cpu;
    private ResourceUsage memory;
    private ResourceUsage directMemory;
    private ResourceUsage bandwidthIn;
    private ResourceUsage bandwidthOut;

    // Message data from the most recent namespace bundle stats.
    private double msgThroughputIn; // bytes/sec
    private double msgThroughputOut;  // bytes/sec
    private double msgRateIn; // messages/sec
    private double msgRateOut; // messages/sec
    private int bundleCount;

    // Load data features computed from the above resources.
    private double maxResourceUsage; // max of resource usages
    /**
     * Exponential moving average(EMA) of max of weighted resource usages among
     * cpu, memory, directMemory, bandwidthIn and bandwidthOut.
     *
     * The resource weights are configured by :
     * loadBalancerCPUResourceWeight,
     * loadBalancerMemoryResourceWeight,
     * loadBalancerDirectMemoryResourceWeight,
     * loadBalancerBandwithInResourceWeight, and
     * loadBalancerBandwithOutResourceWeight.
     *
     * The historical resource percentage is configured by loadBalancerHistoryResourcePercentage.
     */
    private double weightedMaxEMA;
    private long updatedAt;

    @Setter
    private long reportedAt;

    public BrokerLoadData() {
        cpu = new ResourceUsage();
        memory = new ResourceUsage();
        directMemory = new ResourceUsage();
        bandwidthIn = new ResourceUsage();
        bandwidthOut = new ResourceUsage();
        maxResourceUsage = DEFAULT_RESOURCE_USAGE;
        weightedMaxEMA = DEFAULT_RESOURCE_USAGE;
    }

    /**
     * Using the system resource usage from the Pulsar client, update this BrokerLoadData.
     *
     * @param usage
     *            System resource usage (cpu, memory, and direct memory).
     * @param msgThroughputIn
     *            broker-level message input throughput in bytes/s.
     * @param msgThroughputOut
     *            broker-level message output throughput in bytes/s.
     * @param msgRateIn
     *            broker-level message input rate in messages/s.
     * @param msgRateOut
     *            broker-level message output rate in messages/s.
     * @param bundleCount
     *            broker-level bundle counts.
     * @param conf
     *            Service configuration to compute load data features.
     */
    public void update(final SystemResourceUsage usage,
                       double msgThroughputIn,
                       double msgThroughputOut,
                       double msgRateIn,
                       double msgRateOut,
                       int bundleCount,
                       ServiceConfiguration conf) {
        updateSystemResourceUsage(usage.cpu, usage.memory, usage.directMemory, usage.bandwidthIn, usage.bandwidthOut);
        this.msgThroughputIn = msgThroughputIn;
        this.msgThroughputOut = msgThroughputOut;
        this.msgRateIn = msgRateIn;
        this.msgRateOut = msgRateOut;
        this.bundleCount = bundleCount;
        updateFeatures(conf);
        updatedAt = System.currentTimeMillis();
    }

    /**
     * Using another BrokerLoadData, update this.
     *
     * @param other
     *            BrokerLoadData to update from.
     */
    public void update(final BrokerLoadData other) {
        updateSystemResourceUsage(other.cpu, other.memory, other.directMemory, other.bandwidthIn, other.bandwidthOut);
        msgThroughputIn = other.msgThroughputIn;
        msgThroughputOut = other.msgThroughputOut;
        msgRateIn = other.msgRateIn;
        msgRateOut = other.msgRateOut;
        bundleCount = other.bundleCount;
        weightedMaxEMA = other.weightedMaxEMA;
        maxResourceUsage = other.maxResourceUsage;
        updatedAt = other.updatedAt;
        reportedAt = other.reportedAt;
    }

    // Update resource usage given each individual usage.
    private void updateSystemResourceUsage(final ResourceUsage cpu, final ResourceUsage memory,
                                           final ResourceUsage directMemory, final ResourceUsage bandwidthIn,
                                           final ResourceUsage bandwidthOut) {
        this.cpu = cpu;
        this.memory = memory;
        this.directMemory = directMemory;
        this.bandwidthIn = bandwidthIn;
        this.bandwidthOut = bandwidthOut;
    }

    private void updateFeatures(ServiceConfiguration conf) {
        updateMaxResourceUsage();
        updateWeightedMaxEMA(conf);
    }

    private void updateMaxResourceUsage() {
        maxResourceUsage = LocalBrokerData.max(cpu.percentUsage(), directMemory.percentUsage(),
                bandwidthIn.percentUsage(),
                bandwidthOut.percentUsage()) / 100;
    }

    private double getMaxResourceUsageWithWeight(final double cpuWeight, final double memoryWeight,
                                                final double directMemoryWeight, final double bandwidthInWeight,
                                                final double bandwidthOutWeight) {
        return LocalBrokerData.max(cpu.percentUsage() * cpuWeight, memory.percentUsage() * memoryWeight,
                directMemory.percentUsage() * directMemoryWeight, bandwidthIn.percentUsage() * bandwidthInWeight,
                bandwidthOut.percentUsage() * bandwidthOutWeight) / 100;
    }

    private void updateWeightedMaxEMA(ServiceConfiguration conf) {
        var historyPercentage = conf.getLoadBalancerHistoryResourcePercentage();
        var weightedMax = getMaxResourceUsageWithWeight(
                conf.getLoadBalancerCPUResourceWeight(),
                conf.getLoadBalancerMemoryResourceWeight(), conf.getLoadBalancerDirectMemoryResourceWeight(),
                conf.getLoadBalancerBandwithInResourceWeight(),
                conf.getLoadBalancerBandwithOutResourceWeight());
        weightedMaxEMA = updatedAt == 0 ? weightedMax :
                weightedMaxEMA * historyPercentage + (1 - historyPercentage) * weightedMax;
    }

    public String toString(ServiceConfiguration conf) {
        return String.format("cpu= %.2f%%, memory= %.2f%%, directMemory= %.2f%%, "
                        + "bandwithIn= %.2f%%, bandwithOut= %.2f%%, "
                        + "cpuWeight= %f, memoryWeight= %f, directMemoryWeight= %f, "
                        + "bandwithInResourceWeight= %f, bandwithOutResourceWeight= %f, "
                        + "msgThroughputIn= %.2f, msgThroughputOut= %.2f, msgRateIn= %.2f, msgRateOut= %.2f, "
                        + "bundleCount= %d, "
                        + "maxResourceUsage= %.2f%%, weightedMaxEMA= %.2f%%, "
                        + "updatedAt= %d, reportedAt= %d",

                cpu.percentUsage(), memory.percentUsage(), directMemory.percentUsage(),
                bandwidthIn.percentUsage(), bandwidthOut.percentUsage(),
                conf.getLoadBalancerCPUResourceWeight(),
                conf.getLoadBalancerMemoryResourceWeight(),
                conf.getLoadBalancerDirectMemoryResourceWeight(),
                conf.getLoadBalancerBandwithInResourceWeight(),
                conf.getLoadBalancerBandwithOutResourceWeight(),
                msgThroughputIn, msgThroughputOut, msgRateIn, msgRateOut,
                bundleCount,
                maxResourceUsage * 100, weightedMaxEMA * 100,
                updatedAt, reportedAt
        );
    }

    public List<Metrics> toMetrics(String advertisedBrokerAddress) {
        var metrics = new ArrayList<Metrics>();
        var dimensions = new HashMap<String, String>();
        dimensions.put("metric", "loadBalancing");
        dimensions.put("broker", advertisedBrokerAddress);
        {
            var metric = Metrics.create(dimensions);
            metric.put("brk_lb_cpu_usage", getCpu().percentUsage());
            metric.put("brk_lb_memory_usage", getMemory().percentUsage());
            metric.put("brk_lb_directMemory_usage", getDirectMemory().percentUsage());
            metric.put("brk_lb_bandwidth_in_usage", getBandwidthIn().percentUsage());
            metric.put("brk_lb_bandwidth_out_usage", getBandwidthOut().percentUsage());
            metrics.add(metric);
        }
        {
            var dim = new HashMap<>(dimensions);
            dim.put("feature", "max_ema");
            var metric = Metrics.create(dim);
            metric.put("brk_lb_resource_usage", weightedMaxEMA);
            metrics.add(metric);
        }
        {
            var dim = new HashMap<>(dimensions);
            dim.put("feature", "max");
            var metric = Metrics.create(dim);
            metric.put("brk_lb_resource_usage", maxResourceUsage);
            metrics.add(metric);
        }
        return metrics;
    }

}
