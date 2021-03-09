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
package org.apache.pulsar.policies.data.loadbalancer;

import java.util.Set;
import lombok.EqualsAndHashCode;
import org.apache.pulsar.common.policies.data.ResourceQuota;

/**
 * The class containing information about system resources, allocated quota, and loaded bundles.
 */
@EqualsAndHashCode
public class ResourceUnitRanking implements Comparable<ResourceUnitRanking> {

    private static final long KBITS_TO_BYTES = 1024 / 8;
    private static final double PERCENTAGE_DIFFERENCE_THRESHOLD = 5.0;
    private static double cpuUsageByMsgRate = 0.05;

    // system resource usage from last LoadReport
    private SystemResourceUsage systemResourceUsage;
    // sum of quota allocated to bundles which are already loaded by each broker
    private ResourceQuota allocatedQuota;
    // sum of quota allocated to bundles which are assigned and to be loaded by each broker
    private ResourceQuota preAllocatedQuota;
    // list of bundles already loaded
    private Set<String> loadedBundles;
    // list of bundles assigned and to be loaded
    private Set<String> preAllocatedBundles;

    // estimated percentage of resource usage with the already assigned (both loaded and to-be-loaded) bundles
    private double estimatedLoadPercentage;

    // estimated number of total messages with the already assigned (both loaded and to-be-loaded) bundles
    private double estimatedMessageRate;

    private double allocatedLoadPercentageCPU;
    private double allocatedLoadPercentageMemory;
    private double allocatedLoadPercentageBandwidthIn;
    private double allocatedLoadPercentageBandwidthOut;

    private double estimatedLoadPercentageCPU;
    private double estimatedLoadPercentageMemory;
    private double estimatedLoadPercentageDirectMemory;
    private double estimatedLoadPercentageBandwidthIn;
    private double estimatedLoadPercentageBandwidthOut;

    public ResourceUnitRanking(SystemResourceUsage systemResourceUsage, Set<String> loadedBundles,
            ResourceQuota allocatedQuota, Set<String> preAllocatedBundles, ResourceQuota preAllocatedQuota) {
        this.systemResourceUsage = systemResourceUsage;
        this.loadedBundles = loadedBundles;
        this.allocatedQuota = allocatedQuota;
        this.preAllocatedBundles = preAllocatedBundles;
        this.preAllocatedQuota = preAllocatedQuota;
        this.estimateLoadPercentage();
    }

    public static void setCpuUsageByMsgRate(double cpuUsageByMsgRate) {
        ResourceUnitRanking.cpuUsageByMsgRate = cpuUsageByMsgRate;
    }

    /**
     * Estimate the load percentage which is the max percentage of all resource usages.
     */
    private void estimateLoadPercentage() {
        double cpuUsed = this.systemResourceUsage.cpu.usage;
        double cpuAllocated = cpuUsageByMsgRate
                * (this.allocatedQuota.getMsgRateIn() + this.allocatedQuota.getMsgRateOut());
        double cpuPreAllocated = cpuUsageByMsgRate
                * (this.preAllocatedQuota.getMsgRateIn() + this.preAllocatedQuota.getMsgRateOut());
        this.allocatedLoadPercentageCPU = (this.systemResourceUsage.cpu.limit <= 0) ? 0
                : Math.min(100, 100 * cpuAllocated / this.systemResourceUsage.cpu.limit);
        this.estimatedLoadPercentageCPU = (this.systemResourceUsage.cpu.limit <= 0) ? 0
                : Math.min(100,
                        100 * (Math.max(cpuUsed, cpuAllocated) + cpuPreAllocated) / this.systemResourceUsage.cpu.limit);

        double memUsed = this.systemResourceUsage.memory.usage;
        double memAllocated = this.allocatedQuota.getMemory();
        double memPreAllocated = this.preAllocatedQuota.getMemory();
        this.allocatedLoadPercentageMemory = (this.systemResourceUsage.memory.limit <= 0) ? 0
                : Math.min(100, 100 * memAllocated / this.systemResourceUsage.memory.limit);
        this.estimatedLoadPercentageMemory = (this.systemResourceUsage.memory.limit <= 0) ? 0
                : Math.min(100, 100 * (Math.max(memUsed, memAllocated) + memPreAllocated)
                        / this.systemResourceUsage.memory.limit);

        double bandwidthInUsed = this.systemResourceUsage.bandwidthIn.usage;
        double bandwidthInAllocated = this.allocatedQuota.getBandwidthIn() / KBITS_TO_BYTES;
        double bandwidthInPreAllocated = this.preAllocatedQuota.getBandwidthIn() / KBITS_TO_BYTES;
        this.allocatedLoadPercentageBandwidthIn = (this.systemResourceUsage.bandwidthIn.limit <= 0) ? 0
                : Math.min(100, 100 * bandwidthInAllocated / this.systemResourceUsage.bandwidthIn.limit);
        this.estimatedLoadPercentageBandwidthIn = (this.systemResourceUsage.bandwidthIn.limit <= 0) ? 0
                : Math.min(100, 100 * (Math.max(bandwidthInUsed, bandwidthInAllocated) + bandwidthInPreAllocated)
                        / this.systemResourceUsage.bandwidthIn.limit);

        double bandwidthOutUsed = this.systemResourceUsage.bandwidthOut.usage;
        double bandwidthOutAllocated = this.allocatedQuota.getBandwidthOut() / KBITS_TO_BYTES;
        double bandwidthOutPreAllocated = this.preAllocatedQuota.getBandwidthOut() / KBITS_TO_BYTES;
        this.allocatedLoadPercentageBandwidthOut = (this.systemResourceUsage.bandwidthOut.limit <= 0) ? 0
                : Math.min(100, 100 * bandwidthOutAllocated / this.systemResourceUsage.bandwidthOut.limit);
        this.estimatedLoadPercentageBandwidthOut = (this.systemResourceUsage.bandwidthOut.limit <= 0) ? 0
                : Math.min(100, 100 * (Math.max(bandwidthOutUsed, bandwidthOutAllocated) + bandwidthOutPreAllocated)
                        / this.systemResourceUsage.bandwidthOut.limit);

        double directMemoryUsed = this.systemResourceUsage.directMemory.usage;
        this.estimatedLoadPercentageDirectMemory = (this.systemResourceUsage.directMemory.limit <= 0) ? 0
                : Math.min(100, 100 * directMemoryUsed / this.systemResourceUsage.directMemory.limit);

        this.estimatedLoadPercentage = Math.max(this.estimatedLoadPercentageCPU,
                Math.max(this.estimatedLoadPercentageMemory, Math.max(this.estimatedLoadPercentageDirectMemory,
                        Math.max(this.estimatedLoadPercentageBandwidthIn, this.estimatedLoadPercentageBandwidthOut))));

        this.estimatedMessageRate = this.allocatedQuota.getMsgRateIn() + this.allocatedQuota.getMsgRateOut()
            + this.preAllocatedQuota.getMsgRateIn() + this.preAllocatedQuota.getMsgRateOut();

    }

    public int compareTo(ResourceUnitRanking other) {
        if (Math.abs(this.estimatedLoadPercentage - other.estimatedLoadPercentage) > PERCENTAGE_DIFFERENCE_THRESHOLD) {
            return Double.compare(this.estimatedLoadPercentage, other.estimatedLoadPercentage);
        }
        if (Math.abs(this.estimatedLoadPercentageMemory
                - other.estimatedLoadPercentageMemory) > PERCENTAGE_DIFFERENCE_THRESHOLD) {
            return Double.compare(this.estimatedLoadPercentageMemory, other.estimatedLoadPercentageMemory);
        }
        if (Math.abs(
                this.estimatedLoadPercentageCPU - other.estimatedLoadPercentageCPU) > PERCENTAGE_DIFFERENCE_THRESHOLD) {
            return Double.compare(this.estimatedLoadPercentageCPU, other.estimatedLoadPercentageCPU);
        }
        if (Math.abs(this.estimatedLoadPercentageBandwidthIn
                - other.estimatedLoadPercentageBandwidthIn) > PERCENTAGE_DIFFERENCE_THRESHOLD) {
            return Double.compare(this.estimatedLoadPercentageBandwidthIn, other.estimatedLoadPercentageBandwidthIn);
        }
        if (Math.abs(this.estimatedLoadPercentageBandwidthOut
                - other.estimatedLoadPercentageBandwidthOut) > PERCENTAGE_DIFFERENCE_THRESHOLD) {
            return Double.compare(this.estimatedLoadPercentageBandwidthOut, other.estimatedLoadPercentageBandwidthOut);
        }
        return Double.compare(this.estimatedLoadPercentage, other.estimatedLoadPercentage);
    }

    /**
     * Compare two loads based on message rate only.
     */
    public int compareMessageRateTo(ResourceUnitRanking other) {
        return Double.compare(this.estimatedMessageRate, other.estimatedMessageRate);
    }

    /**
     * If the ResourceUnit is idle.
     */
    public boolean isIdle() {
        return this.loadedBundles.isEmpty() && this.preAllocatedBundles.isEmpty();
    }

    /**
     * Check if a ServiceUnit is already loaded by this ResourceUnit.
     */
    public boolean isServiceUnitLoaded(String suName) {
        return this.loadedBundles.contains(suName);
    }

    /**
     * Check if a ServiceUnit is pre-allocated to this ResourceUnit.
     */
    public boolean isServiceUnitPreAllocated(String suName) {
        return this.preAllocatedBundles.contains(suName);
    }

    /**
     * Pre-allocate a ServiceUnit to this ResourceUnit.
     */
    public void addPreAllocatedServiceUnit(String suName, ResourceQuota quota) {
        this.preAllocatedBundles.add(suName);
        this.preAllocatedQuota.add(quota);
        estimateLoadPercentage();
    }

    /**
     * Remove a service unit from the loaded bundle list.
     */
    public void removeLoadedServiceUnit(String suName, ResourceQuota quota) {
        if (this.loadedBundles.remove(suName)) {
            this.allocatedQuota.substract(quota);
            estimateLoadPercentage();
        }
    }

    /**
     * Get the pre-allocated bundles.
     */
    public Set<String> getPreAllocatedBundles() {
        return this.preAllocatedBundles;
    }

    /**
     * Get the loaded bundles.
     */
    public Set<String> getLoadedBundles() {
        return loadedBundles;
    }

    /**
     * Get the estimated load percentage.
     */
    public double getEstimatedLoadPercentage() {
        return this.estimatedLoadPercentage;
    }

    /**
     * Get the estimated message rate.
     */
    public double getEstimatedMessageRate() {
        return this.estimatedMessageRate;
    }

    /**
     * Percentage of CPU allocated to bundle's quota.
     */
    public double getAllocatedLoadPercentageCPU() {
        return this.allocatedLoadPercentageCPU;
    }

    /**
     * Percetage of memory allocated to bundle's quota.
     */
    public double getAllocatedLoadPercentageMemory() {
        return this.allocatedLoadPercentageMemory;
    }

    /**
     * Percentage of inbound bandwidth allocated to bundle's quota.
     */
    public double getAllocatedLoadPercentageBandwidthIn() {
        return this.allocatedLoadPercentageBandwidthIn;
    }

    /**
     * Percentage of outbound bandwidth allocated to bundle's quota.
     */
    public double getAllocatedLoadPercentageBandwidthOut() {
        return this.allocatedLoadPercentageBandwidthOut;
    }

    /**
     * Get the load percentage in String, with detail resource usages.
     */
    public String getEstimatedLoadPercentageString() {
        return String.format(
            "msgrate: %.0f, load: %.1f%% - cpu: %.1f%%, mem: %.1f%%, directMemory: %.1f%%, "
                + "bandwidthIn: %.1f%%, bandwidthOut: %.1f%%",
            this.estimatedMessageRate,
            this.estimatedLoadPercentage, this.estimatedLoadPercentageCPU, this.estimatedLoadPercentageMemory,
            this.estimatedLoadPercentageDirectMemory, this.estimatedLoadPercentageBandwidthIn,
            this.estimatedLoadPercentageBandwidthOut);
    }

    /**
     * Estimate the maximum number of namespace bundles ths ResourceUnit is able to handle with all resource.
     */
    public long estimateMaxCapacity(ResourceQuota defaultQuota) {
        return calculateBrokerMaxCapacity(this.systemResourceUsage, defaultQuota);
    }

    /**
     * Estimate the maximum number namespace bundles a ResourceUnit is able to handle with all resource.
     */
    public static long calculateBrokerMaxCapacity(SystemResourceUsage systemResourceUsage, ResourceQuota defaultQuota) {
        double bandwidthOutLimit = systemResourceUsage.bandwidthOut.limit * KBITS_TO_BYTES;
        double bandwidthInLimit = systemResourceUsage.bandwidthIn.limit * KBITS_TO_BYTES;

        long capacity = calculateBrokerCapacity(defaultQuota, systemResourceUsage.cpu.limit,
                systemResourceUsage.memory.limit, bandwidthOutLimit, bandwidthInLimit);
        return capacity;
    }

    /**
     * Calculate how many bundles could be handle with the specified resources.
     */
    private static long calculateBrokerCapacity(ResourceQuota defaultQuota, double usableCPU, double usableMem,
            double usableBandwidthOut, double usableBandwidthIn) {
        // estimate capacity with usable CPU
        double cpuCapacity = (usableCPU / cpuUsageByMsgRate)
                / (defaultQuota.getMsgRateIn() + defaultQuota.getMsgRateOut());
        // estimate capacity with usable memory
        double memCapacity = usableMem / defaultQuota.getMemory();
        // estimate capacity with usable outbound bandwidth
        double bandwidthOutCapacity = usableBandwidthOut / defaultQuota.getBandwidthOut();
        // estimate capacity with usable inbound bandwidth
        double bandwidthInCapacity = usableBandwidthIn / defaultQuota.getBandwidthIn();

        // the ServiceUnit capacity is determined by the minimum capacity of resources
        double capacity = Math.min(cpuCapacity,
                Math.min(memCapacity, Math.min(bandwidthOutCapacity, bandwidthInCapacity)));

        return (long) Math.max(capacity, 0);
    }
}
