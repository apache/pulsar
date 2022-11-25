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

import lombok.Data;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;

/**
 * Contains all the data that is maintained locally on each broker.
 *
 * Migrate from {@link org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData}.
 * And removed the lookup data, see {@link BrokerLookupData}
 */
@Data
public class BrokerLoadData {

    // Most recently available system resource usage.
    private ResourceUsage cpu;
    private ResourceUsage memory;
    private ResourceUsage directMemory;

    private ResourceUsage bandwidthIn;
    private ResourceUsage bandwidthOut;

    // Message data from the most recent namespace bundle stats.
    private double msgThroughputIn;
    private ResourceUsage msgThroughputInUsage;
    private double msgThroughputOut;
    private ResourceUsage msgThroughputOutUsage;
    private double msgRateIn;
    private double msgRateOut;

    public BrokerLoadData() {
        cpu = new ResourceUsage();
        memory = new ResourceUsage();
        directMemory = new ResourceUsage();
        bandwidthIn = new ResourceUsage();
        bandwidthOut = new ResourceUsage();
        msgThroughputInUsage = new ResourceUsage();
        msgThroughputOutUsage = new ResourceUsage();
    }

    /**
     * Using the system resource usage and bundle stats acquired from the Pulsar client, update this LocalBrokerData.
     *
     * @param systemResourceUsage
     *            System resource usage (cpu, memory, and direct memory).
     */
    public void update(final SystemResourceUsage systemResourceUsage) {
        updateSystemResourceUsage(systemResourceUsage);
    }

    /**
     * Using another LocalBrokerData, update this.
     *
     * @param other
     *            LocalBrokerData to update from.
     */
    public void update(final BrokerLoadData other) {
        updateSystemResourceUsage(other.cpu, other.memory, other.directMemory, other.bandwidthIn, other.bandwidthOut);
    }

    // Set the cpu, memory, and direct memory to that of the new system resource usage data.
    private void updateSystemResourceUsage(final SystemResourceUsage systemResourceUsage) {
        updateSystemResourceUsage(systemResourceUsage.cpu, systemResourceUsage.memory, systemResourceUsage.directMemory,
                systemResourceUsage.bandwidthIn, systemResourceUsage.bandwidthOut);
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

    public double getMaxResourceUsage() {
        return LocalBrokerData.max(cpu.percentUsage(), directMemory.percentUsage(), bandwidthIn.percentUsage(),
                bandwidthOut.percentUsage()) / 100;
    }

    public double getMaxResourceUsageWithWeight(final double cpuWeight, final double memoryWeight,
                                                final double directMemoryWeight, final double bandwidthInWeight,
                                                final double bandwidthOutWeight) {
        return LocalBrokerData.max(cpu.percentUsage() * cpuWeight, memory.percentUsage() * memoryWeight,
                directMemory.percentUsage() * directMemoryWeight, bandwidthIn.percentUsage() * bandwidthInWeight,
                bandwidthOut.percentUsage() * bandwidthOutWeight) / 100;
    }

}
