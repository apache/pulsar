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

/**
 * This class represents a object which reflects system resource usage per resource and the upper limit on the resource.
 */
public class SystemResourceUsage {

    /**
     * Definition of possible resource types.
     */
    public enum ResourceType {
        CPU, Memory, BandwidthIn, BandwidthOut
    }

    public ResourceUsage bandwidthIn;
    public ResourceUsage bandwidthOut;
    public ResourceUsage cpu;
    public ResourceUsage memory;
    public ResourceUsage directMemory;

    public SystemResourceUsage() {
        bandwidthIn = new ResourceUsage(-1, -1);
        bandwidthOut = new ResourceUsage(-1, -1);
        cpu = new ResourceUsage(-1, -1);
        memory = new ResourceUsage(-1, -1);
        directMemory = new ResourceUsage(-1, -1);
    }

    public void reset() {
        bandwidthIn.reset();
        bandwidthOut.reset();
        cpu.reset();
        memory.reset();
        directMemory.reset();
    }

    public ResourceUsage getBandwidthIn() {
        return bandwidthIn;
    }

    public void setBandwidthIn(ResourceUsage bandwidthIn) {
        this.bandwidthIn = bandwidthIn;
    }

    public ResourceUsage getBandwidthOut() {
        return bandwidthOut;
    }

    public void setBandwidthOut(ResourceUsage bandwidthOut) {
        this.bandwidthOut = bandwidthOut;
    }

    public ResourceUsage getCpu() {
        return cpu;
    }

    public void setCpu(ResourceUsage cpu) {
        this.cpu = cpu;
    }

    public ResourceUsage getMemory() {
        return memory;
    }

    public void setMemory(ResourceUsage memory) {
        this.memory = memory;
    }

    public ResourceUsage getDirectMemory() {
        return this.directMemory;
    }

    public void setDirectMemory(ResourceUsage directMemory) {
        this.directMemory = directMemory;
    }
}
