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
package org.apache.pulsar.common.policies.data;

import com.fasterxml.jackson.annotation.JsonIgnore;

import com.google.common.base.Objects;

/**
 * Resource quota for a namespace or namespace bundle.
 */
public class ResourceQuota {

    // messages published per second
    private double msgRateIn;
    // messages consumed per second
    private double msgRateOut;
    // incoming bytes per second
    private double bandwidthIn;
    // outgoing bytes per second
    private double bandwidthOut;
    // used memory in Mbytes
    private double memory;
    // allow the quota be dynamically re-calculated according to real traffic
    private boolean dynamic;

    public ResourceQuota() {
        msgRateIn = 0.0;
        msgRateOut = 0.0;
        bandwidthIn = 0.0;
        bandwidthOut = 0.0;
        memory = 0.0;
        dynamic = true;
    }

    /**
     * Set incoming message rate quota.
     *
     * @param msgRateIn
     *            incoming messages rate quota (msg/sec)
     */
    public void setMsgRateIn(double msgRateIn) {
        this.msgRateIn = msgRateIn;
    }

    /**
     * Get incoming message rate quota.
     *
     * @return incoming message rate quota (msg/sec)
     */
    public double getMsgRateIn() {
        return this.msgRateIn;
    }

    /**
     * Set outgoing message rate quota.
     *
     * @param msgRateOut
     *            outgoing messages rate quota (msg/sec)
     */
    public void setMsgRateOut(double msgRateOut) {
        this.msgRateOut = msgRateOut;
    }

    /**
     * Get outgoing message rate quota.
     *
     * @return outgoing message rate quota (msg/sec)
     */
    public double getMsgRateOut() {
        return this.msgRateOut;
    }

    /**
     * Set inbound bandwidth quota.
     *
     * @param bandwidthIn
     *            inbound bandwidth quota (bytes/sec)
     */
    public void setBandwidthIn(double bandwidthIn) {
        this.bandwidthIn = bandwidthIn;
    }

    /**
     * Get inbound bandwidth quota.
     *
     * @return inbound bandwidth quota (bytes/sec)
     */
    public double getBandwidthIn() {
        return this.bandwidthIn;
    }

    /**
     * Set outbound bandwidth quota.
     *
     * @param bandwidthOut
     *            outbound bandwidth quota (bytes/sec)
     */
    public void setBandwidthOut(double bandwidthOut) {
        this.bandwidthOut = bandwidthOut;
    }

    /**
     * Get outbound bandwidth quota.
     *
     * @return outbound bandwidth quota (bytes/sec)
     */
    public double getBandwidthOut() {
        return this.bandwidthOut;
    }

    /**
     * Set memory quota.
     *
     * @param memory
     *            memory quota (Mbytes)
     */
    public void setMemory(double memory) {
        this.memory = memory;
    }

    /**
     * Get memory quota.
     *
     * @return memory quota (Mbytes)
     */
    public double getMemory() {
        return this.memory;
    }

    /**
     * Set dynamic to true/false.
     *
     * @param dynamic
     *            allow the quota to be dynamically re-calculated
     */
    public void setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
    }

    /**
     * Get dynamic setting.
     *
     * @return is dynamic or not
     */
    public boolean getDynamic() {
        return this.dynamic;
    }

    /**
     * Check if this is a valid quota definition.
     */
    @JsonIgnore
    public boolean isValid() {
        if (this.msgRateIn > 0.0 && this.msgRateOut > 0.0 && this.bandwidthIn > 0.0 && this.bandwidthOut > 0.0
                && this.memory > 0.0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Add quota.
     *
     * @param quota
     *            <code>ResourceQuota</code> to add
     */
    public void add(ResourceQuota quota) {
        this.msgRateIn += quota.msgRateIn;
        this.msgRateOut += quota.msgRateOut;
        this.bandwidthIn += quota.bandwidthIn;
        this.bandwidthOut += quota.bandwidthOut;
        this.memory += quota.memory;
    }

    /**
     * Substract quota.
     *
     * @param quota
     *            <code>ResourceQuota</code> to substract
     */
    public void substract(ResourceQuota quota) {
        this.msgRateIn -= quota.msgRateIn;
        this.msgRateOut -= quota.msgRateOut;
        this.bandwidthIn -= quota.bandwidthIn;
        this.bandwidthOut -= quota.bandwidthOut;
        this.memory -= quota.memory;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(msgRateIn, msgRateOut, bandwidthIn,
                bandwidthOut, memory, dynamic);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ResourceQuota) {
            ResourceQuota other = (ResourceQuota) obj;
            return Objects.equal(this.msgRateIn, other.msgRateIn) && Objects.equal(this.msgRateOut, other.msgRateOut)
                    && Objects.equal(this.bandwidthIn, other.bandwidthIn)
                    && Objects.equal(this.bandwidthOut, other.bandwidthOut) && Objects.equal(this.memory, other.memory)
                    && Objects.equal(this.dynamic, other.dynamic);
        }
        return false;
    }
}
