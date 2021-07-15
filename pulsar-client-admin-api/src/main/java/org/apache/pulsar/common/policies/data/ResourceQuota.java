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

import lombok.Data;

/**
 * Resource quota for a namespace or namespace bundle.
 */
@Data
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

    public boolean getDynamic() {
        return dynamic;
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

}
