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

import com.google.common.collect.ComparisonChain;

/**
 * Information about the broker status.
 */
public class BrokerStatus implements Comparable<BrokerStatus> {
    private String brokerAddress;
    private boolean active;
    private int loadFactor;

    public BrokerStatus(String lookupServiceAddress, boolean active, int loadFactor) {
        this.brokerAddress = lookupServiceAddress;
        this.active = active;
        this.loadFactor = loadFactor;
    }

    public boolean isActive() {
        return this.active;
    }

    public int getLoadFactor() {
        return this.loadFactor;
    }

    public String getBrokerAddress() {
        return this.brokerAddress;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public void setLoadFactor(int loadFactor) {
        this.loadFactor = loadFactor;
    }

    @Override
    public int compareTo(BrokerStatus other) {
        return ComparisonChain.start().compare(this.loadFactor, other.loadFactor)
                .compare(this.brokerAddress, other.brokerAddress).result();
    }

    @Override
    public String toString() {
        return String.format("[brokerAddress=%s, active=%s, loadFactor=%s]", brokerAddress, active, loadFactor);
    }
}
