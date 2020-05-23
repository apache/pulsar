/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker;

import com.google.common.base.MoreObjects;
import org.apache.pulsar.policies.data.loadbalancer.JSONWritable;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;

import java.util.Map;
import java.util.Set;

/**
 * Data class aggregating the short term and long term data across all bundles belonging to a broker.
 */
public class OverallBandwidthBrokerData extends JSONWritable {
    private double overallBandwidthIn;
    private double overallBandwidthOut;

    public OverallBandwidthBrokerData() {
        overallBandwidthIn = 0;
        overallBandwidthOut = 0;
    }

    public void update(final LocalBrokerData localBrokerData) {
        overallBandwidthIn += localBrokerData.getBandwidthIn().usage;
        overallBandwidthOut += localBrokerData.getBandwidthOut().usage;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("overallBandwidthIn", overallBandwidthIn)
                .add("overallBandwidthOut", overallBandwidthOut).toString();
    }

    public double getOverallBandwidthIn() {
        return overallBandwidthIn;
    }

    public void setOverallBandwidthIn(double overallBandwidthIn) {
        this.overallBandwidthIn = overallBandwidthIn;
    }

    public double getOverallBandwidthOut() {
        return overallBandwidthOut;
    }

    public void setOverallBandwidthOut(double overallBandwidthOut) {
        this.overallBandwidthOut = overallBandwidthOut;
    }
}
