/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.loadbalance.data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;
import com.yahoo.pulsar.broker.loadbalance.data.SystemResourceUsage.ResourceType;

/**
 * This class represents the overall load of the broker - it includes overall {@link SystemResourceUsage} and
 * {@link NamespaceUsage} for all the namespaces hosted by this broker.
 */
public class LoadReport {
    private boolean isUnderLoaded;
    private boolean isOverLoaded;
    private String name;
    private long timestamp;
    private double msgRateIn;
    private double msgRateOut;
    private long numTopics;
    private long numConsumers;
    private long numProducers;
    private long numBundles;

    public LoadReport() {
        isUnderLoaded = false;
        isOverLoaded = false;
        timestamp = 0;
        msgRateIn = 0.0;
        msgRateOut = 0.0;
        numTopics = 0;
        numConsumers = 0;
        numProducers = 0;
        numBundles = 0;
    }

    /**
     * overall machine resource used, not just by broker process
     */
    private SystemResourceUsage systemResourceUsage;

    private Map<String, NamespaceBundleStats> bundleStats;

    public void setBundleStats(Map<String, NamespaceBundleStats> stats) {
        bundleStats = (stats == null) ? null : new HashMap<String, NamespaceBundleStats>(stats);
    }

    public Map<String, NamespaceBundleStats> getBundleStats() {
        return bundleStats;
    }

    public String getName() {
        return name;
    }

    public void setName(String brokerName) {
        this.name = brokerName;
    }

    public SystemResourceUsage getSystemResourceUsage() {
        return systemResourceUsage;
    }

    public void setSystemResourceUsage(SystemResourceUsage systemResourceUsage) {
        this.systemResourceUsage = systemResourceUsage;
    }

    public boolean isUnderLoaded() {
        return isUnderLoaded;
    }

    public void setUnderLoaded(boolean isUnderLoaded) {
        this.isUnderLoaded = isUnderLoaded;
    }

    public boolean isOverLoaded() {
        return isOverLoaded;
    }

    @JsonIgnore
    public ResourceType getBottleneckResourceType() {
        ResourceType type = ResourceType.CPU;
        double maxUsage = systemResourceUsage.cpu.percentUsage();
        if (systemResourceUsage.memory.percentUsage() > maxUsage) {
            maxUsage = systemResourceUsage.memory.percentUsage();
            type = ResourceType.Memory;
        }

        if (systemResourceUsage.bandwidthIn.percentUsage() > maxUsage) {
            maxUsage = systemResourceUsage.bandwidthIn.percentUsage();
            type = ResourceType.BandwidthIn;
        }

        if (systemResourceUsage.bandwidthOut.percentUsage() > maxUsage) {
            maxUsage = systemResourceUsage.bandwidthOut.percentUsage();
            type = ResourceType.BandwidthOut;
        }

        return type;
    }

    public void setOverLoaded(boolean isOverLoaded) {
        this.isOverLoaded = isOverLoaded;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getMsgRateIn() {
        msgRateIn = 0.0;
        if (this.bundleStats != null) {
            this.bundleStats.forEach((bundle, stats) -> {
                msgRateIn += stats.msgRateIn;
            });
        }
        return msgRateIn;
    }

    public double getMsgRateOut() {
        msgRateOut = 0.0;
        if (this.bundleStats != null) {
            this.bundleStats.forEach((bundle, stats) -> {
                msgRateOut += stats.msgRateOut;
            });
        }
        return msgRateOut;
    }

    public long getNumTopics() {
        numTopics = 0;
        if (this.bundleStats != null) {
            this.bundleStats.forEach((bundle, stats) -> {
                numTopics += stats.topics;
            });
        }
        return numTopics;
    }

    public long getNumConsumers() {
        numConsumers = 0;
        if (this.bundleStats != null) {
            for (Map.Entry<String, NamespaceBundleStats> entry : this.bundleStats.entrySet()) {
                numConsumers = numConsumers + entry.getValue().consumerCount;
            }
        }
        return numConsumers;
    }

    public long getNumProducers() {
        numProducers = 0;
        if (this.bundleStats != null) {
            for (Map.Entry<String, NamespaceBundleStats> entry : this.bundleStats.entrySet()) {
                numProducers = numProducers + entry.getValue().producerCount;
            }
        }
        return numProducers;
    }

    public long getNumBundles() {
        numBundles = 0;
        if (this.bundleStats != null) {
            numBundles = this.bundleStats.size();
        }
        return numBundles;
    }

    @JsonIgnore
    public Set<String> getBundles() {
        if (this.bundleStats != null) {
            return new HashSet<String>(this.bundleStats.keySet());
        } else {
            return new HashSet<String>();
        }
    }

    @JsonIgnore
    public TreeMap<String, NamespaceBundleStats> getSortedBundleStats(ResourceType resType) {
        if (bundleStats == null) {
            return null;
        }

        NamespaceBundleStatsComparator nsc = new NamespaceBundleStatsComparator(bundleStats, resType);
        TreeMap<String, NamespaceBundleStats> sortedBundleStats = Maps.newTreeMap(nsc);
        sortedBundleStats.putAll(bundleStats);
        return sortedBundleStats;
    }
}
