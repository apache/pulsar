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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.apache.pulsar.common.util.NamespaceBundleStatsComparator;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage.ResourceType;

/**
 * This class represents the overall load of the broker - it includes overall {@link SystemResourceUsage} and
 * {@link NamespaceUsage} for all the namespaces hosted by this broker.
 */
@JsonDeserialize(as = LoadReport.class)
public class LoadReport implements LoadManagerReport {
    private String name;
    private String brokerVersionString;

    private final String webServiceUrl;
    private final String webServiceUrlTls;
    private final String pulsarServiceUrl;
    private final String pulsarServiceUrlTls;
    private boolean persistentTopicsEnabled = true;
    private boolean nonPersistentTopicsEnabled = true;

    private boolean isUnderLoaded;
    private boolean isOverLoaded;
    private long timestamp;
    private double msgRateIn;
    private double msgRateOut;
    private int numTopics;
    private int numConsumers;
    private int numProducers;
    private int numBundles;
    private Map<String, String> protocols;
    // This place-holder requires to identify correct LoadManagerReport type while deserializing
    @SuppressWarnings("checkstyle:ConstantName")
    public static final String loadReportType = LoadReport.class.getSimpleName();

    public LoadReport() {
        this(null, null, null, null);
    }

    public LoadReport(String webServiceUrl, String webServiceUrlTls, String pulsarServiceUrl,
            String pulsarServiceUrlTls) {
        this.webServiceUrl = webServiceUrl;
        this.webServiceUrlTls = webServiceUrlTls;
        this.pulsarServiceUrl = pulsarServiceUrl;
        this.pulsarServiceUrlTls = pulsarServiceUrlTls;
        this.protocols = new HashMap<>();
        bundleLosses = new HashSet<>();
        bundleGains = new HashSet<>();
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
     * Overall machine resource used, not just by broker process.
     */
    private SystemResourceUsage systemResourceUsage;

    private Map<String, NamespaceBundleStats> bundleStats;

    private Set<String> bundleGains;

    private Set<String> bundleLosses;

    private double allocatedCPU;
    private double allocatedMemory;
    private double allocatedBandwidthIn;
    private double allocatedBandwidthOut;
    private double allocatedMsgRateIn;
    private double allocatedMsgRateOut;

    private double preAllocatedCPU;
    private double preAllocatedMemory;
    private double preAllocatedBandwidthIn;
    private double preAllocatedBandwidthOut;
    private double preAllocatedMsgRateIn;
    private double preAllocatedMsgRateOut;

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

    public String getLoadReportType() {
        return loadReportType;
    }

    @Override
    public int getNumTopics() {
        numTopics = 0;
        if (this.bundleStats != null) {
            this.bundleStats.forEach((bundle, stats) -> {
                numTopics += stats.topics;
            });
        }
        return numTopics;
    }

    @Override
    public int getNumConsumers() {
        numConsumers = 0;
        if (this.bundleStats != null) {
            for (Map.Entry<String, NamespaceBundleStats> entry : this.bundleStats.entrySet()) {
                numConsumers = numConsumers + entry.getValue().consumerCount;
            }
        }
        return numConsumers;
    }

    @Override
    public int getNumProducers() {
        numProducers = 0;
        if (this.bundleStats != null) {
            for (Map.Entry<String, NamespaceBundleStats> entry : this.bundleStats.entrySet()) {
                numProducers = numProducers + entry.getValue().producerCount;
            }
        }
        return numProducers;
    }

    @Override
    public int getNumBundles() {
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

    public Set<String> getBundleGains() {
        return bundleGains;
    }

    public void setBundleGains(Set<String> bundleGains) {
        this.bundleGains = bundleGains;
    }

    public Set<String> getBundleLosses() {
        return bundleLosses;
    }

    public void setBundleLosses(Set<String> bundleLosses) {
        this.bundleLosses = bundleLosses;
    }

    public double getAllocatedCPU() {
        return allocatedCPU;
    }

    public void setAllocatedCPU(double allocatedCPU) {
        this.allocatedCPU = allocatedCPU;
    }

    public double getAllocatedMemory() {
        return allocatedMemory;
    }

    public void setAllocatedMemory(double allocatedMemory) {
        this.allocatedMemory = allocatedMemory;
    }

    public double getAllocatedBandwidthIn() {
        return allocatedBandwidthIn;
    }

    public void setAllocatedBandwidthIn(double allocatedBandwidthIn) {
        this.allocatedBandwidthIn = allocatedBandwidthIn;
    }

    public double getAllocatedBandwidthOut() {
        return allocatedBandwidthOut;
    }

    public void setAllocatedBandwidthOut(double allocatedBandwidthOut) {
        this.allocatedBandwidthOut = allocatedBandwidthOut;
    }

    public double getAllocatedMsgRateIn() {
        return allocatedMsgRateIn;
    }

    public void setAllocatedMsgRateIn(double allocatedMsgRateIn) {
        this.allocatedMsgRateIn = allocatedMsgRateIn;
    }

    public double getAllocatedMsgRateOut() {
        return allocatedMsgRateOut;
    }

    public void setAllocatedMsgRateOut(double allocatedMsgRateOut) {
        this.allocatedMsgRateOut = allocatedMsgRateOut;
    }

    public double getPreAllocatedCPU() {
        return preAllocatedCPU;
    }

    public void setPreAllocatedCPU(double preAllocatedCPU) {
        this.preAllocatedCPU = preAllocatedCPU;
    }

    public double getPreAllocatedMemory() {
        return preAllocatedMemory;
    }

    public void setPreAllocatedMemory(double preAllocatedMemory) {
        this.preAllocatedMemory = preAllocatedMemory;
    }

    public double getPreAllocatedBandwidthIn() {
        return preAllocatedBandwidthIn;
    }

    public void setPreAllocatedBandwidthIn(double preAllocatedBandwidthIn) {
        this.preAllocatedBandwidthIn = preAllocatedBandwidthIn;
    }

    public double getPreAllocatedBandwidthOut() {
        return preAllocatedBandwidthOut;
    }

    public void setPreAllocatedBandwidthOut(double preAllocatedBandwidthOut) {
        this.preAllocatedBandwidthOut = preAllocatedBandwidthOut;
    }

    public double getPreAllocatedMsgRateIn() {
        return preAllocatedMsgRateIn;
    }

    public void setPreAllocatedMsgRateIn(double preAllocatedMsgRateIn) {
        this.preAllocatedMsgRateIn = preAllocatedMsgRateIn;
    }

    public double getPreAllocatedMsgRateOut() {
        return preAllocatedMsgRateOut;
    }

    public void setPreAllocatedMsgRateOut(double preAllocatedMsgRateOut) {
        this.preAllocatedMsgRateOut = preAllocatedMsgRateOut;
    }

    public void setBrokerVersionString(String brokerVersionString) {
        this.brokerVersionString = brokerVersionString;
    }

    public String getBrokerVersionString() {
        return brokerVersionString;
    }

    @Override
    public String getWebServiceUrl() {
        return webServiceUrl;
    }

    @Override
    public String getWebServiceUrlTls() {
        return webServiceUrlTls;
    }

    @Override
    public String getPulsarServiceUrl() {
        return pulsarServiceUrl;
    }

    @Override
    public String getPulsarServiceUrlTls() {
        return pulsarServiceUrlTls;
    }

    public boolean isPersistentTopicsEnabled() {
        return persistentTopicsEnabled;
    }

    public void setPersistentTopicsEnabled(boolean persistentTopicsEnabled) {
        this.persistentTopicsEnabled = persistentTopicsEnabled;
    }

    public boolean isNonPersistentTopicsEnabled() {
        return nonPersistentTopicsEnabled;
    }

    public void setNonPersistentTopicsEnabled(boolean nonPersistentTopicsEnabled) {
        this.nonPersistentTopicsEnabled = nonPersistentTopicsEnabled;
    }

    @Override
    public ResourceUsage getCpu() {
        return systemResourceUsage != null ? systemResourceUsage.cpu : null;
    }

    @Override
    public ResourceUsage getMemory() {
        return systemResourceUsage != null ? systemResourceUsage.memory : null;
    }

    @Override
    public ResourceUsage getDirectMemory() {
        return systemResourceUsage != null ? systemResourceUsage.directMemory : null;
    }

    @Override
    public ResourceUsage getBandwidthIn() {
        return systemResourceUsage != null ? systemResourceUsage.bandwidthIn : null;
    }

    @Override
    public ResourceUsage getBandwidthOut() {
        return systemResourceUsage != null ? systemResourceUsage.bandwidthOut : null;
    }

    @Override
    public long getLastUpdate() {
        return timestamp;
    }

    @Override
    public double getMsgThroughputIn() {
        return msgRateIn;
    }

    @Override
    public double getMsgThroughputOut() {
        return msgRateOut;
    }

    @Override
    public Map<String, String> getProtocols() {
        return protocols;
    }

    public void setProtocols(Map<String, String> protocols) {
        this.protocols = protocols;
    }

    @Override
    public Optional<String> getProtocol(String protocol) {
        return Optional.ofNullable(protocols.get(protocol));
    }
}
