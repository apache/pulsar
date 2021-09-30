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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Contains all the data that is maintained locally on each broker.
 */
@JsonDeserialize(as = LocalBrokerData.class)
public class LocalBrokerData implements LoadManagerReport {

    // URLs to satisfy contract of ServiceLookupData (used by NamespaceService).
    private final String webServiceUrl;
    private final String webServiceUrlTls;
    private final String pulsarServiceUrl;
    private final String pulsarServiceUrlTls;
    private boolean persistentTopicsEnabled = true;
    private boolean nonPersistentTopicsEnabled = true;

    // Most recently available system resource usage.
    private ResourceUsage cpu;
    private ResourceUsage memory;
    private ResourceUsage directMemory;

    private ResourceUsage bandwidthIn;
    private ResourceUsage bandwidthOut;

    // Message data from the most recent namespace bundle stats.
    private double msgThroughputIn;
    private double msgThroughputOut;
    private double msgRateIn;
    private double msgRateOut;

    // Timestamp of last update.
    private long lastUpdate;

    // The stats given in the most recent invocation of update.
    private Map<String, NamespaceBundleStats> lastStats;

    private int numTopics;
    private int numBundles;
    private int numConsumers;
    private int numProducers;

    // All bundles belonging to this broker.
    private Set<String> bundles;

    // The bundles gained since the last invocation of update.
    private Set<String> lastBundleGains;

    // The bundles lost since the last invocation of update.
    private Set<String> lastBundleLosses;

    // The version string that this broker is running, obtained from the Maven build artifact in the POM
    private String brokerVersionString;
    // This place-holder requires to identify correct LoadManagerReport type while deserializing
    @SuppressWarnings("checkstyle:ConstantName")
    public static final String loadReportType = LocalBrokerData.class.getSimpleName();

    // the external protocol data advertised by protocol handlers.
    private Map<String, String> protocols;
    //
    private Map<String, AdvertisedListener> advertisedListeners;

    // For JSON only.
    public LocalBrokerData() {
        this(null, null, null, null);
    }

    /**
     * Broker data constructor which takes in four URLs to satisfy the contract of ServiceLookupData.
     */
    public LocalBrokerData(final String webServiceUrl, final String webServiceUrlTls, final String pulsarServiceUrl,
            final String pulsarServiceUrlTls) {
        this(webServiceUrl, webServiceUrlTls, pulsarServiceUrl, pulsarServiceUrlTls,
                Collections.unmodifiableMap(Collections.emptyMap()));
    }

    public LocalBrokerData(final String webServiceUrl, final String webServiceUrlTls, final String pulsarServiceUrl,
                           final String pulsarServiceUrlTls, Map<String, AdvertisedListener> advertisedListeners) {
        this.webServiceUrl = webServiceUrl;
        this.webServiceUrlTls = webServiceUrlTls;
        this.pulsarServiceUrl = pulsarServiceUrl;
        this.pulsarServiceUrlTls = pulsarServiceUrlTls;
        lastStats = Maps.newConcurrentMap();
        lastUpdate = System.currentTimeMillis();
        cpu = new ResourceUsage();
        memory = new ResourceUsage();
        directMemory = new ResourceUsage();
        bandwidthIn = new ResourceUsage();
        bandwidthOut = new ResourceUsage();
        bundles = new HashSet<>();
        lastBundleGains = new HashSet<>();
        lastBundleLosses = new HashSet<>();
        protocols = new HashMap<>();
        this.advertisedListeners = Collections.unmodifiableMap(Maps.newHashMap(advertisedListeners));
    }

    /**
     * Since the broker data is also used as a lock for the broker, we need to have a stable comparison
     * operator that is not affected by the actual load on the broker.
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof LocalBrokerData) {
            LocalBrokerData other = (LocalBrokerData) o;
            return Objects.equals(webServiceUrl, other.webServiceUrl)
                    && Objects.equals(webServiceUrlTls, other.webServiceUrlTls)
                    && Objects.equals(pulsarServiceUrl, other.pulsarServiceUrl)
                    && Objects.equals(pulsarServiceUrlTls, other.pulsarServiceUrlTls);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(webServiceUrl, webServiceUrlTls, pulsarServiceUrl, pulsarServiceUrlTls);
    }

    /**
     * Using the system resource usage and bundle stats acquired from the Pulsar client, update this LocalBrokerData.
     *
     * @param systemResourceUsage
     *            System resource usage (cpu, memory, and direct memory).
     * @param bundleStats
     *            The bundle stats retrieved from the Pulsar client.
     */
    public void update(final SystemResourceUsage systemResourceUsage,
            final Map<String, NamespaceBundleStats> bundleStats) {
        updateSystemResourceUsage(systemResourceUsage);
        updateBundleData(bundleStats);
        lastStats = bundleStats;
    }

    /**
     * Using another LocalBrokerData, update this.
     *
     * @param other
     *            LocalBrokerData to update from.
     */
    public void update(final LocalBrokerData other) {
        updateSystemResourceUsage(other.cpu, other.memory, other.directMemory, other.bandwidthIn, other.bandwidthOut);
        updateBundleData(other.lastStats);
        lastStats = other.lastStats;
    }

    // Set the cpu, memory, and direct memory to that of the new system resource usage data.
    private void updateSystemResourceUsage(final SystemResourceUsage systemResourceUsage) {
        updateSystemResourceUsage(systemResourceUsage.cpu, systemResourceUsage.memory, systemResourceUsage.directMemory,
                systemResourceUsage.bandwidthIn, systemResourceUsage.bandwidthOut);
    }

    // Update resource usage given each individual usage.
    private void updateSystemResourceUsage(final ResourceUsage cpu, final ResourceUsage memory,
            final ResourceUsage directMemory, final ResourceUsage bandwidthIn, final ResourceUsage bandwidthOut) {
        this.cpu = new ResourceUsage(cpu);
        this.memory = new ResourceUsage(memory);
        this.directMemory = new ResourceUsage(directMemory);
        this.bandwidthIn = new ResourceUsage(bandwidthIn);
        this.bandwidthOut = new ResourceUsage(bandwidthOut);
    }

    // Aggregate all message, throughput, topic count, bundle count, consumer
    // count, and producer count across the
    // given data. Also keep track of bundle gains and losses.
    private void updateBundleData(final Map<String, NamespaceBundleStats> bundleStats) {
        msgRateIn = 0;
        msgRateOut = 0;
        msgThroughputIn = 0;
        msgThroughputOut = 0;
        int totalNumTopics = 0;
        int totalNumBundles = 0;
        int totalNumConsumers = 0;
        int totalNumProducers = 0;
        final Iterator<String> oldBundleIterator = bundles.iterator();
        while (oldBundleIterator.hasNext()) {
            final String bundle = oldBundleIterator.next();
            if (!bundleStats.containsKey(bundle)) {
                // If this bundle is in the old bundle set but not the new one,
                // we lost it.
                lastBundleLosses.add(bundle);
                oldBundleIterator.remove();
            }
        }
        for (Map.Entry<String, NamespaceBundleStats> entry : bundleStats.entrySet()) {
            final String bundle = entry.getKey();
            final NamespaceBundleStats stats = entry.getValue();
            if (!bundles.contains(bundle)) {
                // If this bundle is in the new bundle set but not the old one,
                // we gained it.
                lastBundleGains.add(bundle);
                bundles.add(bundle);
            }
            msgThroughputIn += stats.msgThroughputIn;
            msgThroughputOut += stats.msgThroughputOut;
            msgRateIn += stats.msgRateIn;
            msgRateOut += stats.msgRateOut;
            totalNumTopics += stats.topics;
            ++totalNumBundles;
            totalNumConsumers += stats.consumerCount;
            totalNumProducers += stats.producerCount;
        }
        numTopics = totalNumTopics;
        numBundles = totalNumBundles;
        numConsumers = totalNumConsumers;
        numProducers = totalNumProducers;
    }

    public double getMaxResourceUsage() {
        return max(cpu.percentUsage(), memory.percentUsage(), directMemory.percentUsage(), bandwidthIn.percentUsage(),
                bandwidthOut.percentUsage()) / 100;
    }

    public String printResourceUsage() {
        return String.format(
                Locale.ENGLISH,
                "cpu: %.2f%%, memory: %.2f%%, directMemory: %.2f%%, bandwidthIn: %.2f%%, bandwidthOut: %.2f%%",
                cpu.percentUsage(), memory.percentUsage(), directMemory.percentUsage(), bandwidthIn.percentUsage(),
                bandwidthOut.percentUsage());
    }

    public double getMaxResourceUsageWithWeight(final double cpuWeight, final double memoryWeight,
                                                final double directMemoryWeight, final double bandwidthInWeight,
                                                final double bandwidthOutWeight) {
        return max(cpu.percentUsage() * cpuWeight, memory.percentUsage() * memoryWeight,
                directMemory.percentUsage() * directMemoryWeight, bandwidthIn.percentUsage() * bandwidthInWeight,
                bandwidthOut.percentUsage() * bandwidthOutWeight) / 100;
    }

    private static double max(double... args) {
        double max = Double.NEGATIVE_INFINITY;

        for (double d : args) {
            if (d > max) {
                max = d;
            }
        }

        return max;
    }

    private static float max(float...args) {
        float max = Float.NEGATIVE_INFINITY;

        for (float d : args) {
            if (d > max) {
                max = d;
            }
        }

        return max;
    }

    public String getLoadReportType() {
        return loadReportType;
    }

    @Override
    public ResourceUsage getCpu() {
        return cpu;
    }

    public void setCpu(ResourceUsage cpu) {
        this.cpu = cpu;
    }

    @Override
    public ResourceUsage getMemory() {
        return memory;
    }

    public void setMemory(ResourceUsage memory) {
        this.memory = memory;
    }

    @Override
    public ResourceUsage getDirectMemory() {
        return directMemory;
    }

    public void setDirectMemory(ResourceUsage directMemory) {
        this.directMemory = directMemory;
    }

    @Override
    public ResourceUsage getBandwidthIn() {
        return bandwidthIn;
    }

    public void setBandwidthIn(ResourceUsage bandwidthIn) {
        this.bandwidthIn = bandwidthIn;
    }

    @Override
    public ResourceUsage getBandwidthOut() {
        return bandwidthOut;
    }

    public void setBandwidthOut(ResourceUsage bandwidthOut) {
        this.bandwidthOut = bandwidthOut;
    }

    public Set<String> getLastBundleGains() {
        return lastBundleGains;
    }

    public void cleanDeltas() {
        lastBundleGains.clear();
        lastBundleLosses.clear();
    }

    public void setLastBundleGains(Set<String> lastBundleGains) {
        this.lastBundleGains = lastBundleGains;
    }

    public Set<String> getLastBundleLosses() {
        return lastBundleLosses;
    }

    public void setLastBundleLosses(Set<String> lastBundleLosses) {
        this.lastBundleLosses = lastBundleLosses;
    }

    @Override
    public long getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public Set<String> getBundles() {
        return bundles;
    }

    public void setBundles(Set<String> bundles) {
        this.bundles = bundles;
    }

    public Map<String, NamespaceBundleStats> getLastStats() {
        return lastStats;
    }

    public void setLastStats(Map<String, NamespaceBundleStats> lastStats) {
        this.lastStats = lastStats;
    }

    @Override
    public int getNumTopics() {
        return numTopics;
    }

    public void setNumTopics(int numTopics) {
        this.numTopics = numTopics;
    }

    @Override
    public int getNumBundles() {
        return numBundles;
    }

    public void setNumBundles(int numBundles) {
        this.numBundles = numBundles;
    }

    @Override
    public int getNumConsumers() {
        return numConsumers;
    }

    public void setNumConsumers(int numConsumers) {
        this.numConsumers = numConsumers;
    }

    @Override
    public int getNumProducers() {
        return numProducers;
    }

    public void setNumProducers(int numProducers) {
        this.numProducers = numProducers;
    }

    @Override
    public double getMsgThroughputIn() {
        return msgThroughputIn;
    }

    public void setMsgThroughputIn(double msgThroughputIn) {
        this.msgThroughputIn = msgThroughputIn;
    }

    @Override
    public double getMsgThroughputOut() {
        return msgThroughputOut;
    }

    public void setMsgThroughputOut(double msgThroughputOut) {
        this.msgThroughputOut = msgThroughputOut;
    }

    @Override
    public double getMsgRateIn() {
        return msgRateIn;
    }

    public void setMsgRateIn(double msgRateIn) {
        this.msgRateIn = msgRateIn;
    }

    @Override
    public double getMsgRateOut() {
        return msgRateOut;
    }

    public void setMsgRateOut(double msgRateOut) {
        this.msgRateOut = msgRateOut;
    }

    public void setBrokerVersionString(String brokerVersionString) {
        this.brokerVersionString = brokerVersionString;
    }

    @Override
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

    @Override
    public boolean isPersistentTopicsEnabled() {
        return persistentTopicsEnabled;
    }

    public void setPersistentTopicsEnabled(boolean persistentTopicsEnabled) {
        this.persistentTopicsEnabled = persistentTopicsEnabled;
    }

    @Override
    public boolean isNonPersistentTopicsEnabled() {
        return nonPersistentTopicsEnabled;
    }

    public void setNonPersistentTopicsEnabled(boolean nonPersistentTopicsEnabled) {
        this.nonPersistentTopicsEnabled = nonPersistentTopicsEnabled;
    }

    @Override
    public Map<String, NamespaceBundleStats> getBundleStats() {
        return getLastStats();
    }

    public void setProtocols(Map<String, String> protocols) {
        this.protocols = protocols;
    }

    @Override
    public Map<String, String> getProtocols() {
        return protocols;
    }

    @Override
    public Optional<String> getProtocol(String protocol) {
        return Optional.ofNullable(protocols.get(protocol));
    }

    public Map<String, AdvertisedListener> getAdvertisedListeners() {
        return advertisedListeners;
    }

    public void setAdvertisedListeners(Map<String, AdvertisedListener> advertisedListeners) {
        this.advertisedListeners = advertisedListeners;
    }
}
