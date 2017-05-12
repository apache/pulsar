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
package com.yahoo.pulsar.broker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;
import com.yahoo.pulsar.common.policies.data.loadbalancer.ServiceLookupData;
import com.yahoo.pulsar.common.policies.data.loadbalancer.SystemResourceUsage;

/**
 * Contains all the data that is maintained locally on each broker.
 */
public class LocalBrokerData extends JSONWritable implements ServiceLookupData {

    // URLs to satisfy contract of ServiceLookupData (used by NamespaceService).
    private final String webServiceUrl;
    private final String webServiceUrlTls;
    private final String pulsarServiceUrl;
    private final String pulsarServiceUrlTls;

    // Most recently available system resource usage.
    private SystemResourceUsage systemResourceUsage;

    // Timestamp of last update.
    private long lastUpdate;

    // The stats given in the most recent invocation of update.
    private Map<String, NamespaceBundleStats> lastStats;

    // Number of bundles on this broker.
    private int numBundles;

    // Overall bundle data for this.
    private BundleData bundleData;

    // All bundles belonging to this broker.
    private Set<String> bundles;

    // The bundles gained since the last invocation of update.
    private Set<String> lastBundleGains;

    // The bundles lost since the last invocation of update.
    private Set<String> lastBundleLosses;

    // For JSON only.
    public LocalBrokerData() {
        this(null, null, null, null);
    }

    /**
     * Broker data constructor which takes in four URLs to satisfy the contract of ServiceLookupData.
     */
    public LocalBrokerData(final String webServiceUrl, final String webServiceUrlTls, final String pulsarServiceUrl,
            final String pulsarServiceUrlTls) {
        this.webServiceUrl = webServiceUrl;
        this.webServiceUrlTls = webServiceUrlTls;
        this.pulsarServiceUrl = pulsarServiceUrl;
        this.pulsarServiceUrlTls = pulsarServiceUrlTls;
        bundleData = new BundleData();
        lastStats = new HashMap<>();
        lastUpdate = System.currentTimeMillis();
        systemResourceUsage = new SystemResourceUsage();
        bundles = new HashSet<>();
        lastBundleGains = new HashSet<>();
        lastBundleLosses = new HashSet<>();
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
        this.systemResourceUsage = systemResourceUsage;
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
        this.systemResourceUsage = other.systemResourceUsage;
        updateBundleData(other.lastStats);
        lastStats = other.lastStats;
    }

    // Aggregate all message, throughput, topic count, bundle count, consumer
    // count, and producer count across the
    // given data. Also keep track of bundle gains and losses.
    private void updateBundleData(final Map<String, NamespaceBundleStats> bundleStats) {
        double msgRateIn = 0;
        double msgRateOut = 0;
        double msgThroughputIn = 0;
        double msgThroughputOut = 0;
        int numTopics = 0;
        int totalNumBundles = 0;
        int numConsumers = 0;
        int numProducers = 0;
        lastBundleGains.clear();
        lastBundleLosses.clear();
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
            numTopics += stats.topics;
            ++totalNumBundles;
            numConsumers += stats.consumerCount;
            numProducers += stats.producerCount;
        }
        bundleData.setNumConsumers(numConsumers);
        bundleData.setNumProducers(numProducers);
        bundleData.setNumTopics(numTopics);
        final MessageData messageData = bundleData.getMessageData();
        messageData.setMsgRateIn(msgRateIn);
        messageData.setMsgRateOut(msgRateOut);
        messageData.setMsgThroughputIn(msgThroughputIn);
        messageData.setMsgThroughputOut(msgThroughputOut);
        numBundles = totalNumBundles;

    }

    public double getMaxResourceUsage() {
        return Math.max(
                Math.max(Math.max(systemResourceUsage.cpu.percentUsage(), systemResourceUsage.memory.percentUsage()),
                        Math.max(systemResourceUsage.directMemory.percentUsage(),
                                systemResourceUsage.bandwidthIn.percentUsage())),
                systemResourceUsage.bandwidthOut.percentUsage()) / 100;
    }

    public SystemResourceUsage getSystemResourceUsage() {
        return systemResourceUsage;
    }

    public void setSystemResourceUsage(SystemResourceUsage systemResourceUsage) {
        this.systemResourceUsage = systemResourceUsage;
    }

    public BundleData getBundleData() {
        return bundleData;
    }

    public void setBundleData(BundleData bundleData) {
        this.bundleData = bundleData;
    }

    public Set<String> getLastBundleGains() {
        return lastBundleGains;
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

    public int getNumBundles() {
        return numBundles;
    }

    public void setNumBundles(int numBundles) {
        this.numBundles = numBundles;
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

}
